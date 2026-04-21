# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">03 | データ加工 — Silver層（SQL+Python）</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">spark.sql() で型変換・クレンジング・結合を行いSilverテーブルを作成</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 25 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC Bronzeテーブルに対して <code>spark.sql()</code> で型変換・NULL処理・重複排除・テーブル結合を行い、<br>
# MAGIC 分析しやすいSilverテーブルを作成します。また、Delta Lake の Time Travel 機能を体験します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# # リセット用: コメントを外して実行すると sl_ テーブルを全削除
# tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")
# for table in tables_df.collect():
#     table_name = table["tableName"]
#     if table_name.startswith("sl_"):
#         spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
#         print(f"削除されたテーブル: {table_name}")
# print("全ての sl_ で始まるテーブルが削除されました。")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. spark.sql() で Silver テーブル作成

# COMMAND ----------

# DBTITLE 1,sl_stores（型変換 + NULL処理）
sl_stores_df = spark.sql(f"""
    SELECT
        CAST(store_id AS BIGINT) AS store_id,       -- 文字列 → 数値型に変換
        store_name,
        COALESCE(prefecture, '不明') AS prefecture,  -- NULL → デフォルト値で補完
        city,
        address,
        CAST(lat AS DOUBLE) AS lat,
        CAST(lon AS DOUBLE) AS lon,
        CAST(opening_date AS DATE) AS opening_date,  -- 文字列 → DATE型
        CAST(floor_area_sqm AS INT) AS floor_area_sqm,
        CAST(parking_capacity AS INT) AS parking_capacity,
        store_type
    FROM bz_stores
""")

sl_stores_df.write.mode("overwrite").format("delta").saveAsTable(f"sl_stores")
display(sl_stores_df.limit(5))

# COMMAND ----------

# DBTITLE 1,sl_products（型変換）
sl_products_df = spark.sql(f"""
    SELECT
        CAST(product_id AS BIGINT) AS product_id,
        product_name,
        CAST(category_id AS INT) AS category_id,
        category_name,
        subcategory,
        CAST(unit_price AS INT) AS unit_price,
        CAST(is_pb AS BOOLEAN) AS is_pb,             -- プライベートブランドフラグ
        supplier
    FROM bz_products
""")

sl_products_df.write.mode("overwrite").format("delta").saveAsTable(f"sl_products")
print(f"sl_products: {spark.table(f'sl_products').count()} 件")

# COMMAND ----------

# DBTITLE 1,sl_customers（型変換 + NULL処理）
sl_customers_df = spark.sql(f"""
    SELECT
        customer_id,
        name,
        gender,
        CAST(birth_date AS DATE) AS birth_date,
        postal_code,
        COALESCE(prefecture, '不明') AS prefecture,  -- NULL → デフォルト値で補完
        city,
        CAST(registration_date AS DATE) AS registration_date,
        CAST(home_store_id AS INT) AS home_store_id  -- よく利用する店舗ID
    FROM bz_customers
""")

sl_customers_df.write.mode("overwrite").format("delta").saveAsTable(f"sl_customers")
print(f"sl_customers: {spark.table(f'sl_customers').count()} 件")

# COMMAND ----------

# DBTITLE 1,sl_sales（型変換 + 店舗情報結合 = 非正規化）
# 売上トランザクションに店舗マスタを結合し、店舗名・都道府県を非正規化で持たせる
sl_sales_df = spark.sql(f"""
    SELECT
        CAST(s.sale_id AS BIGINT) AS sale_id,
        s.customer_id,
        CAST(s.store_id AS BIGINT) AS store_id,
        st.store_name,                               -- JOIN で付与した店舗名
        st.prefecture AS store_prefecture,            -- JOIN で付与した店舗所在地
        CAST(s.sale_datetime AS TIMESTAMP) AS sale_datetime,
        CAST(s.total_amount AS BIGINT) AS total_amount,
        CAST(s.total_quantity AS INT) AS total_quantity,
        s.payment_method
    FROM bz_sales s
    LEFT JOIN sl_stores st                            -- Silver店舗マスタと結合
        ON CAST(s.store_id AS BIGINT) = st.store_id
""")

sl_sales_df.write.mode("overwrite").format("delta").saveAsTable(f"sl_sales")
print(f"sl_sales: {spark.table(f'sl_sales').count()} 件")

# COMMAND ----------

# DBTITLE 1,sl_sale_items（型変換 + 商品情報結合 + 重複排除）
# ROW_NUMBER で sale_item_id の重複を排除してから、商品マスタを結合
sl_sale_items_df = spark.sql(f"""
    WITH deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY sale_item_id ORDER BY sale_item_id) AS row_num
        FROM bz_sale_items                            -- Bronze には重複行が混入している想定
    )
    SELECT
        CAST(d.sale_item_id AS BIGINT) AS sale_item_id,
        CAST(d.sale_id AS BIGINT) AS sale_id,
        CAST(d.product_id AS BIGINT) AS product_id,
        p.product_name,                               -- JOIN で付与した商品名
        p.category_id,
        p.category_name,                              -- JOIN で付与したカテゴリ名
        CAST(d.quantity AS INT) AS quantity,
        CAST(d.unit_price AS INT) AS unit_price,
        CAST(d.subtotal AS BIGINT) AS subtotal
    FROM deduped d
    LEFT JOIN sl_products p                           -- Silver商品マスタと結合
        ON CAST(d.product_id AS BIGINT) = p.product_id
    WHERE d.row_num = 1                               -- 重複排除: 最初の1行のみ保持
""")

sl_sale_items_df.write.mode("overwrite").format("delta").saveAsTable(f"sl_sale_items")
print(f"sl_sale_items: {spark.table(f'sl_sale_items').count()} 件")

# COMMAND ----------

# DBTITLE 1,sl_weather / sl_facilities
sl_weather_df = spark.sql(f"""
    SELECT
        CAST(date AS DATE) AS date,
        prefecture,
        weather,
        CAST(max_temp AS INT) AS max_temp,
        CAST(min_temp AS INT) AS min_temp,
        CAST(precipitation AS DOUBLE) AS precipitation,
        CAST(humidity AS INT) AS humidity
    FROM bz_weather                                   -- 型変換のみ（クレンジング不要）
""")
sl_weather_df.write.mode("overwrite").format("delta").saveAsTable(f"sl_weather")

sl_facilities_df = spark.sql(f"""
    SELECT
        CAST(facility_id AS INT) AS facility_id,
        facility_type,
        facility_name,
        prefecture,
        city,
        CAST(lat AS DOUBLE) AS lat,
        CAST(lon AS DOUBLE) AS lon,
        CAST(nearest_store_id AS INT) AS nearest_store_id,  -- 最寄り店舗ID
        CAST(distance_km AS DOUBLE) AS distance_km          -- 最寄り店舗までの距離
    FROM bz_facilities
""")
sl_facilities_df.write.mode("overwrite").format("delta").saveAsTable(f"sl_facilities")

print(f"sl_weather: {spark.table(f'sl_weather').count()} 件")
print(f"sl_facilities: {spark.table(f'sl_facilities').count()} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Delta Lake Time Travel

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Time Travel = 「やり直し」ができる安心感</strong><br>
# MAGIC Deltaテーブルは全ての変更履歴を保持しています。<br>
# MAGIC 「加工を間違えた」「うっかりデータを消した」場合でも、<code>RESTORE</code> で任意のバージョンに戻せます。<br>
# MAGIC POC中の試行錯誤が怖くありません。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,テーブルの変更履歴を確認
display(spark.sql(f"DESCRIBE HISTORY sl_stores"))

# COMMAND ----------

# DBTITLE 1,わざと壊してみる
spark.sql(f"DELETE FROM sl_stores WHERE store_type = '大型'")
remaining = spark.table(f"sl_stores").count()
print(f"残りレコード数: {remaining}")

# COMMAND ----------

# DBTITLE 1,RESTORE で復元
spark.sql(f"RESTORE TABLE sl_stores TO VERSION AS OF 0")
restored = spark.table(f"sl_stores").count()
print(f"復元後レコード数: {restored}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Silver層 作成完了</strong><br>
# MAGIC Bronze → Silver の加工が完了しました。<code>spark.sql()</code> での型変換・JOIN・クレンジング、Time Travel を体験しました。<br>
# MAGIC 次のノートブック（04_データ加工_Gold）で、分析用のGoldテーブルを構築します。
# MAGIC </div>
