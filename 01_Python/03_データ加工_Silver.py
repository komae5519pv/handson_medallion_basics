# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">03 | データ加工 — Silver層（Python）</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">PySpark DataFrame API で型変換・クレンジング・結合を行う</p>
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
# MAGIC Bronzeテーブルに対して PySpark DataFrame API で型変換・NULL処理・重複排除・テーブル結合を行い、<br>
# MAGIC 分析しやすいSilverテーブルを作成します。また、Delta Lake の Time Travel 機能を体験します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# -- リセット用: コメントを外して実行すると sl_ テーブルを全削除
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
# MAGIC ## 1. PySpark DataFrame で Silver テーブル作成

# COMMAND ----------

# DBTITLE 1,sl_stores（型変換 + NULL処理）
from pyspark.sql.functions import col, when, lit, to_date

bz_stores = spark.table(f"bz_stores")

sl_stores = (
    bz_stores
    .select(
        col("store_id").cast("bigint").alias("store_id"),         # 文字列 → 数値型に変換
        col("store_name"),
        when(col("prefecture").isNull(), lit("不明"))              # NULL → デフォルト値で補完
            .otherwise(col("prefecture")).alias("prefecture"),
        col("city"),
        col("address"),
        col("lat").cast("double").alias("lat"),
        col("lon").cast("double").alias("lon"),
        to_date(col("opening_date")).alias("opening_date"),       # 文字列 → DATE型
        col("floor_area_sqm").cast("int").alias("floor_area_sqm"),
        col("parking_capacity").cast("int").alias("parking_capacity"),
        col("store_type"),
    )
)

sl_stores.write.mode("overwrite").saveAsTable(f"sl_stores")
display(spark.table(f"sl_stores").limit(5))

# COMMAND ----------

# DBTITLE 1,sl_products（型変換）
bz_products = spark.table(f"bz_products")

sl_products = (
    bz_products
    .select(
        col("product_id").cast("bigint").alias("product_id"),
        col("product_name"),
        col("category_id").cast("int").alias("category_id"),
        col("category_name"),
        col("subcategory"),
        col("unit_price").cast("int").alias("unit_price"),
        col("is_pb").cast("boolean").alias("is_pb"),             # プライベートブランドフラグ
        col("supplier"),
    )
)

sl_products.write.mode("overwrite").saveAsTable(f"sl_products")
print(f"sl_products: {spark.table(f'sl_products').count()} 件")

# COMMAND ----------

# DBTITLE 1,sl_customers（型変換 + NULL処理）
bz_customers = spark.table(f"bz_customers")

sl_customers = (
    bz_customers
    .select(
        col("customer_id"),
        col("name"),
        col("gender"),
        to_date(col("birth_date")).alias("birth_date"),
        col("postal_code"),
        when(col("prefecture").isNull(), lit("不明"))              # NULL → デフォルト値で補完
            .otherwise(col("prefecture")).alias("prefecture"),
        col("city"),
        to_date(col("registration_date")).alias("registration_date"),
        col("home_store_id").cast("int").alias("home_store_id"),  # よく利用する店舗ID
    )
)

sl_customers.write.mode("overwrite").saveAsTable(f"sl_customers")
print(f"sl_customers: {spark.table(f'sl_customers').count()} 件")

# COMMAND ----------

# DBTITLE 1,sl_sales（型変換 + 店舗情報結合 = 非正規化）
from pyspark.sql.functions import col as c

# 売上トランザクションに店舗マスタを結合し、店舗名・都道府県を非正規化で持たせる
bz_sales = spark.table(f"bz_sales")
sl_stores_df = spark.table(f"sl_stores")

sl_sales = (
    bz_sales.alias("s")
    .join(
        sl_stores_df.select(
            col("store_id"),
            col("store_name"),
            col("prefecture").alias("store_prefecture"),
        ).alias("st"),
        c("s.store_id").cast("bigint") == c("st.store_id"),
        "left",                                                   # Silver店舗マスタと結合
    )
    .select(
        c("s.sale_id").cast("bigint").alias("sale_id"),
        c("s.customer_id"),
        c("s.store_id").cast("bigint").alias("store_id"),
        c("st.store_name"),                                       # JOIN で付与した店舗名
        c("st.store_prefecture"),                                  # JOIN で付与した店舗所在地
        c("s.sale_datetime").cast("timestamp").alias("sale_datetime"),
        c("s.total_amount").cast("bigint").alias("total_amount"),
        c("s.total_quantity").cast("int").alias("total_quantity"),
        c("s.payment_method"),
    )
)

sl_sales.write.mode("overwrite").saveAsTable(f"sl_sales")
print(f"sl_sales: {spark.table(f'sl_sales').count()} 件")

# COMMAND ----------

# DBTITLE 1,sl_sale_items（型変換 + 商品情報結合 + 重複排除）
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# ROW_NUMBER で sale_item_id の重複を排除してから、商品マスタを結合
bz_sale_items = spark.table(f"bz_sale_items")
sl_products_df = spark.table(f"sl_products")

window_spec = Window.partitionBy("sale_item_id").orderBy("sale_item_id")

deduped = (
    bz_sale_items                                                 # Bronze には重複行が混入している想定
    .withColumn("row_num", row_number().over(window_spec))
    .filter("row_num = 1")                                        # 重複排除: 最初の1行のみ保持
    .drop("row_num")
)

sl_sale_items = (
    deduped.alias("d")
    .join(
        sl_products_df.select("product_id", "product_name", "category_id", "category_name").alias("p"),
        col("d.product_id").cast("bigint") == col("p.product_id"),
        "left",                                                   # Silver商品マスタと結合
    )
    .select(
        col("d.sale_item_id").cast("bigint").alias("sale_item_id"),
        col("d.sale_id").cast("bigint").alias("sale_id"),
        col("d.product_id").cast("bigint").alias("product_id"),
        col("p.product_name"),                                    # JOIN で付与した商品名
        col("p.category_id"),
        col("p.category_name"),                                   # JOIN で付与したカテゴリ名
        col("d.quantity").cast("int").alias("quantity"),
        col("d.unit_price").cast("int").alias("unit_price"),
        col("d.subtotal").cast("bigint").alias("subtotal"),
    )
)

sl_sale_items.write.mode("overwrite").saveAsTable(f"sl_sale_items")
print(f"sl_sale_items: {spark.table(f'sl_sale_items').count()} 件")

# COMMAND ----------

# DBTITLE 1,sl_weather / sl_facilities
bz_weather = spark.table(f"bz_weather")

sl_weather = bz_weather.select(                                  # 型変換のみ（クレンジング不要）
    to_date(col("date")).alias("date"),
    col("prefecture"),
    col("weather"),
    col("max_temp").cast("int").alias("max_temp"),
    col("min_temp").cast("int").alias("min_temp"),
    col("precipitation").cast("double").alias("precipitation"),
    col("humidity").cast("int").alias("humidity"),
)

sl_weather.write.mode("overwrite").saveAsTable(f"sl_weather")

bz_facilities = spark.table(f"bz_facilities")

sl_facilities = bz_facilities.select(
    col("facility_id").cast("int").alias("facility_id"),
    col("facility_type"),
    col("facility_name"),
    col("prefecture"),
    col("city"),
    col("lat").cast("double").alias("lat"),
    col("lon").cast("double").alias("lon"),
    col("nearest_store_id").cast("int").alias("nearest_store_id"),# 最寄り店舗ID
    col("distance_km").cast("double").alias("distance_km"),       # 最寄り店舗までの距離
)

sl_facilities.write.mode("overwrite").saveAsTable(f"sl_facilities")

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

# DBTITLE 1,Delta操作のリトライヘルパー（Auto-OPTIMIZE競合対策）
import time

def retry_delta(sql_text, max_retries=3, delay=5):
    """自動最適化（Auto-OPTIMIZE）との競合時にリトライする"""
    for attempt in range(max_retries):
        try:
            return spark.sql(sql_text)
        except Exception as e:
            if "DELTA_CONCURRENT_WRITE" in str(e) and attempt < max_retries - 1:
                print(f"⏳ Auto-OPTIMIZE と競合 — {delay}秒後にリトライ ({attempt+1}/{max_retries})")
                time.sleep(delay)
            else:
                raise

# COMMAND ----------

# DBTITLE 1,テーブルの変更履歴を確認
display(spark.sql(f"DESCRIBE HISTORY sl_stores"))

# COMMAND ----------

# DBTITLE 1,わざと壊してみる
retry_delta(f"DELETE FROM sl_stores WHERE store_type = '大型'")
remaining = spark.table(f"sl_stores").count()
print(f"残りレコード数: {remaining}")

# COMMAND ----------

# DBTITLE 1,RESTORE で復元
retry_delta(f"RESTORE TABLE sl_stores TO VERSION AS OF 0")
restored = spark.table(f"sl_stores").count()
print(f"復元後レコード数: {restored}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Silver層 作成完了</strong><br>
# MAGIC Bronze → Silver の加工が完了しました。PySpark DataFrame API での型変換・JOIN・クレンジング、Time Travel を体験しました。<br>
# MAGIC 次のノートブック（04_データ加工_Gold）で、分析用のGoldテーブルを構築します。
# MAGIC </div>
