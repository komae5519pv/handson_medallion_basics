# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">04 | データ加工 — Gold層（SQL+Python）</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">spark.sql() で集計テーブルとAI関数による高度な加工</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 30 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC Silverテーブルを <code>spark.sql()</code> で集計・加工して、分析・BIに直接使えるGoldテーブルを作成します。<br>
# MAGIC RFM分析、店舗別KPI、AI関数による自動分類を体験します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# # リセット用: コメントを外して実行すると gd_ テーブルを全削除
# tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")
# for table in tables_df.collect():
#     table_name = table["tableName"]
#     if table_name.startswith("gd_"):
#         spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
#         print(f"削除されたテーブル: {table_name}")
# print("全ての gd_ で始まるテーブルが削除されました。")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. 店舗別売上サマリ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Gold層の役割</strong><br>
# MAGIC Gold層は「分析者やBIツールがそのまま使えるテーブル」です。<br>
# MAGIC Silver層のデータを目的別に集計・結合し、ダッシュボードやGenieから直接参照できる形にします。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,gd_store_sales_summary（店舗×月別売上）
gd_store_sales_summary_df = spark.sql(f"""
    SELECT
        s.store_id,
        s.store_name,
        s.store_prefecture,
        DATE_TRUNC('month', s.sale_datetime) AS sale_month,  -- 月単位に丸める
        COUNT(DISTINCT s.sale_id) AS tx_count,               -- 取引件数
        COUNT(DISTINCT s.customer_id) AS unique_customers,   -- ユニーク顧客数
        SUM(s.total_amount) AS total_sales,                  -- 売上合計
        ROUND(AVG(s.total_amount), 0) AS avg_basket          -- 平均客単価
    FROM sl_sales s
    GROUP BY s.store_id, s.store_name, s.store_prefecture, DATE_TRUNC('month', s.sale_datetime)
""")

gd_store_sales_summary_df.write.mode("overwrite").format("delta").saveAsTable(f"gd_store_sales_summary")
display(spark.sql(f"SELECT * FROM gd_store_sales_summary ORDER BY store_id, sale_month LIMIT 10"))

# COMMAND ----------

# DBTITLE 1,gd_category_sales（カテゴリ別売上）
gd_category_sales_df = spark.sql(f"""
    SELECT
        si.category_id,
        si.category_name,
        DATE_TRUNC('month', s.sale_datetime) AS sale_month,
        SUM(si.subtotal) AS total_sales,
        SUM(si.quantity) AS total_qty,
        COUNT(DISTINCT s.sale_id) AS tx_count
    FROM sl_sale_items si
    JOIN sl_sales s ON si.sale_id = s.sale_id
    GROUP BY si.category_id, si.category_name, DATE_TRUNC('month', s.sale_datetime)
""")

gd_category_sales_df.write.mode("overwrite").format("delta").saveAsTable(f"gd_category_sales")
display(spark.sql(f"SELECT * FROM gd_category_sales ORDER BY sale_month DESC, total_sales DESC LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. RFM分析

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 RFM分析とは</strong><br>
# MAGIC 顧客を <strong>Recency（最終購買日からの経過日数）</strong>、<strong>Frequency（購買回数）</strong>、<strong>Monetary（累計金額）</strong> の3軸でスコアリングし、セグメント分類する手法です。<br>
# MAGIC 「優良顧客」「離反リスク」「新規」などを定量的に識別できます。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,gd_rfm（RFMスコア算出）
gd_rfm_df = spark.sql(f"""
    -- Step 1: 顧客ごとに R(最終購買からの日数), F(購買回数), M(累計金額) を集計
    WITH rfm_raw AS (
        SELECT
            customer_id,
            DATEDIFF(CURRENT_DATE(), MAX(CAST(sale_datetime AS DATE))) AS recency,
            COUNT(DISTINCT sale_id) AS frequency,
            SUM(total_amount) AS monetary
        FROM sl_sales
        GROUP BY customer_id
    ),
    -- Step 2: NTILE(5) で各指標を5段階にスコアリング（1=低、5=高）
    rfm_scored AS (
        SELECT
            *,
            NTILE(5) OVER (ORDER BY recency ASC) AS r_score,   -- 最近買った人ほど高スコア
            NTILE(5) OVER (ORDER BY frequency DESC) AS f_score, -- 頻繁に買う人ほど高スコア
            NTILE(5) OVER (ORDER BY monetary DESC) AS m_score   -- 高額購入者ほど高スコア
        FROM rfm_raw
    )
    -- Step 3: スコアの組み合わせでセグメント分類
    SELECT
        *,
        CONCAT(r_score, f_score, m_score) AS rfm_code,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'ロイヤル顧客'
            WHEN r_score >= 4 AND f_score >= 3 THEN '優良顧客'
            WHEN r_score >= 4 AND f_score <= 2 THEN '新規顧客'
            WHEN r_score <= 2 AND f_score >= 3 THEN '離反リスク'
            WHEN r_score <= 2 AND f_score <= 2 THEN '休眠顧客'
            ELSE '一般顧客'
        END AS rfm_segment
    FROM rfm_scored
""")

gd_rfm_df.write.mode("overwrite").format("delta").saveAsTable(f"gd_rfm")
display(spark.sql(f"""
    SELECT rfm_segment, count(*) AS cnt
    FROM gd_rfm
    GROUP BY rfm_segment
    ORDER BY cnt DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. AI関数で自動分類

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Databricks AI Functions</strong><br>
# MAGIC SQL内でLLMを呼び出せる組み込み関数群です。データ加工の中で自然言語処理を実行できます。<br>
# MAGIC AI関数はSQL関数として提供されるため、<code>spark.sql()</code> 経由で呼び出します。<br><br>
# MAGIC 📖 <a href="https://docs.databricks.com/ja/large-language-models/ai-functions.html" target="_blank">AI Functions ドキュメント</a>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,ai_classify() — 商品カテゴリの自動分類
df_ai_classify = spark.sql(f"""
    SELECT
        product_name,
        category_name AS original_category,
        ai_classify(product_name, ARRAY('DIY・工具', '園芸・ガーデニング', '住宅設備', '日用品', 'アウトドア')) AS ai_category
    FROM sl_products
    LIMIT 20
""")
display(df_ai_classify)

# COMMAND ----------

# DBTITLE 1,ai_query() — LLMの世界知識で競合分析
# LLMが学習済みの世界知識（実在する店舗・地域情報）を活用して、商圏内の競合リスクを分析
df_ai_query = spark.sql(f"""
    SELECT
        store_name,
        prefecture,
        store_type,
        ai_query(
            'databricks-claude-3-7-sonnet',
            CONCAT(
                'あなたはホームセンター業界の競合分析アナリストです。',
                '以下の店舗の商圏に実在する競合ホームセンターチェーンを最大3つ挙げ、',
                '各競合に対する差別化ポイントを1行で提案してください。',
                ' 店舗名: ', store_name,
                ' 所在地: ', prefecture,
                ' 店舗タイプ: ', store_type
            )
        ) AS ai_competitive_analysis
    FROM sl_stores
    LIMIT 5
""")
display(df_ai_query)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ AI関数の注意点</strong><br>
# MAGIC <ul>
# MAGIC   <li>AI関数はLLMを呼び出すため、<strong>大量データに対して実行するとコストと時間がかかります</strong></li>
# MAGIC   <li>結果はテーブルに保存して再利用するのがベストプラクティスです</li>
# MAGIC   <li>本番利用では、まず少量データで結果を確認してからフルスキャンしてください</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. 商圏分析用テーブル

# COMMAND ----------

# DBTITLE 1,gd_store_trade_area（店舗別商圏サマリ）
# 店舗ごとの売上KPIと、周辺施設情報（競合・駅など）を結合した商圏分析用テーブル
gd_store_trade_area_df = spark.sql(f"""
    -- 店舗別の売上KPIを集計
    WITH customer_store AS (
        SELECT
            s.store_id,
            s.store_name,
            st.prefecture AS store_prefecture,
            st.lat AS store_lat,
            st.lon AS store_lon,
            st.floor_area_sqm,
            st.store_type,
            COUNT(DISTINCT s.customer_id) AS unique_customers,
            COUNT(DISTINCT s.sale_id) AS total_transactions,
            SUM(s.total_amount) AS total_sales
        FROM sl_sales s
        JOIN sl_stores st ON s.store_id = st.store_id
        GROUP BY s.store_id, s.store_name, st.prefecture, st.lat, st.lon, st.floor_area_sqm, st.store_type
    ),
    -- 店舗周辺の施設数を集計（競合店・駅など）
    facility_stats AS (
        SELECT
            nearest_store_id AS store_id,
            COUNT(*) AS nearby_facility_count,
            SUM(CASE WHEN facility_type = '競合店' THEN 1 ELSE 0 END) AS competitor_count,
            SUM(CASE WHEN facility_type = '駅' THEN 1 ELSE 0 END) AS station_count,
            ROUND(AVG(distance_km), 2) AS avg_facility_distance_km
        FROM sl_facilities
        GROUP BY nearest_store_id
    )
    -- 売上KPIと施設統計を結合（施設情報がない店舗は0埋め）
    SELECT
        cs.*,
        COALESCE(fs.nearby_facility_count, 0) AS nearby_facility_count,
        COALESCE(fs.competitor_count, 0) AS competitor_count,
        COALESCE(fs.station_count, 0) AS station_count,
        COALESCE(fs.avg_facility_distance_km, 0) AS avg_facility_distance_km
    FROM customer_store cs
    LEFT JOIN facility_stats fs ON cs.store_id = fs.store_id
""")

gd_store_trade_area_df.write.mode("overwrite").format("delta").saveAsTable(f"gd_store_trade_area")
display(spark.sql(f"SELECT * FROM gd_store_trade_area ORDER BY total_sales DESC LIMIT 10"))

# COMMAND ----------

# DBTITLE 1,gd_weather_sales（天気×売上相関）
# 天気と売上の相関を分析するため、天気データに日次売上を結合して月単位に集計
gd_weather_sales_df = spark.sql(f"""
    SELECT
        w.weather,
        w.prefecture,
        DATE_TRUNC('month', w.date) AS month,
        ROUND(AVG(w.max_temp), 1) AS avg_max_temp,
        ROUND(AVG(w.precipitation), 1) AS avg_precipitation,
        COALESCE(SUM(s.daily_sales), 0) AS total_sales,
        COALESCE(SUM(s.daily_tx), 0) AS total_tx
    FROM sl_weather w
    LEFT JOIN (
        -- 都道府県×日ごとの売上を事前集計してから天気と結合
        SELECT
            store_prefecture AS prefecture,
            CAST(sale_datetime AS DATE) AS sale_date,
            SUM(total_amount) AS daily_sales,
            COUNT(*) AS daily_tx
        FROM sl_sales
        GROUP BY store_prefecture, CAST(sale_datetime AS DATE)
    ) s ON w.prefecture = s.prefecture AND w.date = s.sale_date
    GROUP BY w.weather, w.prefecture, DATE_TRUNC('month', w.date)
""")

gd_weather_sales_df.write.mode("overwrite").format("delta").saveAsTable(f"gd_weather_sales")
display(spark.sql(f"""
    SELECT weather, ROUND(AVG(total_sales), 0) AS avg_daily_sales
    FROM gd_weather_sales
    GROUP BY weather
    ORDER BY avg_daily_sales DESC
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Gold層 作成完了</strong><br>
# MAGIC 以下のGoldテーブルが作成されました:
# MAGIC <ul>
# MAGIC   <li><strong>gd_store_sales_summary</strong> — 店舗×月別売上サマリ</li>
# MAGIC   <li><strong>gd_category_sales</strong> — カテゴリ別売上</li>
# MAGIC   <li><strong>gd_rfm</strong> — RFM分析（顧客セグメント）</li>
# MAGIC   <li><strong>gd_store_trade_area</strong> — 店舗別商圏サマリ</li>
# MAGIC   <li><strong>gd_weather_sales</strong> — 天気×売上相関</li>
# MAGIC </ul>
# MAGIC 次のノートブック（05_テーブル設定）で、PK/FK制約・コメント・カラムマスキングを設定します。
# MAGIC </div>
