# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">04 | データ加工 — Gold層（Python）</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">PySpark DataFrame API で分析用の集計テーブルを作成</p>
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
# MAGIC Silverテーブルを PySpark DataFrame API で集計・加工して、分析・BIに直接使えるGoldテーブルを作成します。<br>
# MAGIC RFM分析、店舗別KPI、AI関数による自動分類を体験します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# -- リセット用: コメントを外して実行すると gd_ テーブルを全削除
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
from pyspark.sql.functions import col, count, countDistinct, sum as _sum, round as _round, avg, date_trunc

sl_sales = spark.table(f"sl_sales")

gd_store_sales_summary = (
    sl_sales
    .withColumn("sale_month", date_trunc("month", col("sale_datetime")))  # 月単位に丸める
    .groupBy("store_id", "store_name", "store_prefecture", "sale_month")
    .agg(
        countDistinct("sale_id").alias("tx_count"),              # 取引件数
        countDistinct("customer_id").alias("unique_customers"),  # ユニーク顧客数
        _sum("total_amount").alias("total_sales"),               # 売上合計
        _round(avg("total_amount"), 0).alias("avg_basket"),      # 平均客単価
    )
)

gd_store_sales_summary.write.mode("overwrite").saveAsTable(f"gd_store_sales_summary")
display(spark.table(f"gd_store_sales_summary").orderBy("store_id", "sale_month").limit(10))

# COMMAND ----------

# DBTITLE 1,gd_category_sales（カテゴリ別売上）
sl_sale_items = spark.table(f"sl_sale_items")
sl_sales = spark.table(f"sl_sales")

gd_category_sales = (
    sl_sale_items.alias("si")
    .join(sl_sales.select("sale_id", "sale_datetime").alias("s"), col("si.sale_id") == col("s.sale_id"))
    .withColumn("sale_month", date_trunc("month", col("s.sale_datetime")))
    .groupBy(col("si.category_id"), col("si.category_name"), "sale_month")
    .agg(
        _sum("subtotal").alias("total_sales"),
        _sum("quantity").alias("total_qty"),
        countDistinct(col("s.sale_id")).alias("tx_count"),
    )
)

gd_category_sales.write.mode("overwrite").saveAsTable(f"gd_category_sales")
display(spark.table(f"gd_category_sales").orderBy(col("sale_month").desc(), col("total_sales").desc()).limit(10))

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
from pyspark.sql.functions import datediff, current_date, max as _max, concat, when, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import ntile

sl_sales = spark.table(f"sl_sales")

# Step 1: 顧客ごとに R(最終購買からの日数), F(購買回数), M(累計金額) を集計
rfm_raw = (
    sl_sales
    .groupBy("customer_id")
    .agg(
        datediff(current_date(), _max(col("sale_datetime").cast("date"))).alias("recency"),
        countDistinct("sale_id").alias("frequency"),
        _sum("total_amount").alias("monetary"),
    )
)

# Step 2: NTILE(5) で各指標を5段階にスコアリング（1=低、5=高）
w_r = Window.orderBy(col("recency").asc())    # 最近買った人ほど高スコア
w_f = Window.orderBy(col("frequency").desc())  # 頻繁に買う人ほど高スコア
w_m = Window.orderBy(col("monetary").desc())   # 高額購入者ほど高スコア

rfm_scored = (
    rfm_raw
    .withColumn("r_score", ntile(5).over(w_r))
    .withColumn("f_score", ntile(5).over(w_f))
    .withColumn("m_score", ntile(5).over(w_m))
)

# Step 3: スコアの組み合わせでセグメント分類
gd_rfm = (
    rfm_scored
    .withColumn("rfm_code", concat(col("r_score"), col("f_score"), col("m_score")))
    .withColumn(
        "rfm_segment",
        when((col("r_score") >= 4) & (col("f_score") >= 4) & (col("m_score") >= 4), lit("ロイヤル顧客"))
        .when((col("r_score") >= 4) & (col("f_score") >= 3), lit("優良顧客"))
        .when((col("r_score") >= 4) & (col("f_score") <= 2), lit("新規顧客"))
        .when((col("r_score") <= 2) & (col("f_score") >= 3), lit("離反リスク"))
        .when((col("r_score") <= 2) & (col("f_score") <= 2), lit("休眠顧客"))
        .otherwise(lit("一般顧客")),
    )
)

gd_rfm.write.mode("overwrite").saveAsTable(f"gd_rfm")

display(
    spark.table(f"gd_rfm")
    .groupBy("rfm_segment").agg(count("*").alias("cnt"))
    .orderBy(col("cnt").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. AI関数で自動分類

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Databricks AI Functions</strong><br>
# MAGIC SQL内でLLMを呼び出せる組み込み関数群です。<br>
# MAGIC AI関数はSQL関数として提供されるため、Pythonからは <code>spark.sql()</code> 経由で呼び出します。<br><br>
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
        'databricks-claude-opus-4-7',
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
from pyspark.sql.functions import coalesce

sl_sales = spark.table(f"sl_sales")
sl_stores_df = spark.table(f"sl_stores")
sl_facilities = spark.table(f"sl_facilities")

# 店舗ごとの売上KPIと、周辺施設情報（競合・駅など）を結合した商圏分析用テーブル
# -- 店舗別の売上KPIを集計
customer_store = (
    sl_sales.alias("s")
    .join(sl_stores_df.alias("st"), col("s.store_id") == col("st.store_id"))
    .groupBy(
        col("s.store_id"), col("s.store_name"),
        col("st.prefecture").alias("store_prefecture"),
        col("st.lat").alias("store_lat"), col("st.lon").alias("store_lon"),
        col("st.floor_area_sqm"), col("st.store_type"),
    )
    .agg(
        countDistinct("s.customer_id").alias("unique_customers"),
        countDistinct("s.sale_id").alias("total_transactions"),
        _sum("s.total_amount").alias("total_sales"),
    )
)

# -- 店舗周辺の施設数を集計（競合店・駅など）
facility_stats = (
    sl_facilities
    .groupBy(col("nearest_store_id").alias("store_id"))
    .agg(
        count("*").alias("nearby_facility_count"),
        _sum(when(col("facility_type") == "競合店", 1).otherwise(0)).alias("competitor_count"),
        _sum(when(col("facility_type") == "駅", 1).otherwise(0)).alias("station_count"),
        _round(avg("distance_km"), 2).alias("avg_facility_distance_km"),
    )
)

# -- 売上KPIと施設統計を結合（施設情報がない店舗は0埋め）
gd_store_trade_area = (
    customer_store.alias("cs")
    .join(facility_stats.alias("fs"), col("cs.store_id") == col("fs.store_id"), "left")
    .select(
        col("cs.*"),
        coalesce(col("fs.nearby_facility_count"), lit(0)).alias("nearby_facility_count"),
        coalesce(col("fs.competitor_count"), lit(0)).alias("competitor_count"),
        coalesce(col("fs.station_count"), lit(0)).alias("station_count"),
        coalesce(col("fs.avg_facility_distance_km"), lit(0)).alias("avg_facility_distance_km"),
    )
)

gd_store_trade_area.write.mode("overwrite").saveAsTable(f"gd_store_trade_area")
display(spark.table(f"gd_store_trade_area").orderBy(col("total_sales").desc()).limit(10))

# COMMAND ----------

# DBTITLE 1,gd_weather_sales（天気×売上相関）
# 天気と売上の相関を分析するため、天気データに日次売上を結合して月単位に集計
sl_weather = spark.table(f"sl_weather")
sl_sales = spark.table(f"sl_sales")

# 都道府県×日ごとの売上を事前集計してから天気と結合
daily_sales = (
    sl_sales
    .withColumn("sale_date", col("sale_datetime").cast("date"))
    .groupBy(col("store_prefecture").alias("prefecture"), "sale_date")
    .agg(
        _sum("total_amount").alias("daily_sales"),
        count("*").alias("daily_tx"),
    )
)

gd_weather_sales = (
    sl_weather.alias("w")
    .join(daily_sales.alias("s"), (col("w.prefecture") == col("s.prefecture")) & (col("w.date") == col("s.sale_date")), "left")
    .withColumn("month", date_trunc("month", col("w.date")))
    .groupBy("weather", col("w.prefecture"), "month")
    .agg(
        _round(avg("max_temp"), 1).alias("avg_max_temp"),
        _round(avg("precipitation"), 1).alias("avg_precipitation"),
        coalesce(_sum("daily_sales"), lit(0)).alias("total_sales"),
        coalesce(_sum("daily_tx"), lit(0)).alias("total_tx"),
    )
)

gd_weather_sales.write.mode("overwrite").saveAsTable(f"gd_weather_sales")

display(
    spark.table(f"gd_weather_sales")
    .groupBy("weather").agg(_round(avg("total_sales"), 0).alias("avg_daily_sales"))
    .orderBy(col("avg_daily_sales").desc())
)

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
