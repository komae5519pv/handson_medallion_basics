# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">02 | データ取り込み — Bronze層（Python）</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">PySpark DataFrame API で Volume の CSV を Bronze テーブルに取り込む</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 20 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC VolumeにあるCSVファイルを <code>spark.read.csv()</code> で読み込み、Delta形式のBronzeテーブルとして保存します。<br>
# MAGIC PySpark DataFrame API を使ったデータ取り込みの基本パターンを体験します。
# MAGIC <table style="margin-top: 10px; border-collapse: collapse; width: 100%;">
# MAGIC   <tr style="background: #1B3139; color: white;">
# MAGIC     <th style="padding: 6px 10px; text-align: left;">作成テーブル</th>
# MAGIC     <th style="padding: 6px 10px; text-align: left;">内容</th>
# MAGIC     <th style="padding: 6px 10px; text-align: left;">取り込み方法</th>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 6px 10px;"><code>bz_stores</code></td>
# MAGIC     <td style="padding: 6px 10px;">店舗マスタ（住所・座標・面積）</td>
# MAGIC     <td style="padding: 6px 10px;">spark.read.csv + saveAsTable</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 6px 10px;"><code>bz_products</code></td>
# MAGIC     <td style="padding: 6px 10px;">商品マスタ（カテゴリ・単価）</td>
# MAGIC     <td style="padding: 6px 10px;">spark.read.csv + saveAsTable</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 6px 10px;"><code>bz_customers</code></td>
# MAGIC     <td style="padding: 6px 10px;">会員マスタ（性別・都道府県）</td>
# MAGIC     <td style="padding: 6px 10px;">spark.read.csv + saveAsTable</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 6px 10px;"><code>bz_weather</code></td>
# MAGIC     <td style="padding: 6px 10px;">天候データ（気温・降水量）</td>
# MAGIC     <td style="padding: 6px 10px;">spark.read.csv + saveAsTable</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 6px 10px;"><code>bz_facilities</code></td>
# MAGIC     <td style="padding: 6px 10px;">周辺施設（競合店・駅）</td>
# MAGIC     <td style="padding: 6px 10px;">spark.read.csv + saveAsTable</td>
# MAGIC   </tr>
# MAGIC   <tr style="border-top: 2px solid #F57C00;">
# MAGIC     <td style="padding: 6px 10px;"><code>bz_sales</code></td>
# MAGIC     <td style="padding: 6px 10px;">売上トランザクション</td>
# MAGIC     <td style="padding: 6px 10px;">spark.read.csv + saveAsTable</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 6px 10px;"><code>bz_sale_items</code></td>
# MAGIC     <td style="padding: 6px 10px;">売上明細（商品単位）</td>
# MAGIC     <td style="padding: 6px 10px;">spark.read.csv + saveAsTable</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# -- リセット用: コメントを外して実行すると bz_ テーブルを全削除
# tables_df = spark.sql(f"SHOW TABLES IN {MY_CATALOG}.{MY_SCHEMA}")
# for table in tables_df.collect():
#     table_name = table["tableName"]
#     if table_name.startswith("bz_"):
#         spark.sql(f"DROP TABLE IF EXISTS {MY_CATALOG}.{MY_SCHEMA}.{table_name}")
#         print(f"削除されたテーブル: {table_name}")
# print("全ての bz_ で始まるテーブルが削除されました。")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Python vs SQL の取り込み手法比較</strong>
# MAGIC <table style="margin-top: 8px; border-collapse: collapse; width: 100%;">
# MAGIC   <tr style="background: #1B3139; color: white;">
# MAGIC     <th style="padding: 8px; text-align: left;">Python API</th>
# MAGIC     <th style="padding: 8px; text-align: left;">SQL 相当</th>
# MAGIC     <th style="padding: 8px; text-align: left;">用途</th>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 8px;"><code>spark.read.csv()</code></td>
# MAGIC     <td style="padding: 8px;"><code>read_files()</code></td>
# MAGIC     <td style="padding: 8px;">ファイル読み込み</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 8px;"><code>df.write.saveAsTable()</code></td>
# MAGIC     <td style="padding: 8px;"><code>CREATE TABLE AS SELECT</code></td>
# MAGIC     <td style="padding: 8px;">テーブル保存</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 8px;"><code>df.withColumn()</code></td>
# MAGIC     <td style="padding: 8px;"><code>SELECT *, expr AS col</code></td>
# MAGIC     <td style="padding: 8px;">カラム追加</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 8px;"><code>（なし — SQL専用機能）</code></td>
# MAGIC     <td style="padding: 8px;"><code>COPY INTO</code></td>
# MAGIC     <td style="padding: 8px;">冪等な増分ロード</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ 増分ロードについて</strong><br>
# MAGIC 本番のETLでは、トランザクション系データに <code>COPY INTO</code>（SQL）や <code>AutoLoader</code> を使って増分ロードします。<br>
# MAGIC これらはSQL/Structured Streaming APIで提供されるため、本ノートブックでは全テーブルを <code>spark.read.csv()</code> で全量取り込みします。<br>
# MAGIC 📖 <a href="https://docs.databricks.com/ja/ingestion/file-ingestion/copy-into.html" target="_blank">COPY INTO リファレンス</a> ｜
# MAGIC <a href="https://docs.databricks.com/ja/ingestion/cloud-object-storage/auto-loader/index.html" target="_blank">Auto Loader ドキュメント</a>
# MAGIC </div>

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. `spark.read.csv()` でCSVをクイック確認

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 spark.read.csv() とは</strong><br>
# MAGIC PySpark の DataFrameReader API です。CSVファイルを読み込んで DataFrame として返します。<br>
# MAGIC <code>header=True</code> で1行目をカラム名として認識、<code>inferSchema=True</code> でデータ型を自動推論します。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,spark.read.csv() でCSVの中身をクイック確認
# spark.read.csv() = CSVを DataFrame として読み込む
# header=True      = 1行目をカラム名として使用
# inferSchema=True = データ型を自動推論（String/Int/Double等）
df_preview = spark.read.csv(f"{VOLUME_PATH}/stores.csv", header=True, inferSchema=True)
display(df_preview.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. マスタ系テーブルの取り込み

# COMMAND ----------

# DBTITLE 1,bz_stores
df = (
    spark.read.csv(f"{VOLUME_PATH}/stores.csv", header=True, inferSchema=True)
    .withColumn("_ingested_at", current_timestamp())                     # 取り込み日時を記録（監査用）
    .withColumn("_source_file", lit(f"{VOLUME_PATH}/stores.csv"))        # 取り込み元ファイルパスを記録（監査用）
)
# .write.mode("overwrite") = テーブルが存在すれば上書き
# .format("delta")         = Delta Lake 形式で保存
# .saveAsTable()           = Unity Catalog のマネージドテーブルとして登録
df.write.mode("overwrite").format("delta").saveAsTable("bz_stores")
print(f"✅ bz_stores: {spark.table('bz_stores').count()} 件")

# COMMAND ----------

# DBTITLE 1,bz_products
df = (
    spark.read.csv(f"{VOLUME_PATH}/products.csv", header=True, inferSchema=True)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit(f"{VOLUME_PATH}/products.csv"))
)
df.write.mode("overwrite").format("delta").saveAsTable("bz_products")
print(f"✅ bz_products: {spark.table('bz_products').count()} 件")

# COMMAND ----------

# DBTITLE 1,bz_customers
df = (
    spark.read.csv(f"{VOLUME_PATH}/customers.csv", header=True, inferSchema=True)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit(f"{VOLUME_PATH}/customers.csv"))
)
df.write.mode("overwrite").format("delta").saveAsTable("bz_customers")
print(f"✅ bz_customers: {spark.table('bz_customers').count()} 件")

# COMMAND ----------

# DBTITLE 1,bz_weather
df = (
    spark.read.csv(f"{VOLUME_PATH}/weather.csv", header=True, inferSchema=True)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit(f"{VOLUME_PATH}/weather.csv"))
)
df.write.mode("overwrite").format("delta").saveAsTable("bz_weather")
print(f"✅ bz_weather: {spark.table('bz_weather').count()} 件")

# COMMAND ----------

# DBTITLE 1,bz_facilities
df = (
    spark.read.csv(f"{VOLUME_PATH}/facilities.csv", header=True, inferSchema=True)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit(f"{VOLUME_PATH}/facilities.csv"))
)
df.write.mode("overwrite").format("delta").saveAsTable("bz_facilities")
print(f"✅ bz_facilities: {spark.table('bz_facilities').count()} 件")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ UI操作: カラムプロファイリング</strong><br>
# MAGIC 上のセル結果の右上にある「<strong>+</strong>」→「<strong>Data Profile</strong>」をクリックすると、<br>
# MAGIC 各カラムの統計情報（NULL率・値の分布・ユニーク数・外れ値）がワンクリックで確認できます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. トランザクション系テーブルの取り込み

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 マスタ系 vs トランザクション系</strong><br>
# MAGIC <ul>
# MAGIC   <li><strong>マスタ系</strong>（stores, products等）→ 件数が少なく変更頻度も低い → 全洗い替え（<code>mode("overwrite")</code>）でOK</li>
# MAGIC   <li><strong>トランザクション系</strong>（sales, sale_items）→ 日々新しいデータが追加される → 本番では増分ロード</li>
# MAGIC </ul>
# MAGIC ハンズオンでは初回ロードのため <code>mode("overwrite")</code> で全量取り込みします。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,bz_sales
df = (
    spark.read.csv(f"{VOLUME_PATH}/sales.csv", header=True, inferSchema=True)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit(f"{VOLUME_PATH}/sales.csv"))
)
df.write.mode("overwrite").format("delta").saveAsTable("bz_sales")
print(f"✅ bz_sales: {spark.table('bz_sales').count()} 件")

# COMMAND ----------

# DBTITLE 1,bz_sale_items
df = (
    spark.read.csv(f"{VOLUME_PATH}/sale_items.csv", header=True, inferSchema=True)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit(f"{VOLUME_PATH}/sale_items.csv"))
)
df.write.mode("overwrite").format("delta").saveAsTable("bz_sale_items")
print(f"✅ bz_sale_items: {spark.table('bz_sale_items').count()} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 取り込み結果の確認

# COMMAND ----------

# DBTITLE 1,Bronzeテーブル一覧
display(spark.sql("SHOW TABLES LIKE 'bz_*'"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 参考ドキュメント</strong><br>
# MAGIC <ul>
# MAGIC   <li><a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html" target="_blank">spark.read.csv() リファレンス</a></li>
# MAGIC   <li><a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.saveAsTable.html" target="_blank">df.write.saveAsTable() リファレンス</a></li>
# MAGIC   <li><a href="https://docs.databricks.com/ja/connect/storage/volumes.html" target="_blank">Unity Catalog Volume</a></li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Bronze層 取り込み完了</strong><br>
# MAGIC 全CSVファイルを <code>spark.read.csv()</code> + <code>df.write.saveAsTable()</code> でBronzeテーブルとして取り込みました。<br>
# MAGIC 次のノートブック（03_データ加工_Silver）で、DataFrame APIを使って型変換・クレンジング・結合を行います。
# MAGIC </div>
