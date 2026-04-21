# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">02 | データ取り込み — Bronze層（SQL+Python）</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">spark.sql() で Volume の CSV を Bronze テーブルに取り込む</p>
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
# MAGIC VolumeにあるCSVファイルをDelta形式のBronzeテーブルとして取り込みます。<br>
# MAGIC <code>spark.sql()</code> を使って、PythonからSQLを実行する方法を体験します。
# MAGIC <table style="margin-top: 10px; border-collapse: collapse; width: 100%;">
# MAGIC   <tr style="background: #1B3139; color: white;">
# MAGIC     <th style="padding: 6px 10px; text-align: left;">作成テーブル</th>
# MAGIC     <th style="padding: 6px 10px; text-align: left;">内容</th>
# MAGIC     <th style="padding: 6px 10px; text-align: left;">取り込み方法</th>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 6px 10px;"><code>bz_stores</code></td>
# MAGIC     <td style="padding: 6px 10px;">店舗マスタ（住所・座標・面積）</td>
# MAGIC     <td style="padding: 6px 10px;">read_files + CTAS</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 6px 10px;"><code>bz_products</code></td>
# MAGIC     <td style="padding: 6px 10px;">商品マスタ（カテゴリ・単価）</td>
# MAGIC     <td style="padding: 6px 10px;">read_files + CTAS</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 6px 10px;"><code>bz_customers</code></td>
# MAGIC     <td style="padding: 6px 10px;">会員マスタ（性別・都道府県）</td>
# MAGIC     <td style="padding: 6px 10px;">read_files + CTAS</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 6px 10px;"><code>bz_weather</code></td>
# MAGIC     <td style="padding: 6px 10px;">天候データ（気温・降水量）</td>
# MAGIC     <td style="padding: 6px 10px;">read_files + CTAS</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 6px 10px;"><code>bz_facilities</code></td>
# MAGIC     <td style="padding: 6px 10px;">周辺施設（競合店・駅）</td>
# MAGIC     <td style="padding: 6px 10px;">read_files + CTAS</td>
# MAGIC   </tr>
# MAGIC   <tr style="border-top: 2px solid #F57C00;">
# MAGIC     <td style="padding: 6px 10px;"><code>bz_sales</code></td>
# MAGIC     <td style="padding: 6px 10px;">売上トランザクション</td>
# MAGIC     <td style="padding: 6px 10px;">COPY INTO（増分ロード）</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 6px 10px;"><code>bz_sale_items</code></td>
# MAGIC     <td style="padding: 6px 10px;">売上明細（商品単位）</td>
# MAGIC     <td style="padding: 6px 10px;">COPY INTO（増分ロード）</td>
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
# MAGIC <strong>🔗 データ取り込み手法の使い分け</strong>
# MAGIC <table style="margin-top: 8px; border-collapse: collapse; width: 100%;">
# MAGIC   <tr style="background: #1B3139; color: white;">
# MAGIC     <th style="padding: 8px; text-align: left;">手法</th>
# MAGIC     <th style="padding: 8px; text-align: left;">用途</th>
# MAGIC     <th style="padding: 8px; text-align: left;">特徴</th>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 8px;"><code>read_files()</code></td>
# MAGIC     <td style="padding: 8px;">クイック確認・プロトタイプ</td>
# MAGIC     <td style="padding: 8px;">SQLワンラインでCSVを即テーブル化。スキーマ自動推論</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 8px;"><code>COPY INTO</code></td>
# MAGIC     <td style="padding: 8px;">本番ETL・増分ロード</td>
# MAGIC     <td style="padding: 8px;">処理済みファイルを追跡し、同じファイルを再実行しても重複しない（冪等性）</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ AutoLoader について</strong><br>
# MAGIC Databricksには <code>AutoLoader（cloudFiles）</code> という強力な増分ロード機能もあります。<br>
# MAGIC AutoLoaderはStructured Streamingベースでチェックポイント管理が必要なため、本ハンズオンでは扱いません。<br>
# MAGIC 単発取り込みは <code>read_files()</code>、増分ロードは <code>COPY INTO</code> でカバーします。<br>
# MAGIC 本番環境でのファイル増分ロードには AutoLoader の利用を検討してください。<br>
# MAGIC 📖 <a href="https://docs.databricks.com/ja/ingestion/cloud-object-storage/auto-loader/index.html" target="_blank">Auto Loader ドキュメント（日本語）</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. `read_files()` — spark.sql() でCSVをクイック確認

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 read_files() とは</strong><br>
# MAGIC SQLの <code>FROM</code> 句でファイルパスを指定するだけで、CSVやJSON等をテーブルとして読める関数です。<br>
# MAGIC スキーマ推論が自動で行われるため、POCの初期段階で「まずデータの中身を見たい」ときに最速です。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,read_files() でCSVの中身をクイック確認
# spark.sql()  = PythonからSQLを実行する関数。f"..." でPython変数（VOLUME_PATH等）を埋め込める
# display()    = 結果をテーブル形式で表示する Databricks の関数
display(spark.sql(f"SELECT * FROM read_files('{VOLUME_PATH}/stores.csv', format => 'csv', header => true) LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. `read_files()` + CTAS でマスタ系テーブルを取り込み

# COMMAND ----------

# DBTITLE 1,bz_stores
spark.sql(f"""
    CREATE OR REPLACE TABLE bz_stores AS
    SELECT *,
        current_timestamp() AS _ingested_at,              -- 取り込み日時を記録（監査用）
        '{VOLUME_PATH}/stores.csv' AS _source_file       -- 取り込み元ファイルパスを記録（監査用）
    FROM read_files('{VOLUME_PATH}/stores.csv', format => 'csv', header => true)
""")
# spark.table()  = テーブルを参照
# .count()       = 行数を取得
print(f"✅ bz_stores: {spark.table(f'bz_stores').count()} 件")

# COMMAND ----------

# DBTITLE 1,bz_products
spark.sql(f"""
    CREATE OR REPLACE TABLE bz_products AS
    SELECT *,
        current_timestamp() AS _ingested_at,
        '{VOLUME_PATH}/products.csv' AS _source_file
    FROM read_files('{VOLUME_PATH}/products.csv', format => 'csv', header => true)
""")
print(f"✅ bz_products: {spark.table(f'bz_products').count()} 件")

# COMMAND ----------

# DBTITLE 1,bz_customers
spark.sql(f"""
    CREATE OR REPLACE TABLE bz_customers AS
    SELECT *,
        current_timestamp() AS _ingested_at,
        '{VOLUME_PATH}/customers.csv' AS _source_file
    FROM read_files('{VOLUME_PATH}/customers.csv', format => 'csv', header => true)
""")
print(f"✅ bz_customers: {spark.table(f'bz_customers').count()} 件")

# COMMAND ----------

# DBTITLE 1,bz_weather
spark.sql(f"""
    CREATE OR REPLACE TABLE bz_weather AS
    SELECT *,
        current_timestamp() AS _ingested_at,
        '{VOLUME_PATH}/weather.csv' AS _source_file
    FROM read_files('{VOLUME_PATH}/weather.csv', format => 'csv', header => true)
""")
print(f"✅ bz_weather: {spark.table(f'bz_weather').count()} 件")

# COMMAND ----------

# DBTITLE 1,bz_facilities
spark.sql(f"""
    CREATE OR REPLACE TABLE bz_facilities AS
    SELECT *,
        current_timestamp() AS _ingested_at,
        '{VOLUME_PATH}/facilities.csv' AS _source_file
    FROM read_files('{VOLUME_PATH}/facilities.csv', format => 'csv', header => true)
""")
print(f"✅ bz_facilities: {spark.table(f'bz_facilities').count()} 件")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ UI操作: カラムプロファイリング</strong><br>
# MAGIC 上のセル結果の右上にある「<strong>+</strong>」→「<strong>Data Profile</strong>」をクリックすると、<br>
# MAGIC 各カラムの統計情報（NULL率・値の分布・ユニーク数・外れ値）がワンクリックで確認できます。<br>
# MAGIC データ品質チェックのSQLを書く必要がありません。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. `COPY INTO` — 冪等な増分ロード（トランザクション系）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 なぜトランザクション系だけ COPY INTO？</strong><br>
# MAGIC <ul>
# MAGIC   <li><strong>マスタ系</strong>（stores, products等）→ 件数が少なく変更頻度も低い → <code>CREATE OR REPLACE</code> で全洗い替えすればOK</li>
# MAGIC   <li><strong>トランザクション系</strong>（sales, sale_items）→ 日々新しいデータが追加される → <code>COPY INTO</code> で増分ロード</li>
# MAGIC </ul>
# MAGIC <code>COPY INTO</code> は取り込み済みファイルを内部で追跡しているため、<strong>同じコマンドを何度実行しても重複取り込みが起きません</strong>（冪等性）。<br>
# MAGIC ETLジョブが途中でエラーになって再実行しても安全です。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,bz_sales（COPY INTO）
# COPY INTO は先にテーブルを作っておく必要がある（read_files + CTAS とは異なる点）
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bz_sales (
        sale_id STRING, customer_id STRING, store_id STRING,
        sale_datetime STRING, total_amount STRING, total_quantity STRING,
        payment_method STRING
    ) USING DELTA                                                -- Delta Lake 形式でテーブルを作成
""")

spark.sql(f"""
    COPY INTO bz_sales
    FROM '{VOLUME_PATH}/sales.csv'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')  -- 1行目をヘッダーとして扱う
    COPY_OPTIONS ('mergeSchema' = 'true')                        -- カラム差分を自動マージ
""")
print(f"✅ bz_sales: {spark.table(f'bz_sales').count()} 件")

# COMMAND ----------

# DBTITLE 1,bz_sale_items（COPY INTO）
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bz_sale_items (
        sale_item_id STRING, sale_id STRING, product_id STRING,
        quantity STRING, unit_price STRING, subtotal STRING
    ) USING DELTA
""")

spark.sql(f"""
    COPY INTO bz_sale_items
    FROM '{VOLUME_PATH}/sale_items.csv'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")
print(f"✅ bz_sale_items: {spark.table(f'bz_sale_items').count()} 件")

# COMMAND ----------

# DBTITLE 1,COPY INTO 再実行（冪等性の確認）
# 同じコマンドをもう一度実行 → 0 rows affected になる = 重複取り込みが起きない！
result = spark.sql(f"""
    COPY INTO bz_sales
    FROM '{VOLUME_PATH}/sales.csv'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")
display(result)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ 冪等性を体験！</strong><br>
# MAGIC 2回目の実行で「0 rows affected」になりましたか？<br>
# MAGIC これがCOPY INTOの冪等性です。処理済みファイルは再取り込みされないため、ジョブの再実行が安全にできます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 取り込み結果の確認

# COMMAND ----------

# DBTITLE 1,Bronzeテーブル一覧
display(spark.sql(f"SHOW TABLES LIKE 'bz_*'"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 参考ドキュメント</strong><br>
# MAGIC <ul>
# MAGIC   <li><a href="https://docs.databricks.com/ja/ingestion/file-ingestion/read-files.html" target="_blank">read_files() リファレンス</a></li>
# MAGIC   <li><a href="https://docs.databricks.com/ja/ingestion/file-ingestion/copy-into.html" target="_blank">COPY INTO リファレンス</a></li>
# MAGIC   <li><a href="https://docs.databricks.com/ja/ingestion/cloud-object-storage/auto-loader/index.html" target="_blank">Auto Loader（本番用増分ロード）</a></li>
# MAGIC   <li><a href="https://docs.databricks.com/ja/connect/storage/volumes.html" target="_blank">Unity Catalog Volume</a></li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Bronze層 取り込み完了</strong><br>
# MAGIC 全CSVファイルをDelta形式のBronzeテーブルとして取り込みました。<br>
# MAGIC 次のノートブック（03_データ加工_Silver）で、型変換・クレンジング・結合を行います。
# MAGIC </div>
