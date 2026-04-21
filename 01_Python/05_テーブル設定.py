# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">05 | テーブル設定（Python）</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">PK/FK制約・コメント・カラムマスキングでガバナンスを整える</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 15 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC テーブルにPK/FK制約、テーブル/カラムコメント、カラムマスキングを設定し、<br>
# MAGIC Unity Catalogのガバナンス機能を体験します。<br>
# MAGIC DDL操作はSQL構文が最も簡潔なため、<code>spark.sql()</code> 経由で実行します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. テーブル・カラムコメント

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 なぜコメントが重要か</strong><br>
# MAGIC Unity Catalogではテーブルやカラムに日本語コメントを付けられます。<br>
# MAGIC コメントは <strong>カタログエクスプローラ</strong> や <strong>Genie</strong> から参照されるため、<br>
# MAGIC 適切なコメントを付けることで「データの意味」が組織全体で共有されます。<br>
# MAGIC Genieがユーザーの質問を正しく理解するためにも、コメントは非常に重要です。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,テーブルコメント
table_comments = {
    "sl_stores": "Silver層: 店舗マスタ。店舗の基本情報（住所・座標・面積・タイプ）を管理",
    "sl_products": "Silver層: 商品マスタ。商品名・カテゴリ・単価・PB区分を管理",
    "sl_customers": "Silver層: 会員マスタ。会員の基本情報・登録店舗を管理",
    "sl_sales": "Silver層: 売上トランザクション。店舗情報を結合済み",
    "sl_sale_items": "Silver層: 売上明細。商品情報を結合済み、重複排除済み",
    "gd_rfm": "Gold層: RFM分析結果。顧客セグメント（ロイヤル/優良/新規/離反リスク/休眠/一般）",
    "gd_store_trade_area": "Gold層: 店舗別商圏サマリ。売上・顧客数・周辺施設情報を集約",
    "gd_store_sales_summary": "Gold層: 店舗×月別売上サマリ。取引件数・ユニーク顧客数・売上合計を集計",
    "gd_category_sales": "Gold層: カテゴリ×月別売上。商品カテゴリごとの売上・数量を集計",
    "gd_weather_sales": "Gold層: 天気×売上相関。天候条件と売上の関係を分析",
}

for table, comment in table_comments.items():
    spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")

print(f"✅ {len(table_comments)} テーブルにコメントを設定しました")

# COMMAND ----------

# DBTITLE 1,カラムコメント（主要テーブル）
column_comments = [
    ("sl_stores", "store_id", "店舗ID（一意）"),
    ("sl_stores", "store_name", "店舗名"),
    ("sl_stores", "prefecture", "都道府県（NULLは「不明」に変換済み）"),
    ("sl_stores", "lat", "緯度"),
    ("sl_stores", "lon", "経度"),
    ("sl_stores", "floor_area_sqm", "売場面積（平方メートル）"),
    ("sl_stores", "store_type", "店舗タイプ（大型/標準/コンパクト）"),
    ("sl_sales", "sale_id", "売上ID（一意）"),
    ("sl_sales", "customer_id", "会員ID（sl_customersへのFK）"),
    ("sl_sales", "store_id", "店舗ID（sl_storesへのFK）"),
    ("sl_sales", "total_amount", "合計金額（税込）"),
    ("sl_sales", "payment_method", "決済手段（現金/クレジットカード/電子マネー/QRコード決済/ポイント）"),
    ("gd_rfm", "r_score", "Recencyスコア（5=最近購入, 1=長期未購入）"),
    ("gd_rfm", "f_score", "Frequencyスコア（5=高頻度, 1=低頻度）"),
    ("gd_rfm", "m_score", "Monetaryスコア（5=高額, 1=低額）"),
    ("gd_rfm", "rfm_segment", "RFMセグメント（ロイヤル顧客/優良顧客/新規顧客/離反リスク/休眠顧客/一般顧客）"),
]

for table, column, comment in column_comments:
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{comment}'")

print(f"✅ {len(column_comments)} カラムにコメントを設定しました")

# COMMAND ----------

# DBTITLE 1,コメント確認
display(spark.sql(f"DESCRIBE TABLE EXTENDED sl_stores"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. PK / FK 制約

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Unity Catalog の PK/FK 制約</strong><br>
# MAGIC Unity Catalog ではテーブルに主キー（PK）と外部キー（FK）を宣言できます。<br>
# MAGIC <strong>現時点では「情報的制約（Informational）」</strong> — データ書き込み時に強制はされません。<br>
# MAGIC しかし以下のメリットがあります:
# MAGIC <ul>
# MAGIC   <li><strong>ドキュメント</strong>: テーブル間のリレーションシップがカタログエクスプローラで可視化される</li>
# MAGIC   <li><strong>クエリ最適化</strong>: オプティマイザがJOINの最適化に利用する</li>
# MAGIC   <li><strong>BI連携</strong>: Genie や BI ツールがリレーションシップを自動認識する</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,PK制約 — sl_stores
spark.sql(f"ALTER TABLE sl_stores ALTER COLUMN store_id SET NOT NULL")
spark.sql(f"ALTER TABLE sl_stores DROP CONSTRAINT IF EXISTS pk_stores CASCADE")
spark.sql(f"ALTER TABLE sl_stores ADD CONSTRAINT pk_stores PRIMARY KEY (store_id)")
print("✅ sl_stores: PK(store_id)")

# COMMAND ----------

# DBTITLE 1,PK制約 — sl_products
spark.sql(f"ALTER TABLE sl_products ALTER COLUMN product_id SET NOT NULL")
spark.sql(f"ALTER TABLE sl_products DROP CONSTRAINT IF EXISTS pk_products CASCADE")
spark.sql(f"ALTER TABLE sl_products ADD CONSTRAINT pk_products PRIMARY KEY (product_id)")
print("✅ sl_products: PK(product_id)")

# COMMAND ----------

# DBTITLE 1,PK制約 — sl_customers
spark.sql(f"ALTER TABLE sl_customers ALTER COLUMN customer_id SET NOT NULL")
spark.sql(f"ALTER TABLE sl_customers DROP CONSTRAINT IF EXISTS pk_customers CASCADE")
spark.sql(f"ALTER TABLE sl_customers ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id)")
print("✅ sl_customers: PK(customer_id)")

# COMMAND ----------

# DBTITLE 1,PK制約 — sl_sales
spark.sql(f"ALTER TABLE sl_sales ALTER COLUMN sale_id SET NOT NULL")
spark.sql(f"ALTER TABLE sl_sales DROP CONSTRAINT IF EXISTS pk_sales CASCADE")
spark.sql(f"ALTER TABLE sl_sales ADD CONSTRAINT pk_sales PRIMARY KEY (sale_id)")
print("✅ sl_sales: PK(sale_id)")

# COMMAND ----------

# DBTITLE 1,PK制約 — sl_sale_items
spark.sql(f"ALTER TABLE sl_sale_items ALTER COLUMN sale_item_id SET NOT NULL")
spark.sql(f"ALTER TABLE sl_sale_items DROP CONSTRAINT IF EXISTS pk_sale_items CASCADE")
spark.sql(f"ALTER TABLE sl_sale_items ADD CONSTRAINT pk_sale_items PRIMARY KEY (sale_item_id)")
print("✅ sl_sale_items: PK(sale_item_id)")

# COMMAND ----------

# DBTITLE 1,FK制約 — sl_sales → sl_stores
spark.sql(f"ALTER TABLE sl_sales DROP CONSTRAINT IF EXISTS fk_sales_store")
spark.sql(f"""
    ALTER TABLE sl_sales
    ADD CONSTRAINT fk_sales_store FOREIGN KEY (store_id) REFERENCES sl_stores(store_id)
""")
print("✅ sl_sales.store_id → sl_stores.store_id")

# COMMAND ----------

# DBTITLE 1,FK制約 — sl_sale_items → sl_sales / sl_products
spark.sql(f"ALTER TABLE sl_sale_items DROP CONSTRAINT IF EXISTS fk_items_sale")
spark.sql(f"""
    ALTER TABLE sl_sale_items
    ADD CONSTRAINT fk_items_sale FOREIGN KEY (sale_id) REFERENCES sl_sales(sale_id)
""")

spark.sql(f"ALTER TABLE sl_sale_items DROP CONSTRAINT IF EXISTS fk_items_product")
spark.sql(f"""
    ALTER TABLE sl_sale_items
    ADD CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES sl_products(product_id)
""")
print("✅ sl_sale_items.sale_id → sl_sales.sale_id")
print("✅ sl_sale_items.product_id → sl_products.product_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold テーブル

# COMMAND ----------

# DBTITLE 1,PK制約 — gd_rfm
spark.sql(f"ALTER TABLE gd_rfm ALTER COLUMN customer_id SET NOT NULL")
spark.sql(f"ALTER TABLE gd_rfm DROP CONSTRAINT IF EXISTS pk_rfm CASCADE")
spark.sql(f"ALTER TABLE gd_rfm ADD CONSTRAINT pk_rfm PRIMARY KEY (customer_id)")
print("✅ gd_rfm: PK(customer_id)")

# COMMAND ----------

# DBTITLE 1,PK制約 — gd_store_trade_area
spark.sql(f"ALTER TABLE gd_store_trade_area ALTER COLUMN store_id SET NOT NULL")
spark.sql(f"ALTER TABLE gd_store_trade_area DROP CONSTRAINT IF EXISTS pk_store_trade_area CASCADE")
spark.sql(f"ALTER TABLE gd_store_trade_area ADD CONSTRAINT pk_store_trade_area PRIMARY KEY (store_id)")
print("✅ gd_store_trade_area: PK(store_id)")

# COMMAND ----------

# DBTITLE 1,PK制約 — gd_store_sales_summary
spark.sql(f"ALTER TABLE gd_store_sales_summary ALTER COLUMN store_id SET NOT NULL")
spark.sql(f"ALTER TABLE gd_store_sales_summary ALTER COLUMN sale_month SET NOT NULL")
spark.sql(f"ALTER TABLE gd_store_sales_summary DROP CONSTRAINT IF EXISTS pk_store_sales_summary CASCADE")
spark.sql(f"ALTER TABLE gd_store_sales_summary ADD CONSTRAINT pk_store_sales_summary PRIMARY KEY (store_id, sale_month)")
print("✅ gd_store_sales_summary: PK(store_id, sale_month)")

# COMMAND ----------

# DBTITLE 1,PK制約 — gd_category_sales
spark.sql(f"ALTER TABLE gd_category_sales ALTER COLUMN category_id SET NOT NULL")
spark.sql(f"ALTER TABLE gd_category_sales ALTER COLUMN sale_month SET NOT NULL")
spark.sql(f"ALTER TABLE gd_category_sales DROP CONSTRAINT IF EXISTS pk_category_sales CASCADE")
spark.sql(f"ALTER TABLE gd_category_sales ADD CONSTRAINT pk_category_sales PRIMARY KEY (category_id, sale_month)")
print("✅ gd_category_sales: PK(category_id, sale_month)")

# COMMAND ----------

# DBTITLE 1,PK制約 — gd_weather_sales
spark.sql(f"ALTER TABLE gd_weather_sales ALTER COLUMN weather SET NOT NULL")
spark.sql(f"ALTER TABLE gd_weather_sales ALTER COLUMN prefecture SET NOT NULL")
spark.sql(f"ALTER TABLE gd_weather_sales ALTER COLUMN `month` SET NOT NULL")
spark.sql(f"ALTER TABLE gd_weather_sales DROP CONSTRAINT IF EXISTS pk_weather_sales CASCADE")
spark.sql(f"ALTER TABLE gd_weather_sales ADD CONSTRAINT pk_weather_sales PRIMARY KEY (weather, prefecture, `month`)")
print("✅ gd_weather_sales: PK(weather, prefecture, month)")

# COMMAND ----------

# DBTITLE 1,FK制約 — gd_rfm → sl_customers
spark.sql(f"ALTER TABLE gd_rfm DROP CONSTRAINT IF EXISTS fk_rfm_customer")
spark.sql(f"""
    ALTER TABLE gd_rfm
    ADD CONSTRAINT fk_rfm_customer FOREIGN KEY (customer_id) REFERENCES sl_customers(customer_id)
""")
print("✅ gd_rfm.customer_id → sl_customers.customer_id")

# COMMAND ----------

# DBTITLE 1,FK制約 — gd_store_trade_area → sl_stores
spark.sql(f"ALTER TABLE gd_store_trade_area DROP CONSTRAINT IF EXISTS fk_trade_area_store")
spark.sql(f"""
    ALTER TABLE gd_store_trade_area
    ADD CONSTRAINT fk_trade_area_store FOREIGN KEY (store_id) REFERENCES sl_stores(store_id)
""")
print("✅ gd_store_trade_area.store_id → sl_stores.store_id")

# COMMAND ----------

# DBTITLE 1,FK制約 — gd_store_sales_summary → sl_stores
spark.sql(f"ALTER TABLE gd_store_sales_summary DROP CONSTRAINT IF EXISTS fk_sales_summary_store")
spark.sql(f"""
    ALTER TABLE gd_store_sales_summary
    ADD CONSTRAINT fk_sales_summary_store FOREIGN KEY (store_id) REFERENCES sl_stores(store_id)
""")
print("✅ gd_store_sales_summary.store_id → sl_stores.store_id")

# COMMAND ----------

# DBTITLE 1,制約確認
display(spark.sql(f"""
    SELECT table_name, constraint_name, constraint_type
    FROM information_schema.table_constraints
    WHERE table_schema = '{MY_SCHEMA}'
    ORDER BY table_name, constraint_type
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. カラムマスキング

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 カラムマスキングとは</strong><br>
# MAGIC 特定のカラムの値を、ユーザーの権限に応じて<strong>自動的にマスクして表示</strong>する機能です。<br>
# MAGIC 例: 管理者グループには氏名をそのまま表示し、一般ユーザーには「***」と表示する。<br>
# MAGIC データを物理的に変更せず、アクセス制御で見え方を変えることで、個人情報保護を実現します。<br><br>
# MAGIC 📖 <a href="https://docs.databricks.com/ja/tables/row-filters-and-column-masks.html" target="_blank">行フィルタ・カラムマスキング ドキュメント</a>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,マスキング関数の作成
spark.sql(f"""
    CREATE OR REPLACE FUNCTION mask_name(name STRING)
    RETURNS STRING
    RETURN
      CASE
        WHEN is_account_group_member('admins') THEN name
        ELSE CONCAT(LEFT(name, 1), '***')
      END
""")
print("✅ マスキング関数を作成しました")

# COMMAND ----------

# DBTITLE 1,カラムマスキングを適用
spark.sql(f"""
    ALTER TABLE sl_customers
    ALTER COLUMN name SET MASK mask_name
""")
print("✅ sl_customers.name にカラムマスキングを適用しました")

# COMMAND ----------

# DBTITLE 1,マスキング動作確認
display(spark.sql(f"""
    SELECT customer_id, name, gender, prefecture
    FROM sl_customers
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ マスキングの解除（ハンズオン後のクリーンアップ用）</strong><br>
# MAGIC マスキングを解除する場合は以下を実行してください:
# MAGIC <pre>spark.sql(f"ALTER TABLE sl_customers ALTER COLUMN name DROP MASK")</pre>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ テーブル設定完了</strong><br>
# MAGIC PK/FK制約、テーブル/カラムコメント、カラムマスキングを設定しました。<br>
# MAGIC カタログエクスプローラで制約やコメントが反映されていることを確認してみてください。<br>
# MAGIC 次のノートブック（06_Genie_ダッシュボード）で、GenieとAI/BIダッシュボードを体験します。
# MAGIC </div>
