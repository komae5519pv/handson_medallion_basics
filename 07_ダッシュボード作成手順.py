# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">07 | AI/BI ダッシュボード作成手順</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">AIにプロンプトを渡すだけ — ノーコードでダッシュボードを作成する</p>
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
# MAGIC AI/BI ダッシュボードの<strong>エージェントモード</strong>を使って、プロンプト1つでダッシュボードを自動生成します。<br>
# MAGIC テーブルの内容を理解し、適切なチャートとレイアウトをAIが提案してくれることを体験しましょう！
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. AI/BI ダッシュボードとは

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 AI/BI ダッシュボードとは</strong><br>
# MAGIC Databricks のノーコード BI ツールです。<br>
# MAGIC <ul>
# MAGIC   <li>SQLで定義したデータセットを元に、ドラッグ&ドロップでチャートを作成</li>
# MAGIC   <li><strong>Genie Code</strong>にプロンプトを渡すと、データセット・チャート・レイアウトを自動生成</li>
# MAGIC   <li>フィルタ・パラメータで動的に絞り込み可能</li>
# MAGIC   <li>公開してチームメンバーと共有可能</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. 使用するテーブルの確認

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 ダッシュボードのデータソース</strong><br>
# MAGIC AIにダッシュボードを作らせるとき、テーブルのカタログ・スキーマ名をプロンプトに含めます。<br>
# MAGIC テーブルコメント・カラムコメントが設定済みなので、AIがデータの意味を理解して適切なチャートを提案してくれます。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,ダッシュボードに使うテーブル一覧
tables = [
    ("gd_store_trade_area",    "店舗別 商圏サマリ（売上・顧客数・競合店数・周辺施設）"),
    ("gd_store_sales_summary", "店舗×月別 売上サマリ（取引件数・顧客数・売上）"),
    ("gd_category_sales",      "カテゴリ×月別 売上（園芸用品/工具/塗料/木材/日用品）"),
    ("gd_rfm",                 "RFM分析結果（顧客セグメント別の特徴）"),
    ("gd_weather_sales",       "天気×売上 相関分析（気温・降水量と売上の関係）"),
]

print(f"カタログ: {MY_CATALOG}")
print(f"スキーマ: {MY_SCHEMA}")
print(f"{'─' * 60}")
for table, desc in tables:
    fqn = f"{MY_CATALOG}.{MY_SCHEMA}.{table}"
    count = spark.table(table).count()
    print(f"  {fqn:<55} {count:>6,} 件")
    print(f"    └ {desc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. ダッシュボードの作成手順

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: AIエージェントでダッシュボードを作成する</strong><br><br>
# MAGIC <strong>手順:</strong>
# MAGIC <ol>
# MAGIC   <li>左メニューの「<strong>Dashboards</strong>」をクリック</li>
# MAGIC   <li>右上の「<strong>Create dashboard</strong>」をクリック</li>
# MAGIC   <li>空のダッシュボードが開いたら、右下の <strong>Genie Code（💬アイコン）</strong> をクリック</li>
# MAGIC   <li>下のセルに表示される<strong>プロンプトをコピー</strong>して貼り付け</li>
# MAGIC   <li>AIがデータセット・チャート・レイアウトを自動生成するのを待つ</li>
# MAGIC   <li>生成されたダッシュボードを確認し、必要に応じて調整</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ プロンプト A（シンプル版） — まずはこちらを試してください</strong><br>
# MAGIC テーブルのスキーマだけ渡して、AIにお任せでダッシュボードを作らせます。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,プロンプト A: シンプル版
prompt_simple = f"""{MY_CATALOG}.{MY_SCHEMA} スキーマ配下のテーブルを使って、ホームセンターの運営会社の本部が使う商圏分析ダッシュボードを作って"""

print(prompt_simple)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 シンプル版のポイント</strong><br>
# MAGIC テーブルにコメントや PK/FK 制約を設定済みなので、スキーマ名を渡すだけでもAIがデータの意味を理解し、それなりのダッシュボードを作ってくれます。<br>
# MAGIC ただし、チャートの種類やレイアウトはAI任せになるため、意図と違う結果になることもあります。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ プロンプト B（詳細指定版） — より具体的に指示する場合はこちら</strong><br>
# MAGIC チャートの種類やレイアウトまで指定すると、意図に近いダッシュボードが生成されます。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,プロンプト B: 詳細指定版
prompt = f"""以下のテーブルを使って、ホームセンターチェーンの本部向け「商圏分析ダッシュボード」を作ってください。

■ テーブル（すべて {MY_CATALOG}.{MY_SCHEMA} 配下）:
- gd_store_trade_area: 店舗別の商圏サマリ（売上・顧客数・競合店数・駅数・周辺施設距離・売場面積・店舗タイプ）
- gd_store_sales_summary: 店舗×月別の売上サマリ（取引件数・ユニーク顧客数・売上合計・平均客単価）
- gd_category_sales: 商品カテゴリ×月別の売上（園芸用品/工具/塗料/木材/日用品）
- gd_rfm: 顧客RFM分析結果（ロイヤル/優良/新規/離反リスク/休眠/一般の6セグメント）
- gd_weather_sales: 天気×都道府県×月別の売上（気温・降水量との相関）

■ ダッシュボードの構成:
1. 上段: 全店KPIカウンター（総売上・総顧客数・総取引数・平均客単価）
2. 月別売上トレンド（折れ線グラフ）
3. 店舗タイプ別（大型/標準/コンパクト）の売上比較（棒グラフ）
4. 商品カテゴリ別の売上構成（円グラフまたは棒グラフ）
5. 商圏分析: 競合店数と売上の関係（散布図）
6. RFMセグメント別の顧客数分布（棒グラフ）
7. 店舗別KPI一覧テーブル（売上・顧客数・競合店数・売場面積）

■ 表示ルール:
- 金額は万円単位で表示（ROUND(value / 10000, 1)）
- 店舗タイプは「大型」「標準」「コンパクト」の3種類
- ダッシュボードのタイトルは「ホームセンター商圏分析」"""

print(prompt)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 プロンプトのポイント</strong><br>
# MAGIC <ul>
# MAGIC   <li><strong>テーブル名をフルパスで指定</strong>: AIがデータソースを正確に参照できる</li>
# MAGIC   <li><strong>各テーブルの内容を簡潔に説明</strong>: カラムの意味をAIが理解しやすくなる</li>
# MAGIC   <li><strong>チャートの種類を指定</strong>: 「散布図」「折れ線」など具体的に書くと精度が上がる</li>
# MAGIC   <li><strong>表示ルールを明示</strong>: 万円単位など、ビジネス要件をプロンプトに含める</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. 生成されたダッシュボードの確認

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 確認ポイント</strong><br>
# MAGIC AIが生成したダッシュボードを以下の観点で確認してください:
# MAGIC <ul>
# MAGIC   <li>データセットのSQL: テーブル結合やフィルタは正しいか？</li>
# MAGIC   <li>チャートの種類: データの特性に合ったチャートが選ばれているか？</li>
# MAGIC   <li>レイアウト: 情報の優先度に沿った配置になっているか？</li>
# MAGIC   <li>金額の単位: 万円単位で表示されているか？</li>
# MAGIC </ul>
# MAGIC <br>
# MAGIC 修正したい場合は、Genie Codeに追加の指示を出すか、手動で編集できます。<br>
# MAGIC 例: 「競合店数のフィルタを追加して」「テーブルに売場面積の列も追加して」
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 うまくいかないときは</strong><br>
# MAGIC <ul>
# MAGIC   <li>AIが生成した<strong>データセットのSQLを確認</strong>して、テーブル名が正しいか見てみましょう</li>
# MAGIC   <li>チャートの種類が意図と違う場合は「この chart を棒グラフに変えて」と指示できます</li>
# MAGIC   <li>一度に全部作らず、「まず売上トレンドだけ作って」と段階的に依頼するのも有効です</li>
# MAGIC   <li>手動でデータセットを追加し、チャートを配置することもできます</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. ダッシュボードに Genie を連携する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 ダッシュボード × Genie 連携とは</strong><br>
# MAGIC ダッシュボードに Genie スペースを紐づけると、閲覧者がダッシュボード上で<strong>自然言語で追加の質問</strong>ができるようになります。<br>
# MAGIC 「このグラフの内訳は？」「先月との比較は？」など、チャートを見ながら深掘りできます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Genie スペースをダッシュボードにリンクする</strong><br><br>
# MAGIC <strong>Step 1:</strong> 前のノートブック（06）で作成した <strong>Genie スペース</strong> を開きます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 Step 2:</strong> Genie スペースの右上「<strong>共有</strong>」をクリックし、左下の「<strong>リンクをコピー</strong>」でURLを取得します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ![](./_images/step1_copy_genie_link.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 Step 3:</strong> ダッシュボードの編集画面に戻り、以下の手順で Genie をリンクします。
# MAGIC <ol>
# MAGIC   <li>右上の「<strong>⋮</strong>」メニュー →「<strong>設定とテーマ</strong>」をクリック</li>
# MAGIC   <li>「<strong>Genieを有効にしてください</strong>」のトグルを <strong>ON</strong></li>
# MAGIC   <li>「<strong>既存のGenie spaceをリンク</strong>」を選択</li>
# MAGIC   <li>コピーしたURLから <strong>Genie スペースID</strong> を貼り付け</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ![](./_images/step2_add_genie_url.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 完成イメージ</strong><br>
# MAGIC Genie連携後、ダッシュボードを公開すると閲覧者が自然言語で質問できるようになります。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ![](./_images/step3_dashboard.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ ダッシュボード作成完了</strong><br>
# MAGIC Genie Codeにプロンプトを渡すだけで、データの特性に合ったダッシュボードが自動生成されることを体験しました。<br>
# MAGIC テーブルコメントや PK/FK 制約が設定済みであるほど、AIの生成精度が向上します。<br>
# MAGIC さらに Genie を連携することで、閲覧者が自然言語でダッシュボードを深掘りできるようになります。
# MAGIC </div>
