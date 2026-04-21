# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">06 | Genie & AI/BI ダッシュボード</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">自然言語で問い合わせ & ノーコードでダッシュボード作成</p>
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
# MAGIC Goldテーブルを使って、Genie Space（自然言語BI）と AI/BI ダッシュボードを作成します。<br>
# MAGIC このノートブックでは <strong>設定手順のガイド</strong> と <strong>動作確認クエリ</strong> を行います。<br>
# MAGIC 実際のGenie Space・ダッシュボード作成はUI操作で行います。
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Genie Space

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Genie Space とは</strong><br>
# MAGIC テーブルを登録するだけで、<strong>自然言語（日本語OK）</strong>でデータに問い合わせできるAI機能です。<br>
# MAGIC SQLを書けない業務担当者でも「先月の売上トップ3店舗は？」と聞くだけでデータを取得できます。<br><br>
# MAGIC <strong>Genieが賢く動くために重要なこと:</strong>
# MAGIC <ul>
# MAGIC   <li>テーブル/カラムに<strong>コメント</strong>を付ける（前のノートブックで設定済み）</li>
# MAGIC   <li><strong>PK/FK制約</strong>を宣言する（テーブル間のリレーションをGenieが理解）</li>
# MAGIC   <li><strong>サンプル質問</strong>を登録する（回答精度が向上）</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Genie Space を作成する</strong><br><br>
# MAGIC <strong>手順:</strong>
# MAGIC <ol>
# MAGIC   <li>左メニューの「<strong>Genie</strong>」をクリック</li>
# MAGIC   <li>「<strong>New</strong>」→「<strong>Genie space</strong>」をクリック</li>
# MAGIC   <li>名前を入力（例: 「ホームセンター商圏分析」）</li>
# MAGIC   <li>テーブルを追加:
# MAGIC     <ul>
# MAGIC       <li><code>gd_store_sales_summary</code></li>
# MAGIC       <li><code>gd_rfm</code></li>
# MAGIC       <li><code>gd_store_trade_area</code></li>
# MAGIC       <li><code>gd_category_sales</code></li>
# MAGIC       <li><code>sl_stores</code></li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC   <li>SQLウェアハウスを選択</li>
# MAGIC   <li>「<strong>Save</strong>」をクリック</li>
# MAGIC </ol>
# MAGIC <br>
# MAGIC <strong>試してみる質問:</strong>
# MAGIC <ul>
# MAGIC   <li>「売上トップ5の店舗を教えて」</li>
# MAGIC   <li>「東京都の店舗の月別売上推移を見せて」</li>
# MAGIC   <li>「ロイヤル顧客は何人いる？」</li>
# MAGIC   <li>「園芸用品カテゴリの売上が一番多い月は？」</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Genieが参照するテーブルの確認
# MAGIC %sql
# MAGIC -- Genieに登録するテーブルのデータを確認
# MAGIC SELECT 'gd_store_sales_summary' AS table_name, count(*) AS row_count FROM ${catalog}.${schema}.gd_store_sales_summary
# MAGIC UNION ALL
# MAGIC SELECT 'gd_rfm', count(*) FROM ${catalog}.${schema}.gd_rfm
# MAGIC UNION ALL
# MAGIC SELECT 'gd_store_trade_area', count(*) FROM ${catalog}.${schema}.gd_store_trade_area
# MAGIC UNION ALL
# MAGIC SELECT 'gd_category_sales', count(*) FROM ${catalog}.${schema}.gd_category_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. AI/BI ダッシュボード

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 AI/BI ダッシュボードとは</strong><br>
# MAGIC Databricksのノーコード/ローコードBIツールです。<br>
# MAGIC <ul>
# MAGIC   <li>SQLで定義したデータセットを元に、ドラッグ&ドロップでチャートを作成</li>
# MAGIC   <li>AI アシスタントが「このデータに適したチャート」を提案してくれる</li>
# MAGIC   <li>フィルタ・パラメータで動的に絞り込み可能</li>
# MAGIC   <li>公開してチームメンバーと共有可能</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: AI/BI ダッシュボードを作成する</strong><br><br>
# MAGIC <strong>手順:</strong>
# MAGIC <ol>
# MAGIC   <li>左メニューの「<strong>Dashboards</strong>」をクリック</li>
# MAGIC   <li>「<strong>Create dashboard</strong>」をクリック</li>
# MAGIC   <li>名前を入力（例: 「ホームセンター商圏分析ダッシュボード」）</li>
# MAGIC   <li>「<strong>Data</strong>」タブでデータセットを追加（以下のSQLを使用）</li>
# MAGIC   <li>「<strong>Canvas</strong>」タブでウィジェットを配置</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,ダッシュボード用クエリ① 店舗別KPI
# MAGIC %sql
# MAGIC -- データセット: 店舗KPI一覧
# MAGIC SELECT
# MAGIC   store_name,
# MAGIC   store_prefecture,
# MAGIC   store_type,
# MAGIC   total_sales,
# MAGIC   unique_customers,
# MAGIC   total_transactions,
# MAGIC   competitor_count,
# MAGIC   ROUND(total_sales / unique_customers, 0) AS sales_per_customer
# MAGIC FROM ${catalog}.${schema}.gd_store_trade_area
# MAGIC ORDER BY total_sales DESC

# COMMAND ----------

# DBTITLE 1,ダッシュボード用クエリ② 月別売上トレンド
# MAGIC %sql
# MAGIC -- データセット: 月別売上トレンド
# MAGIC SELECT
# MAGIC   sale_month,
# MAGIC   SUM(total_sales) AS monthly_sales,
# MAGIC   SUM(tx_count) AS monthly_tx,
# MAGIC   SUM(unique_customers) AS monthly_customers
# MAGIC FROM ${catalog}.${schema}.gd_store_sales_summary
# MAGIC GROUP BY sale_month
# MAGIC ORDER BY sale_month

# COMMAND ----------

# DBTITLE 1,ダッシュボード用クエリ③ RFMセグメント分布
# MAGIC %sql
# MAGIC -- データセット: RFMセグメント分布
# MAGIC SELECT
# MAGIC   rfm_segment,
# MAGIC   count(*) AS customer_count,
# MAGIC   ROUND(AVG(monetary), 0) AS avg_monetary,
# MAGIC   ROUND(AVG(frequency), 1) AS avg_frequency,
# MAGIC   ROUND(AVG(recency), 0) AS avg_recency_days
# MAGIC FROM ${catalog}.${schema}.gd_rfm
# MAGIC GROUP BY rfm_segment
# MAGIC ORDER BY customer_count DESC

# COMMAND ----------

# DBTITLE 1,ダッシュボード用クエリ④ カテゴリ別売上構成
# MAGIC %sql
# MAGIC -- データセット: カテゴリ別売上構成
# MAGIC SELECT
# MAGIC   category_name,
# MAGIC   SUM(total_sales) AS total_sales,
# MAGIC   SUM(total_qty) AS total_qty,
# MAGIC   SUM(tx_count) AS tx_count
# MAGIC FROM ${catalog}.${schema}.gd_category_sales
# MAGIC GROUP BY category_name
# MAGIC ORDER BY total_sales DESC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ダッシュボードの推奨レイアウト</strong><br><br>
# MAGIC <table style="border-collapse: collapse; width: 100%;">
# MAGIC   <tr style="background: #1B3139; color: white;">
# MAGIC     <th style="padding: 8px;">位置</th>
# MAGIC     <th style="padding: 8px;">ウィジェット</th>
# MAGIC     <th style="padding: 8px;">チャート種別</th>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 8px;">上段左</td>
# MAGIC     <td style="padding: 8px;">KPIカウンター（総売上 / 総顧客数 / 総取引数）</td>
# MAGIC     <td style="padding: 8px;">Counter</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 8px;">中段左</td>
# MAGIC     <td style="padding: 8px;">月別売上トレンド</td>
# MAGIC     <td style="padding: 8px;">折れ線グラフ</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 8px;">中段右</td>
# MAGIC     <td style="padding: 8px;">カテゴリ別売上構成</td>
# MAGIC     <td style="padding: 8px;">円グラフ / 棒グラフ</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 8px;">下段左</td>
# MAGIC     <td style="padding: 8px;">RFMセグメント分布</td>
# MAGIC     <td style="padding: 8px;">棒グラフ</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 8px;">下段右</td>
# MAGIC     <td style="padding: 8px;">店舗別KPIテーブル</td>
# MAGIC     <td style="padding: 8px;">テーブル</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC <br>
# MAGIC 💡 <strong>Tips:</strong> 「AIで生成」ボタンを使うと、データセットに最適なチャートをAIが提案してくれます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Genie & ダッシュボード作成完了</strong><br>
# MAGIC Genie Spaceで自然言語クエリ、AI/BIダッシュボードでビジュアル分析ができるようになりました。<br>
# MAGIC 次のノートブック（07_Jobsワークフロー）で、ETLパイプラインの自動化を体験します。
# MAGIC </div>
