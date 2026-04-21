# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">09 | Genie Code インタラクティブ分析</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">エージェントモードでプロンプトを投げるだけ — AIがコードも洞察も自動生成</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 15 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 手順</strong>
# MAGIC <ol>
# MAGIC   <li>右上の <strong>Genie Code アイコン（💬）</strong> をクリック</li>
# MAGIC   <li>入力欄の左にある <strong>エージェントモードのトグルを ON</strong> にする</li>
# MAGIC   <li>下のプロンプトをコピーして貼り付け（<code>{catalog}</code> と <code>{schema}</code> はご自身の環境に合わせて置き換えてください）</li>
# MAGIC   <li>Genie が自動的にセルを生成・実行し、分析結果と洞察を返します</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト A: 商圏の全体像を把握する

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {catalog}.{schema} スキーマにあるテーブルを使って、ホームセンターの商圏分析を行って。
# MAGIC まずテーブル一覧とスキーマを確認し、店舗タイプ別の売上比較と競合店数の関係をグラフで可視化して。
# MAGIC 各セルの実行結果の直後に、マークダウンセルで結果の解釈と商圏戦略上の示唆をコメントして。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト B: 顧客セグメントを深掘りする

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {catalog}.{schema} の gd_rfm テーブルと gd_store_trade_area テーブルを使って、
# MAGIC RFMセグメントごとの顧客数と購買特性を分析して。
# MAGIC さらに、駅近の店舗と郊外の店舗で顧客セグメントの構成比に違いがあるか比較して。
# MAGIC 分析結果ごとにマークダウンでビジネス上の示唆を添えて。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト C: 天候 x 売上の関係を探る

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {catalog}.{schema} の gd_weather_sales テーブルと gd_category_sales テーブルを分析して。
# MAGIC 天気ごとに売上がどう変わるか、カテゴリ別に可視化して。
# MAGIC 雨の日に売上が伸びるカテゴリと落ちるカテゴリを特定し、天候に応じた品揃え戦略を提案して。
# MAGIC 各分析ステップの結果にマークダウンで解釈を付けて。
# MAGIC ```
