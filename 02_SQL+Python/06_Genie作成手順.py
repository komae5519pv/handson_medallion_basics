# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">06 | Genie スペース作成手順</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">テーブルに日本語で話しかける — 自然言語BIを体験する</p>
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
# MAGIC ここまでに作ったテーブルを使って、<strong>Genie スペース</strong>を作成します。<br>
# MAGIC Genie に日本語で話しかけて「SQLを書かずにデータ分析」する体験をしましょう！<br>
# MAGIC <ul>
# MAGIC   <li>Genie スペースの作成手順を確認する</li>
# MAGIC   <li>General Instructions（Genieへの指示）を設定する</li>
# MAGIC   <li>サンプル質問で「Wow！」を体験する</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Genie とは

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Genie Space とは</strong><br>
# MAGIC テーブルを登録するだけで、<strong>自然言語（日本語OK）</strong>でデータに問い合わせできるAI機能です。<br>
# MAGIC SQLを書けない業務担当者でも「先月の売上トップ3店舗は？」と聞くだけでデータを取得できます。<br><br>
# MAGIC <strong>Genieが賢く動くためのポイント:</strong>
# MAGIC <ul>
# MAGIC   <li>テーブル/カラムに<strong>コメント</strong>を付ける（05_テーブル設定で設定済み）</li>
# MAGIC   <li><strong>PK/FK制約</strong>を宣言する（テーブル間のリレーションをGenieが理解）</li>
# MAGIC   <li><strong>General Instructions</strong>でGenieの振る舞いを指示する</li>
# MAGIC   <li><strong>サンプル質問</strong>を登録する（回答精度が向上）</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Genie スペースの作成手順

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Genie スペースを作成する</strong><br><br>
# MAGIC <strong>手順:</strong>
# MAGIC <ol>
# MAGIC   <li>左メニューの「<strong>Genie</strong>」をクリック</li>
# MAGIC   <li>「<strong>New</strong>」→「<strong>Genie space</strong>」をクリック</li>
# MAGIC   <li>タイトルを入力: <strong>ホームセンター商圏分析</strong></li>
# MAGIC   <li>テーブルを追加（下の7テーブル）</li>
# MAGIC   <li>SQLウェアハウスを選択</li>
# MAGIC   <li>「<strong>Save</strong>」をクリック</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #7B1FA2; background: #F3E5F5; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔧 設定内容</strong>
# MAGIC <table style="width: 100%; border-collapse: collapse; border-radius: 8px; overflow: hidden; margin-top: 8px;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #7B1FA2; color: white;">
# MAGIC       <th style="padding: 10px 15px; text-align: left;">設定項目</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">値</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>タイトル</b></td>
# MAGIC       <td style="padding: 8px 15px;">ホームセンター商圏分析</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;"><b>説明</b></td>
# MAGIC       <td style="padding: 8px 15px;">ホームセンターチェーンの商圏・売上・顧客・天候データを自然言語で分析できます</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>テーブル</b></td>
# MAGIC       <td style="padding: 8px 15px;">
# MAGIC         <code>sl_stores</code><br/>
# MAGIC         <code>sl_customers</code><br/>
# MAGIC         <code>gd_store_sales_summary</code><br/>
# MAGIC         <code>gd_rfm</code><br/>
# MAGIC         <code>gd_store_trade_area</code><br/>
# MAGIC         <code>gd_category_sales</code><br/>
# MAGIC         <code>gd_weather_sales</code>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. General Instructions の設定

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 General Instructions とは</strong><br>
# MAGIC Genie への「業務ルール」や「回答スタイル」の指示です。<br>
# MAGIC ここに書いた内容を踏まえて、Genie がSQLを生成します。<br>
# MAGIC 例えば「金額は万円で表示して」と書くと、Genie は <code>ROUND(amount / 10000, 1)</code> のようなSQLを生成します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ General Instructions — 以下をそのままコピーしてください</strong><br>
# MAGIC Genie スペースの設定画面 →「<strong>Instructions</strong>」タブに貼り付けます。
# MAGIC </div>
# MAGIC
# MAGIC ```
# MAGIC あなたはホームセンターチェーンの商圏分析アシスタントです。
# MAGIC エリアマネージャーや店長からの質問に、売上データ・顧客分析・天候データ・周辺施設情報を使って回答します。
# MAGIC
# MAGIC ■ 対応できる分析:
# MAGIC - 商圏分析: 競合店数・最寄り駅距離・周辺施設数と売上の関係
# MAGIC - 店舗立地評価: 売場面積・店舗タイプ（大型/標準/コンパクト）・都道府県別の比較
# MAGIC - 売上分析: 店舗別・月別・カテゴリ別（園芸用品/工具/塗料/木材/日用品）の実績
# MAGIC - 顧客分析: RFMセグメント（ロイヤル/優良/新規/離反リスク/休眠/一般）ごとの特徴
# MAGIC - 天候影響: 天気・気温・降水量と売上・カテゴリの相関
# MAGIC
# MAGIC ■ 回答のルール:
# MAGIC - 金額は日本円で、万円単位（例: 1,234万円）で表示してください
# MAGIC - 距離はkm単位で表示してください
# MAGIC - 店舗名はそのまま使ってください（例: 店舗_001）
# MAGIC - 店舗タイプは「大型」「標準」「コンパクト」の3種類です
# MAGIC - 表やグラフで視覚的にわかりやすく回答してください
# MAGIC - 分析結果には、商圏戦略上の示唆も一言添えてください
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. サンプル質問を登録する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 サンプル質問を登録する理由</strong><br>
# MAGIC サンプル質問は「こういう聞き方にはこう答えて」というお手本です。<br>
# MAGIC 登録しておくと、似た質問が来たときに Genie の回答精度がぐっと上がります。<br>
# MAGIC Genie スペースの設定画面 →「<strong>Sample questions</strong>」タブに登録します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 通常モード vs エージェントモード</strong><br>
# MAGIC Genie には 2 つのモードがあります:
# MAGIC <table style="margin-top: 8px; border-collapse: collapse; width: 100%;">
# MAGIC   <tr style="background: #1B3139; color: white;">
# MAGIC     <th style="padding: 8px; text-align: left;">モード</th>
# MAGIC     <th style="padding: 8px; text-align: left;">得意なこと</th>
# MAGIC     <th style="padding: 8px; text-align: left;">動き方</th>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 8px;"><strong>通常モード</strong></td>
# MAGIC     <td style="padding: 8px;">1つのテーブルに対するシンプルな質問</td>
# MAGIC     <td style="padding: 8px;">SQLを1本生成して即回答</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 8px;"><strong>エージェントモード</strong></td>
# MAGIC     <td style="padding: 8px;">複数テーブルを跨ぐ複雑な分析</td>
# MAGIC     <td style="padding: 8px;">質問を分解 → 複数SQLを順番に実行 → 結果を統合して回答</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ サンプル質問（通常モード） — 以下を登録してください</strong>
# MAGIC </div>
# MAGIC
# MAGIC | # | サンプル質問 | 対象テーブル |
# MAGIC |---|---|---|
# MAGIC | 1 | 競合店が多い店舗トップ5を教えて | gd_store_trade_area |
# MAGIC | 2 | 売場面積が大きい店舗ほど売上は高い？ | gd_store_trade_area |
# MAGIC | 3 | 園芸用品の売上が一番多い月はいつ？ | gd_category_sales |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #7B1FA2; background: #F3E5F5; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ サンプル質問（エージェントモード） — 以下も登録してください</strong><br>
# MAGIC 複数テーブルを組み合わせる質問です。エージェントが自動的に分析計画を立てて実行します。
# MAGIC </div>
# MAGIC
# MAGIC | # | サンプル質問 | 組み合わせるテーブル |
# MAGIC |---|---|---|
# MAGIC | 4 | 競合店が多い商圏なのに売上好調な店舗はどこ？立地の特徴を分析して | gd_store_trade_area + gd_store_sales_summary |
# MAGIC | 5 | 駅近の店舗と郊外の店舗で、顧客層や売れ筋カテゴリに違いはある？ | gd_store_trade_area + gd_rfm + gd_category_sales |
# MAGIC | 6 | 雨の日に売上が伸びるカテゴリと落ちるカテゴリを比較して、天候対策を提案して | gd_weather_sales + gd_category_sales |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Genie に話しかけてみよう！

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 まずは通常モードで体験</strong><br><br>
# MAGIC Genie スペースを開いて、以下の質問を試してみてください。<br>
# MAGIC 自然言語がSQLに変換される様子に注目！<br><br>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #388E3C; padding-left: 12px; color: #2E7D32; font-style: italic; margin: 8px 0;">
# MAGIC 「競合店が一番多い商圏の店舗はどこ？」
# MAGIC </blockquote>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #388E3C; padding-left: 12px; color: #2E7D32; font-style: italic; margin: 8px 0;">
# MAGIC 「売場面積が500㎡以上の大型店舗の一覧と売上を見せて」
# MAGIC </blockquote>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #388E3C; padding-left: 12px; color: #2E7D32; font-style: italic; margin: 8px 0;">
# MAGIC 「都道府県別の店舗数と平均売上を比較して」
# MAGIC </blockquote>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #7B1FA2; background: #F3E5F5; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 次はエージェントモードを体験！</strong><br><br>
# MAGIC 入力欄の左にある <strong>エージェントモードのトグル</strong> をONにして、以下を試してください。<br>
# MAGIC Genie が質問を分解 → 複数のSQLを順番に実行 → 結果を統合して回答する様子が見られます！<br><br>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #7B1FA2; padding-left: 12px; color: #6A1B9A; font-style: italic; margin: 8px 0;">
# MAGIC 「競合店が3つ以上ある商圏で、それでも売上が好調な店舗の特徴を分析して」
# MAGIC </blockquote>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #7B1FA2; padding-left: 12px; color: #6A1B9A; font-style: italic; margin: 8px 0;">
# MAGIC 「駅近の店舗と郊外の店舗で、顧客ロイヤルティや売れ筋カテゴリに違いはある？」
# MAGIC </blockquote>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #7B1FA2; padding-left: 12px; color: #6A1B9A; font-style: italic; margin: 8px 0;">
# MAGIC 「雨の日に売上が伸びるカテゴリと落ちるカテゴリを比較して、天候に応じた品揃え戦略を提案して」
# MAGIC </blockquote>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #7B1FA2; padding-left: 12px; color: #6A1B9A; font-style: italic; margin: 8px 0;">
# MAGIC 「商圏環境が厳しい（競合多・駅遠・売場面積小）のに健闘している店舗を見つけて、成功要因を分析して」
# MAGIC </blockquote>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 うまくいかないときは</strong><br>
# MAGIC <ul>
# MAGIC   <li>質問を<strong>より具体的</strong>にしてみてください（「売上」→「月別の売上合計」）</li>
# MAGIC   <li>Genie が生成した <strong>SQL を確認</strong>して、意図通りか見てみましょう</li>
# MAGIC   <li>フィードバック（👍👎）を送ると、Genie の精度が向上します</li>
# MAGIC   <li>サンプル質問を追加すると、似た質問の精度が上がります</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Genie スペース作成完了</strong><br>
# MAGIC SQLを一行も書かずに、自然言語だけでデータ分析ができることを体験しました。<br>
# MAGIC テーブルコメント・PK/FK制約をしっかり設定しておくことで、Genie の精度が大きく向上します。<br><br>
# MAGIC 次は AI/BI ダッシュボード（06_Genie_ダッシュボード の後半）でビジュアル分析を体験しましょう。
# MAGIC </div>
