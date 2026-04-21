# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">08 | Jobs ワークフロー作成手順</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">ETLパイプラインをDAGで自動化する</p>
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
# MAGIC これまで手動で実行してきたノートブック（02→03→04→05）を、<br>
# MAGIC Databricks Jobs を使って<strong>ワークフロー化（DAG）</strong>し、自動実行できるようにします。<br>
# MAGIC ここではUI操作でジョブを作成する手順を説明します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Databricks Jobs とは

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Databricks Jobs とは</strong><br>
# MAGIC ノートブックやスクリプトを<strong>スケジュール実行</strong>・<strong>依存関係付きDAG実行</strong>できる機能です。<br>
# MAGIC <ul>
# MAGIC   <li><strong>タスクDAG</strong>: タスク間の依存関係を定義し、並列/直列に実行</li>
# MAGIC   <li><strong>スケジュール</strong>: cron形式で定期実行（毎朝6時、毎時間、etc.）</li>
# MAGIC   <li><strong>リトライ/アラート</strong>: 失敗時の自動リトライ、メール/Slack通知</li>
# MAGIC   <li><strong>Serverless</strong>: コンピュートの起動待ち時間ゼロで実行</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. 作成するDAG構成

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 パイプラインの全体像</strong><br>
# MAGIC 4つのノートブックを順番に実行する直列DAGを作成します。
# MAGIC </div>
# MAGIC <div style="display: flex; justify-content: center; margin: 16px 0;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 880 120" width="820">
# MAGIC   <defs>
# MAGIC     <marker id="arr" viewBox="0 0 10 7" refX="10" refY="3.5" markerWidth="10" markerHeight="7" orient="auto">
# MAGIC       <polygon points="0 0, 10 3.5, 0 7" fill="#94a3b8"/>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC   <style>text{font-family:"Helvetica Neue",Helvetica,Arial,"Hiragino Kaku Gothic ProN","Hiragino Sans",Meiryo,sans-serif}</style>
# MAGIC   <rect x="0" y="10" width="185" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="0" y="10" width="4" height="56" rx="2" fill="#3b82f6"/>
# MAGIC   <text x="16" y="33" font-size="12" font-weight="600" fill="#1e293b">02_データ取り込み</text>
# MAGIC   <text x="16" y="52" font-size="10" fill="#94a3b8">Bronze — CSV → Delta</text>
# MAGIC   <line x1="185" y1="38" x2="220" y2="38" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
# MAGIC   <rect x="225" y="10" width="185" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="225" y="10" width="4" height="56" rx="2" fill="#64748b"/>
# MAGIC   <text x="241" y="33" font-size="12" font-weight="600" fill="#1e293b">03_データ加工 Silver</text>
# MAGIC   <text x="241" y="52" font-size="10" fill="#94a3b8">型変換・JOIN・クレンジング</text>
# MAGIC   <line x1="410" y1="38" x2="445" y2="38" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
# MAGIC   <rect x="450" y="10" width="185" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="450" y="10" width="4" height="56" rx="2" fill="#f59e0b"/>
# MAGIC   <text x="466" y="33" font-size="12" font-weight="600" fill="#1e293b">04_データ加工 Gold</text>
# MAGIC   <text x="466" y="52" font-size="10" fill="#94a3b8">RFM分析・AI関数・集計</text>
# MAGIC   <line x1="635" y1="38" x2="670" y2="38" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
# MAGIC   <rect x="675" y="10" width="185" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="675" y="10" width="4" height="56" rx="2" fill="#22c55e"/>
# MAGIC   <text x="691" y="33" font-size="12" font-weight="600" fill="#1e293b">05_テーブル設定</text>
# MAGIC   <text x="691" y="52" font-size="10" fill="#94a3b8">PK/FK・コメント・マスキング</text>
# MAGIC   <rect x="0" y="84" width="7" height="7" rx="2" fill="#3b82f6"/>
# MAGIC   <text x="11" y="91" font-size="9" fill="#94a3b8">Bronze</text>
# MAGIC   <rect x="58" y="84" width="7" height="7" rx="2" fill="#64748b"/>
# MAGIC   <text x="69" y="91" font-size="9" fill="#94a3b8">Silver</text>
# MAGIC   <rect x="110" y="84" width="7" height="7" rx="2" fill="#f59e0b"/>
# MAGIC   <text x="121" y="91" font-size="9" fill="#94a3b8">Gold</text>
# MAGIC   <rect x="155" y="84" width="7" height="7" rx="2" fill="#22c55e"/>
# MAGIC   <text x="166" y="91" font-size="9" fill="#94a3b8">Governance</text>
# MAGIC </svg>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. ジョブの作成手順

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 1 — ジョブを新規作成</strong><br>
# MAGIC <ol>
# MAGIC   <li>左メニューの「<strong>Workflows</strong>」をクリック</li>
# MAGIC   <li>「<strong>Create job</strong>」をクリック</li>
# MAGIC   <li>ジョブ名を入力: <strong>HC商圏分析_ETLパイプライン</strong></li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 2 — タスクを追加</strong><br><br>
# MAGIC 以下の4タスクを順番に追加してください。<br>
# MAGIC ノートブックのパスは、ご自身がハンズオンで使用したトラック（SQL / Python）に合わせてください。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #7B1FA2; background: #F3E5F5; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔧 タスク構成</strong>
# MAGIC <table style="width: 100%; border-collapse: collapse; border-radius: 8px; overflow: hidden; margin-top: 8px;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #7B1FA2; color: white;">
# MAGIC       <th style="padding: 10px 15px; text-align: left;">タスク名</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">ノートブック</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">依存先</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>ingest</b></td>
# MAGIC       <td style="padding: 8px 15px;"><code>01_SQL/02_データ取り込み</code><br/>または <code>02_Python/02_データ取り込み</code></td>
# MAGIC       <td style="padding: 8px 15px;">（なし — 最初に実行）</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;"><b>silver</b></td>
# MAGIC       <td style="padding: 8px 15px;"><code>01_SQL/03_データ加工_Silver</code><br/>または <code>02_Python/03_データ加工_Silver</code></td>
# MAGIC       <td style="padding: 8px 15px;">ingest</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>gold</b></td>
# MAGIC       <td style="padding: 8px 15px;"><code>01_SQL/04_データ加工_Gold</code><br/>または <code>02_Python/04_データ加工_Gold</code></td>
# MAGIC       <td style="padding: 8px 15px;">silver</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;"><b>governance</b></td>
# MAGIC       <td style="padding: 8px 15px;"><code>01_SQL/05_テーブル設定</code><br/>または <code>02_Python/05_テーブル設定</code></td>
# MAGIC       <td style="padding: 8px 15px;">gold</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 3 — 各タスクの設定</strong><br><br>
# MAGIC 各タスクで以下を設定してください:
# MAGIC <ol>
# MAGIC   <li><strong>Type</strong>: <code>Notebook</code> を選択</li>
# MAGIC   <li><strong>Source</strong>: <code>Workspace</code> を選択</li>
# MAGIC   <li><strong>Path</strong>: 上の表のノートブックパスを指定</li>
# MAGIC   <li><strong>Compute</strong>: <code>Serverless</code> を選択（起動待ち時間ゼロ）</li>
# MAGIC   <li><strong>Depends on</strong>: 上の表の依存先タスクを選択</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 ノートブックのパスについて</strong><br>
# MAGIC <ul>
# MAGIC   <li>「Path」欄をクリックするとワークスペースのファイルブラウザが開きます</li>
# MAGIC   <li>ハンズオンのフォルダまでナビゲートし、対象ノートブックを選択してください</li>
# MAGIC   <li>SQL トラック / Python トラックどちらを選んでも結果は同じです</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. ジョブの実行

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 4 — ジョブを実行する</strong><br>
# MAGIC <ol>
# MAGIC   <li>右上の「<strong>Run now</strong>」をクリック</li>
# MAGIC   <li>DAGビューで各タスクの実行状況を確認</li>
# MAGIC   <li>全タスクが緑色（成功）になれば完了</li>
# MAGIC </ol>
# MAGIC <br>
# MAGIC 💡 タスクをクリックすると、そのノートブックの実行結果を確認できます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ 失敗した場合</strong><br>
# MAGIC <ul>
# MAGIC   <li>失敗したタスク（赤色）をクリックしてエラーログを確認してください</li>
# MAGIC   <li>ノートブックのパスやコンピュートの設定が正しいか確認してください</li>
# MAGIC   <li>修正後、失敗したタスクだけを「<strong>Repair run</strong>」で再実行できます（最初からやり直す必要なし）</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. スケジュール設定（オプション）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: スケジュールを設定する</strong><br><br>
# MAGIC 本番運用では、このジョブを定期実行するようスケジュール設定します。
# MAGIC <ol>
# MAGIC   <li>ジョブ画面の右上「<strong>Add trigger</strong>」→「<strong>Scheduled</strong>」をクリック</li>
# MAGIC   <li>スケジュールを設定（例: 毎日 6:00 AM JST）</li>
# MAGIC   <li>タイムゾーンが <strong>Asia/Tokyo</strong> になっていることを確認</li>
# MAGIC   <li>「<strong>Save</strong>」をクリック</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 本番運用で追加したい設定</strong><br>
# MAGIC <table style="width: 100%; border-collapse: collapse; border-radius: 8px; overflow: hidden; margin-top: 8px;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1976D2; color: white;">
# MAGIC       <th style="padding: 10px 15px; text-align: left;">設定項目</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">推奨値</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">設定場所</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;">リトライ回数</td>
# MAGIC       <td style="padding: 8px 15px;">2回</td>
# MAGIC       <td style="padding: 8px 15px;">各タスク → Advanced → Retries</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;">タイムアウト</td>
# MAGIC       <td style="padding: 8px 15px;">30分</td>
# MAGIC       <td style="padding: 8px 15px;">各タスク → Advanced → Timeout</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;">メール通知</td>
# MAGIC       <td style="padding: 8px 15px;">失敗時に管理者へ通知</td>
# MAGIC       <td style="padding: 8px 15px;">ジョブ設定 → Notifications</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. クリーンアップ（オプション）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #D32F2F; background: #FFEBEE; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🗑️ ハンズオン環境のクリーンアップ</strong><br>
# MAGIC ハンズオン終了後、作成したリソースを削除する場合:
# MAGIC <ul>
# MAGIC   <li><strong>ジョブの削除</strong>: Workflows → 該当ジョブ → 右上「⋮」→ Delete</li>
# MAGIC   <li><strong>スキーマの削除</strong>: 以下のSQLを実行（データも全て削除されます）</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,クリーンアップ（必要な場合のみコメント解除して実行）
# 以下のコメントを外して実行するとスキーマごと削除されます
# spark.sql(f"DROP SCHEMA IF EXISTS {MY_CATALOG}.{MY_SCHEMA} CASCADE")
# print(f"🗑️ スキーマ '{MY_CATALOG}.{MY_SCHEMA}' を削除しました")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ ハンズオン完了!</strong><br>
# MAGIC おつかれさまでした！以下を体験しました:
# MAGIC <ul>
# MAGIC   <li><strong>00</strong>: Unity Catalog 環境設定（カタログ > スキーマ > Volume）</li>
# MAGIC   <li><strong>01</strong>: サンプルデータ生成</li>
# MAGIC   <li><strong>02</strong>: データ取り込み（Bronze — CSV → Delta）</li>
# MAGIC   <li><strong>03</strong>: データ加工 Silver（型変換・JOIN・Time Travel）</li>
# MAGIC   <li><strong>04</strong>: データ加工 Gold（RFM分析・AI関数・商圏分析）</li>
# MAGIC   <li><strong>05</strong>: テーブル設定（PK/FK・コメント・カラムマスキング）</li>
# MAGIC   <li><strong>06</strong>: Genie スペース（自然言語BI）</li>
# MAGIC   <li><strong>07</strong>: AI/BI ダッシュボード（AIでノーコード作成）</li>
# MAGIC   <li><strong>08</strong>: Jobs ワークフロー（ETLパイプライン自動化）</li>
# MAGIC </ul>
# MAGIC </div>
