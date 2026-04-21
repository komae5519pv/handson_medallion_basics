# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">07 | Jobs ワークフロー</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">ETLパイプラインの自動化</p>
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
# MAGIC これまで手動で実行してきたノートブック（02→03→04→05）を、<br>
# MAGIC Databricks Jobsを使って<strong>ワークフロー化（DAG）</strong>し、自動実行できるようにします。
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Databricks Jobs ワークフロー

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Databricks Jobs とは</strong><br>
# MAGIC ノートブックやスクリプトを<strong>スケジュール実行</strong>・<strong>依存関係付きDAG実行</strong>できる機能です。<br>
# MAGIC <ul>
# MAGIC   <li><strong>タスクDAG</strong>: タスク間の依存関係を定義し、並列/直列に実行</li>
# MAGIC   <li><strong>スケジュール</strong>: cron形式で定期実行（毎朝6時、毎時間、etc.）</li>
# MAGIC   <li><strong>リトライ/アラート</strong>: 失敗時の自動リトライ、メール/Slack通知</li>
# MAGIC   <li><strong>パラメータ</strong>: 実行時にパラメータを渡せる（環境切り替え等）</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: ワークフローを作成する</strong><br><br>
# MAGIC <strong>作成するDAG構成:</strong>
# MAGIC <div style="display: flex; justify-content: center; margin: 16px 0;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 880 120" width="820">
# MAGIC   <defs>
# MAGIC     <marker id="arr" viewBox="0 0 10 7" refX="10" refY="3.5" markerWidth="10" markerHeight="7" orient="auto">
# MAGIC       <polygon points="0 0, 10 3.5, 0 7" fill="#94a3b8"/>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC   <style>text{font-family:"Helvetica Neue",Helvetica,Arial,"Hiragino Kaku Gothic ProN","Hiragino Sans",Meiryo,sans-serif}</style>
# MAGIC   <!-- Node 1: Bronze -->
# MAGIC   <rect x="0" y="10" width="185" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="0" y="10" width="4" height="56" rx="2" fill="#3b82f6"/>
# MAGIC   <text x="16" y="33" font-size="12" font-weight="600" fill="#1e293b">02_データ取り込み</text>
# MAGIC   <text x="16" y="52" font-size="10" fill="#94a3b8">Bronze — CSV → Delta</text>
# MAGIC   <!-- Arrow 1→2 -->
# MAGIC   <line x1="185" y1="38" x2="220" y2="38" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
# MAGIC   <!-- Node 2: Silver -->
# MAGIC   <rect x="225" y="10" width="185" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="225" y="10" width="4" height="56" rx="2" fill="#64748b"/>
# MAGIC   <text x="241" y="33" font-size="12" font-weight="600" fill="#1e293b">03_データ加工 Silver</text>
# MAGIC   <text x="241" y="52" font-size="10" fill="#94a3b8">型変換・JOIN・クレンジング</text>
# MAGIC   <!-- Arrow 2→3 -->
# MAGIC   <line x1="410" y1="38" x2="445" y2="38" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
# MAGIC   <!-- Node 3: Gold -->
# MAGIC   <rect x="450" y="10" width="185" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="450" y="10" width="4" height="56" rx="2" fill="#f59e0b"/>
# MAGIC   <text x="466" y="33" font-size="12" font-weight="600" fill="#1e293b">04_データ加工 Gold</text>
# MAGIC   <text x="466" y="52" font-size="10" fill="#94a3b8">RFM分析・AI関数・集計</text>
# MAGIC   <!-- Arrow 3→4 -->
# MAGIC   <line x1="635" y1="38" x2="670" y2="38" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
# MAGIC   <!-- Node 4: Governance -->
# MAGIC   <rect x="675" y="10" width="185" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="675" y="10" width="4" height="56" rx="2" fill="#22c55e"/>
# MAGIC   <text x="691" y="33" font-size="12" font-weight="600" fill="#1e293b">05_テーブル設定</text>
# MAGIC   <text x="691" y="52" font-size="10" fill="#94a3b8">PK/FK・コメント・マスキング</text>
# MAGIC   <!-- Legend -->
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
# MAGIC <br>
# MAGIC <strong>手順:</strong>
# MAGIC <ol>
# MAGIC   <li>左メニューの「<strong>Workflows</strong>」をクリック</li>
# MAGIC   <li>「<strong>Create job</strong>」をクリック</li>
# MAGIC   <li>ジョブ名を入力（例: 「HC商圏分析_ETLパイプライン」）</li>
# MAGIC   <li>以下の4タスクを追加（依存関係を設定）:</li>
# MAGIC </ol>
# MAGIC
# MAGIC <table style="border-collapse: collapse; width: 100%; margin-top: 8px;">
# MAGIC   <tr style="background: #1B3139; color: white;">
# MAGIC     <th style="padding: 8px;">タスク名</th>
# MAGIC     <th style="padding: 8px;">ノートブック</th>
# MAGIC     <th style="padding: 8px;">依存先</th>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 8px;">ingest</td>
# MAGIC     <td style="padding: 8px;">02_データ取り込み</td>
# MAGIC     <td style="padding: 8px;">（なし = 最初に実行）</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 8px;">silver</td>
# MAGIC     <td style="padding: 8px;">03_データ加工_Silver</td>
# MAGIC     <td style="padding: 8px;">ingest</td>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 8px;">gold</td>
# MAGIC     <td style="padding: 8px;">04_データ加工_Gold</td>
# MAGIC     <td style="padding: 8px;">silver</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 8px;">governance</td>
# MAGIC     <td style="padding: 8px;">05_テーブル設定</td>
# MAGIC     <td style="padding: 8px;">gold</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC <br>
# MAGIC <strong>設定ポイント:</strong>
# MAGIC <ul>
# MAGIC   <li>コンピュート: 「<strong>Serverless</strong>」を選択（起動待ち時間ゼロ）</li>
# MAGIC   <li>パラメータ: <code>catalog</code> = <code>${catalog}</code>, <code>schema</code> = <code>${schema}</code>, <code>volume</code> = <code>${volume}</code></li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 ジョブ作成をPythonで自動化する（参考）</strong><br>
# MAGIC UI操作の代わりに、Databricks SDK / REST APIでもジョブを作成できます。<br>
# MAGIC 以下はPythonでの作成例です。実行するとジョブが作成されます。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Python SDK でジョブ作成（参考）
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, TaskDependency, JobCluster
)

w = WorkspaceClient()

# 現在のノートブックのパスからベースパスを取得
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
base_path = "/".join(notebook_path.split("/")[:-1])

job = w.jobs.create(
    name="HC商圏分析_ETLパイプライン",
    tasks=[
        Task(
            task_key="ingest",
            notebook_task=NotebookTask(
                notebook_path=f"{base_path}/02_データ取り込み",
                base_parameters={"catalog": MY_CATALOG, "schema": MY_SCHEMA, "volume": MY_VOLUME}
            ),
        ),
        Task(
            task_key="silver",
            depends_on=[TaskDependency(task_key="ingest")],
            notebook_task=NotebookTask(
                notebook_path=f"{base_path}/03_データ加工_Silver",
                base_parameters={"catalog": MY_CATALOG, "schema": MY_SCHEMA, "volume": MY_VOLUME}
            ),
        ),
        Task(
            task_key="gold",
            depends_on=[TaskDependency(task_key="silver")],
            notebook_task=NotebookTask(
                notebook_path=f"{base_path}/04_データ加工_Gold",
                base_parameters={"catalog": MY_CATALOG, "schema": MY_SCHEMA, "volume": MY_VOLUME}
            ),
        ),
        Task(
            task_key="governance",
            depends_on=[TaskDependency(task_key="gold")],
            notebook_task=NotebookTask(
                notebook_path=f"{base_path}/05_テーブル設定",
                base_parameters={"catalog": MY_CATALOG, "schema": MY_SCHEMA, "volume": MY_VOLUME}
            ),
        ),
    ],
)

print(f"✅ ジョブ作成完了: {job.job_id}")
print(f"   ジョブ名: HC商圏分析_ETLパイプライン")
print(f"   タスク数: 4")

# COMMAND ----------

# DBTITLE 1,ジョブを実行
run = w.jobs.run_now(job_id=job.job_id)
print(f"🚀 ジョブ実行開始: Run ID = {run.run_id}")
print(f"   Workflows UI で進捗を確認してください")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ 実行時の注意</strong><br>
# MAGIC <ul>
# MAGIC   <li>ジョブ実行にはコンピュートリソースが必要です。Serverlessを選択すると起動待ちなしで実行できます</li>
# MAGIC   <li>ジョブの実行状況は左メニュー「Workflows」→ 該当ジョブで確認できます</li>
# MAGIC   <li>失敗した場合はタスクごとにログを確認し、個別に再実行できます</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. スケジュール設定

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: スケジュールを設定する</strong><br><br>
# MAGIC <strong>手順:</strong>
# MAGIC <ol>
# MAGIC   <li>Workflows画面で作成したジョブを開く</li>
# MAGIC   <li>右上の「<strong>Add trigger</strong>」→「<strong>Scheduled</strong>」をクリック</li>
# MAGIC   <li>スケジュールを設定:
# MAGIC     <ul>
# MAGIC       <li>例: 毎日 6:00 AM（JST）</li>
# MAGIC       <li>cron式: <code>0 0 6 * * ?</code></li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC   <li>「<strong>Save</strong>」をクリック</li>
# MAGIC </ol>
# MAGIC <br>
# MAGIC 💡 <strong>本番運用では:</strong> リトライ回数（例: 2回）、タイムアウト、失敗時のメール通知も設定しましょう。
# MAGIC </div>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. クリーンアップ（オプション）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #D32F2F; background: #FFEBEE; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🗑️ ハンズオン環境のクリーンアップ</strong><br>
# MAGIC ハンズオン終了後、リソースを削除する場合は以下を実行してください。<br>
# MAGIC <strong>※ この操作は取り消せません。必要なデータがないことを確認してから実行してください。</strong>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,クリーンアップ（必要な場合のみ実行）
# 以下のコメントを外して実行するとスキーマごと削除されます
# spark.sql(f"DROP SCHEMA IF EXISTS {MY_CATALOG}.{MY_SCHEMA} CASCADE")
# print(f"🗑️ スキーマ '{MY_CATALOG}.{MY_SCHEMA}' を削除しました")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ ハンズオン完了!</strong><br>
# MAGIC おつかれさまでした！以下を体験しました:
# MAGIC <ul>
# MAGIC   <li><strong>00</strong>: Unity Catalog環境設定（カタログ > スキーマ > Volume）</li>
# MAGIC   <li><strong>01</strong>: サンプルデータ生成</li>
# MAGIC   <li><strong>02</strong>: データ取り込み（read_files / COPY INTO / PySpark）</li>
# MAGIC   <li><strong>03</strong>: Silver加工（型変換・JOIN・SQL↔Python連携・Time Travel）</li>
# MAGIC   <li><strong>04</strong>: Gold加工（RFM分析・AI関数）</li>
# MAGIC   <li><strong>05</strong>: テーブル設定（PK/FK・コメント・カラムマスキング）</li>
# MAGIC   <li><strong>06</strong>: Genie & AI/BIダッシュボード</li>
# MAGIC   <li><strong>07</strong>: Jobsワークフロー</li>
# MAGIC </ul>
# MAGIC </div>
