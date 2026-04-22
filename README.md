# ホームセンター商圏分析 ハンズオン

## 概要

ホームセンターの商圏分析をテーマに、Databricksでのデータ分析基盤構築をハンズオン形式で体験する。  
データ取り込みからメダリオンアーキテクチャ（Bronze/Silver/Gold）によるETL、AI Functionsを活用した分析、Genieによる自然言語クエリまでを一気通貫で実施する。

## 技術スタック

| カテゴリ | 技術 |
|---|---|
| コンピュート | Serverless |
| 言語 | SQL, Python (PySpark) |
| テーブルフォーマット | Delta Lake |
| データガバナンス | Unity Catalog |
| データストレージ | Unity Catalog Volume |
| データ取り込み | `read_files()`, `COPY INTO`, PySpark DataFrame API |
| AI | AI Functions (`ai_query`, `ai_classify`) |
| 可視化 | Genie Space, AI/BI Dashboard |
| ワークフロー | Jobs |

## 対象

- データ分析チーム（SQL経験あり・Python初心者〜初級）
- 約2時間

## ディレクトリ構成

同じETL処理を **Pythonトラック** と **SQL+Pythonトラック** の2通りで用意しています。  
対象者のスキルに応じて選択、または両方を比較しながら進められます。  
各トラックは **自己完結** しており、選んだフォルダの中だけで 01〜08 を順に実行すれば完了します。

```
handson_hc_trade_area/
├── 00_config.py                         ← 共通: 変数定義・スキーマ・Volume作成
│
├── 01_Python/                           ← Python トラック（PySpark DataFrame API 中心）
│   ├── 01_データ準備.py                   サンプルCSV生成 → Volume格納
│   ├── 02_データ取り込み_Bronze.py         spark.read.csv() / saveAsTable()
│   ├── 03_データ加工_Silver.py            DataFrame cast/join/window
│   ├── 04_データ加工_Gold.py              groupBy/agg + spark.sql() for AI関数
│   ├── 05_テーブル設定.py                 spark.sql() でDDL実行
│   ├── 06_Genie作成手順.py               Genie Space セットアップ
│   ├── 07_ダッシュボード作成手順.py         AI/BI ダッシュボード作成
│   ├── 08_Jobsワークフロー作成手順.py      Jobs DAGワークフロー
│   └── 09_Genieインタラクティブ分析.py     Genie Code エージェントモードで対話的分析
│
├── 02_SQL+Python/                       ← SQL+Python トラック（spark.sql() + Pythonループ）
│   ├── 01_データ準備.py                   サンプルCSV生成 → Volume格納
│   ├── 02_データ取り込み_Bronze.py         spark.sql() + Pythonループで一括取り込み
│   ├── 03_データ加工_Silver.py            spark.sql() → .write.saveAsTable()
│   ├── 04_データ加工_Gold.py              spark.sql() でCTE/Window/AI関数
│   ├── 05_テーブル設定.py                 Python辞書＋ループでDDL一括実行
│   ├── 06_Genie作成手順.py               Genie Space セットアップ
│   ├── 07_ダッシュボード作成手順.py         AI/BI ダッシュボード作成
│   ├── 08_Jobsワークフロー作成手順.py      Jobs DAGワークフロー
│   └── 09_Genieインタラクティブ分析.py     Genie Code エージェントモードで対話的分析
│
└── _images/                             ← ダッシュボード手順用スクリーンショット
```

### トラックの使い分け

| トラック | 対象者 | 特徴 |
|---|---|---|
| **01_Python/** | Python学習中・DataFrame APIを体験したい方 | PySpark DataFrame API 中心。AI関数・DDLは `spark.sql()` 経由 |
| **02_SQL+Python/** | 実務で使いたい方（推奨） | SQLは `spark.sql()` で実行、繰り返し処理はPythonループ。本番ETLに近いスタイル |

## サンプルデータ

ホームセンター業態のサンプルデータを使用。

| データ | 件数 | 概要 |
|---|---|---|
| 店舗マスタ | 30 | 店名, 住所, 緯度経度, 売場面積, 駐車場台数 |
| 商品マスタ | 500 | 商品名, カテゴリ(園芸/工具/塗料/木材/金物/電材/水道/収納/日用品/アウトドア), 単価 |
| 会員マスタ | 5,000 | 氏名, 住所, 登録店舗 |
| 売上 | 200,000 | 日時, 店舗, 顧客, 金額 |
| 売上明細 | 600,000 | 商品, 数量, 小計 |
| 天気 | 10,000 | 日付, 都道府県, 天気, 気温, 降水量 |
| 周辺施設 | 500 | 施設種別, 位置, 最寄り店舗 |

---

## ノートブック構成

| # | ノートブック | 時間 | 内容 |
|---|---|---|---|
| 00 | config | 5分 | 変数定義, `dbutils.widgets`, スキーマ・Volume作成 |
| 01 | データ準備 | 10分 | PythonでサンプルCSV生成, Volume格納 |
| 02 | データ取り込み_Bronze | 20分 | CSV → Bronze Delta テーブル |
| 03 | データ加工_Silver | 25分 | 型変換・NULL処理・重複排除・JOIN, Delta Time Travel |
| 04 | データ加工_Gold | 25分 | 商圏分析テーブル構築, RFM分析, `ai_classify()`, `ai_query()` |
| 05 | テーブル設定 | 10分 | PK/FK制約, カラム/テーブルコメント, カラムマスキング |
| 06 | Genie作成手順 | 20分 | Genie Space 作成・General Instructions・サンプル質問 |
| 07 | ダッシュボード作成手順 | 15分 | AI/BI ダッシュボード作成（Genie Code）・Genie連携 |
| 08 | Jobsワークフロー作成手順 | 15分 | ノートブックをJobsでDAG化・実行体験 |
| 09 | Genie Codeインタラクティブ分析 | 15分 | Genie Code エージェントモードで自然言語→コード生成→分析洞察 |
