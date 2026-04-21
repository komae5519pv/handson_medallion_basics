# ホームセンター商圏分析 ハンズオン

## 概要

ホームセンターの商圏分析をテーマに、Databricksでのデータ分析基盤構築をハンズオン形式で体験する。  
データ取り込みからメダリオンアーキテクチャ（Bronze/Silver/Gold）によるETL、AI Functionsを活用した分析、Genieによる自然言語クエリまでを一気通貫で実施する。

## 技術スタック

| カテゴリ | 技術 |
|---|---|
| Runtime | Databricks Runtime 16.x LTS |
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

同じETL処理を **SQLトラック** と **Pythonトラック** の2通りで用意しています。  
対象者のスキルに応じて選択、または両方を比較しながら進められます。

```
handson_hc_trade_area/
├── 00_config.py                     ← 共通: 変数定義・スキーマ・Volume作成
├── 01_データ準備.py                   ← 共通: サンプルCSV生成
│
├── 01_SQL/                          ← SQL トラック（%sql セル中心）
│   ├── 02_データ取り込み.py           read_files() / COPY INTO
│   ├── 03_データ加工_Silver.py        型変換・JOIN・Time Travel
│   ├── 04_データ加工_Gold.py          RFM分析・AI関数・集計
│   └── 05_テーブル設定.py             PK/FK・コメント・マスキング
│
├── 02_Python/                       ← Python トラック（PySpark DataFrame API 中心）
│   ├── 02_データ取り込み.py           spark.read.csv() / saveAsTable()
│   ├── 03_データ加工_Silver.py        DataFrame cast/join/window
│   ├── 04_データ加工_Gold.py          groupBy/agg + spark.sql() for AI関数
│   └── 05_テーブル設定.py             spark.sql() でDDL実行
│
├── 03_SQL+Python/                   ← ハイブリッド トラック（spark.sql() + Pythonループ）
│   ├── 02_データ取り込み.py           spark.sql() + Pythonループで一括取り込み
│   ├── 03_データ加工_Silver.py        spark.sql() → .write.saveAsTable()
│   ├── 04_データ加工_Gold.py          spark.sql() でCTE/Window/AI関数
│   └── 05_テーブル設定.py             Python辞書＋ループでDDL一括実行
│
├── 06_Genie_ダッシュボード.py         ← 共通: Genie Space・AI/BIダッシュボード
└── 07_Jobsワークフロー.py            ← 共通: Jobs DAGワークフロー
```

### トラックの使い分け

| トラック | 対象者 | 特徴 |
|---|---|---|
| **01_SQL/** | SQLに慣れている分析者 | 全て `%sql` セルで完結。Pythonコードなし |
| **02_Python/** | Python学習中・両方比較したい方 | PySpark DataFrame API 中心。AI関数・DDLは `spark.sql()` 経由 |
| **03_SQL+Python/** | 実務で使いたい方（推奨） | SQLは `spark.sql()` で実行、繰り返し処理はPythonループ。本番ETLに近いスタイル |

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
| 02 | データ取り込み | 20分 | CSV → Bronze Delta テーブル（SQLトラック: `read_files()`/`COPY INTO`、Pythonトラック: `spark.read.csv()`） |
| 03 | データ加工_Silver | 25分 | 型変換・NULL処理・重複排除・JOIN, Delta Time Travel |
| 04 | データ加工_Gold | 25分 | 商圏分析テーブル構築, RFM分析, `ai_classify()`, `ai_query()` |
| 05 | テーブル設定 | 10分 | PK/FK制約, カラム/テーブルコメント, カラムマスキング |
| 06 | Genie_ダッシュボード | 15分 | Genie Space セットアップ・デモ, AI/BI ダッシュボードデモ |
| 07 | Jobs ワークフロー | 10分 | ノートブックをJobsでDAG化・実行体験 |
