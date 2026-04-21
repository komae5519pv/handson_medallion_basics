# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 26px;">01 | データ準備</h1>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">サンプルCSVデータを生成し、Volumeに格納する</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 10 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC ホームセンターの商圏分析に使うサンプルデータ（CSV）を生成し、Unity Catalog Volume に格納します。<br>
# MAGIC このノートブックはデータ準備用のヘルパーです。実行するだけでOK — 分析の本番は次のノートブックからです。
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,ライブラリのインポート
import random
import string
import builtins
import pandas as pd
from datetime import datetime, timedelta, date
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, lit, concat, lpad, floor, rand, date_add, concat_ws,
    element_at, array, explode, sequence, monotonically_increasing_id,
    sum as _sum
)

_round = builtins.round

random.seed(42)

# COMMAND ----------

# DBTITLE 1,データ生成パラメータ
NUM_STORES = 30
NUM_PRODUCTS = 500
NUM_CUSTOMERS = 5000
NUM_SALES = 200000
AVG_ITEMS_PER_SALE = 3
NUM_FACILITIES = 500

SALES_START = date(2022, 1, 1)
SALES_END = date(2025, 12, 31)
SALES_DAYS = (SALES_END - SALES_START).days

print(f"店舗: {NUM_STORES}, 商品: {NUM_PRODUCTS}, 会員: {NUM_CUSTOMERS}")
print(f"売上: {NUM_SALES}, 売上明細: 約{NUM_SALES * AVG_ITEMS_PER_SALE:,}")
print(f"期間: {SALES_START} 〜 {SALES_END}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 店舗マスタ

# COMMAND ----------

# DBTITLE 1,店舗マスタ生成
store_locations = [
    ("ホームプラザ世田谷店", "東京都", "世田谷区", "世田谷区玉川3-1-1", 35.611, 139.625),
    ("ホームプラザ八王子店", "東京都", "八王子市", "八王子市高倉町49-3", 35.666, 139.344),
    ("ホームプラザ足立店", "東京都", "足立区", "足立区加平1-5-8", 35.775, 139.805),
    ("ホームプラザ横浜店", "神奈川県", "横浜市", "横浜市都筑区池辺町4035", 35.530, 139.575),
    ("ホームプラザ相模原店", "神奈川県", "相模原市", "相模原市中央区田名3000", 35.571, 139.373),
    ("ホームプラザ千葉店", "千葉県", "千葉市", "千葉市稲毛区園生町368", 35.643, 140.094),
    ("ホームプラザ柏店", "千葉県", "柏市", "柏市風早1-1-1", 35.868, 139.976),
    ("ホームプラザさいたま店", "埼玉県", "さいたま市", "さいたま市北区宮原町1-853", 35.922, 139.612),
    ("ホームプラザ川越店", "埼玉県", "川越市", "川越市小仙波町2-20", 35.925, 139.486),
    ("ホームプラザつくば店", "茨城県", "つくば市", "つくば市研究学園5-2", 36.082, 140.112),
    ("ホームプラザ宇都宮店", "栃木県", "宇都宮市", "宇都宮市インターパーク4-1-3", 36.510, 139.918),
    ("ホームプラザ前橋店", "群馬県", "前橋市", "前橋市関根町3-3-5", 36.407, 139.067),
    ("ホームプラザ堺店", "大阪府", "堺市", "堺市北区中百舌鳥町2-55", 34.547, 135.504),
    ("ホームプラザ東大阪店", "大阪府", "東大阪市", "東大阪市稲田上町1-9-16", 34.679, 135.601),
    ("ホームプラザ名古屋店", "愛知県", "名古屋市", "名古屋市北区辻町7-1", 35.212, 136.919),
    ("ホームプラザ豊田店", "愛知県", "豊田市", "豊田市秋葉町4-75", 35.083, 137.156),
    ("ホームプラザ札幌店", "北海道", "札幌市", "札幌市白石区南郷通1丁目南", 43.050, 141.385),
    ("ホームプラザ旭川店", "北海道", "旭川市", "旭川市永山2条10丁目", 43.786, 142.393),
    ("ホームプラザ仙台店", "宮城県", "仙台市", "仙台市太白区西中田2-16", 38.213, 140.883),
    ("ホームプラザ広島店", "広島県", "広島市", "広島市安佐南区大塚西6-12", 34.444, 132.415),
    ("ホームプラザ福岡店", "福岡県", "福岡市", "福岡市東区多の津1-14-1", 33.612, 130.442),
    ("ホームプラザ北九州店", "福岡県", "北九州市", "北九州市八幡西区則松東1-1", 33.860, 130.740),
    ("ホームプラザ浜松店", "静岡県", "浜松市", "浜松市東区天王町1981-3", 34.738, 137.750),
    ("ホームプラザ新潟店", "新潟県", "新潟市", "新潟市中央区姥ケ山45-1", 37.893, 139.070),
    ("ホームプラザ松本店", "長野県", "松本市", "松本市高宮中1-22", 36.218, 137.987),
    ("ホームプラザ岡山店", "岡山県", "岡山市", "岡山市北区今8-14-34", 34.677, 133.876),
    ("ホームプラザ熊本店", "熊本県", "熊本市", "熊本市東区御領6-1-60", 32.803, 130.765),
    ("ホームプラザ鹿児島店", "鹿児島県", "鹿児島市", "鹿児島市与次郎1-7-28", 31.561, 130.562),
    ("ホームプラザ金沢店", "石川県", "金沢市", "金沢市西念4-24-2", 36.578, 136.633),
    ("ホームプラザ松山店", "愛媛県", "松山市", "松山市朝生田町5-3-28", 33.826, 132.784),
]

store_types = ["大型", "標準", "コンパクト"]

stores_rows = []
for i, (name, pref, city, addr, lat, lon) in enumerate(store_locations, 1):
    stype = random.choice(store_types)
    area = {"大型": random.randint(10000, 15000), "標準": random.randint(5000, 9999), "コンパクト": random.randint(2000, 4999)}[stype]
    parking = int(area * random.uniform(0.03, 0.05))
    open_date = date(random.randint(2005, 2022), random.randint(1, 12), random.randint(1, 28))
    stores_rows.append((i, name, pref, city, addr, lat, lon, str(open_date), area, parking, stype))

stores_schema = StructType([
    StructField("store_id", IntegerType()), StructField("store_name", StringType()),
    StructField("prefecture", StringType()), StructField("city", StringType()),
    StructField("address", StringType()),
    StructField("lat", DoubleType()), StructField("lon", DoubleType()),
    StructField("opening_date", StringType()), StructField("floor_area_sqm", IntegerType()),
    StructField("parking_capacity", IntegerType()), StructField("store_type", StringType()),
])
df_stores = pd.DataFrame(stores_rows, columns=[f.name for f in stores_schema.fields])
df_stores.to_csv(f"{VOLUME_PATH}/stores.csv", index=False)
print(f"✅ stores: {len(df_stores)} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 商品マスタ

# COMMAND ----------

# DBTITLE 1,商品マスタ生成
categories = {
    100: ("園芸用品", ["培養土", "花苗", "プランター", "肥料", "園芸はさみ", "じょうろ", "支柱セット", "防虫剤", "種", "鉢底石"], (198, 2980)),
    200: ("工具", ["電動ドリル", "インパクトドライバー", "のこぎり", "ドライバーセット", "メジャー", "水平器", "クランプ", "ペンチ", "レンチ", "カンナ"], (498, 19800)),
    300: ("塗料", ["水性ペンキ", "油性ペンキ", "ニス", "刷毛", "ローラー", "マスキングテープ", "養生シート", "うすめ液", "防水塗料", "木部保護塗料"], (198, 5980)),
    400: ("木材・建材", ["2×4材", "合板", "角材", "フローリング材", "タイル", "レンガ", "モルタル", "断熱材", "石膏ボード", "化粧板"], (298, 8980)),
    500: ("金物", ["ネジセット", "ボルトナット", "蝶番", "錠前", "L字金具", "アングル", "ワイヤー", "チェーン", "フック", "キャスター"], (98, 3980)),
    600: ("電材・照明", ["LED電球", "シーリングライト", "スイッチ", "コンセント", "配線", "延長コード", "センサーライト", "懐中電灯", "テーブルランプ", "ソーラーライト"], (198, 9800)),
    700: ("水道用品", ["混合水栓", "シャワーヘッド", "ホース", "配管パーツ", "水栓パッキン", "排水口カバー", "散水ノズル", "浄水器", "トイレ部品", "蛇口"], (198, 12800)),
    800: ("収納・インテリア", ["カラーボックス", "メタルラック", "収納ケース", "壁掛けフック", "カーテンレール", "突っ張り棒", "ウォールシェルフ", "クッション", "ラグ", "時計"], (298, 9800)),
    900: ("日用品・消耗品", ["ゴミ袋", "電池", "洗剤", "軍手", "ブルーシート", "ガムテープ", "結束バンド", "掃除用品", "スポンジ", "雑巾"], (98, 1980)),
    1000: ("アウトドア・レジャー", ["テント", "BBQグリル", "クーラーボックス", "ランタン", "折りたたみチェア", "タープ", "寝袋", "アウトドアテーブル", "焚き火台", "ハンモック"], (980, 29800)),
}

sizes = ["S", "M", "L", "ミニ", "大容量"]
grades = ["スタンダード", "プロ仕様", "お買い得品", "プレミアム", "限定モデル"]
suppliers = ["メーカーA", "メーカーB", "メーカーC", "メーカーD", "メーカーE", "PB"]

products_rows = []
pid = 1
for cat_id, (cat_name, base_items, (price_min, price_max)) in categories.items():
    for base in base_items:
        for variant in grades:
            if pid > NUM_PRODUCTS:
                break
            price = random.randint(price_min // 100, price_max // 100) * 100 + random.choice([0, 80, 90])
            is_pb = 1 if random.random() < 0.15 else 0
            supplier = "PB" if is_pb else random.choice(suppliers[:5])
            products_rows.append((pid, f"{base} {variant}", cat_id, cat_name, base, price, is_pb, supplier))
            pid += 1
        if pid > NUM_PRODUCTS:
            break

products_schema = StructType([
    StructField("product_id", IntegerType()), StructField("product_name", StringType()),
    StructField("category_id", IntegerType()), StructField("category_name", StringType()),
    StructField("subcategory", StringType()), StructField("unit_price", IntegerType()),
    StructField("is_pb", IntegerType()), StructField("supplier", StringType()),
])
df_products = pd.DataFrame(products_rows, columns=[f.name for f in products_schema.fields])
df_products.to_csv(f"{VOLUME_PATH}/products.csv", index=False)
print(f"✅ products: {len(df_products)} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 会員マスタ

# COMMAND ----------

# DBTITLE 1,会員マスタ生成
last_names = ["田中", "鈴木", "佐藤", "高橋", "山田", "渡辺", "中村", "小林", "伊藤", "加藤",
              "山本", "吉田", "松本", "井上", "木村", "林", "斎藤", "清水", "山口", "阿部",
              "池田", "橋本", "森", "石川", "前田", "藤田", "岡田", "後藤", "長谷川", "村上"]
first_names = ["太郎", "花子", "一郎", "美咲", "健太", "さくら", "大輔", "陽子", "翔太", "愛",
               "拓也", "真由美", "直樹", "恵", "和也", "裕子", "浩二", "由美", "誠", "明美",
               "大地", "優子", "隆", "麻衣", "亮", "千尋", "勇気", "彩", "悠", "凛"]
genders = ["M", "F"]

# 店舗の都道府県リスト（顧客の住所に使用）
store_prefs = [row[1] for row in store_locations]

customers_rows = []
for i in range(1, NUM_CUSTOMERS + 1):
    cid = f"C{i:05d}"
    gender = random.choice(genders)
    last = random.choice(last_names)
    first = random.choice(first_names)
    name = f"{last} {first}"
    birth = date(random.randint(1955, 2005), random.randint(1, 12), random.randint(1, 28))
    pref = random.choice(store_prefs)
    city = random.choice([loc[2] for loc in store_locations if loc[1] == pref])
    postal = f"{random.randint(100, 999)}-{random.randint(1000, 9999)}"
    reg_date = date(random.randint(2018, 2025), random.randint(1, 12), random.randint(1, 28))
    home_store = random.randint(1, NUM_STORES)
    customers_rows.append((cid, name, gender, str(birth), postal, pref, city, str(reg_date), home_store))

customers_schema = StructType([
    StructField("customer_id", StringType()), StructField("name", StringType()),
    StructField("gender", StringType()), StructField("birth_date", StringType()),
    StructField("postal_code", StringType()), StructField("prefecture", StringType()),
    StructField("city", StringType()), StructField("registration_date", StringType()),
    StructField("home_store_id", IntegerType()),
])
df_customers = pd.DataFrame(customers_rows, columns=[f.name for f in customers_schema.fields])
df_customers.to_csv(f"{VOLUME_PATH}/customers.csv", index=False)
print(f"✅ customers: {len(df_customers)} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 売上・売上明細

# COMMAND ----------

# DBTITLE 1,売上・売上明細生成（PySpark）

payment_methods = ["現金", "クレジットカード", "電子マネー", "QRコード決済", "ポイント"]

# 売上テーブル（PySpark で高速生成）
df_sales_base = (
    spark.range(1, NUM_SALES + 1)
    .withColumn("sale_id", col("id"))
    .withColumn("customer_id", concat(lit("C"), lpad((floor(rand() * NUM_CUSTOMERS) + 1).cast("string"), 5, "0")))
    .withColumn("store_id", (floor(rand() * NUM_STORES) + 1).cast("int"))
    .withColumn("days_offset", (rand() * SALES_DAYS).cast("int"))
    .withColumn("sale_date", date_add(lit(SALES_START.isoformat()), col("days_offset")))
    .withColumn("sale_hour", (floor(rand() * 14) + 8).cast("int"))
    .withColumn("sale_minute", (floor(rand() * 60)).cast("int"))
    .withColumn("sale_datetime", concat_ws(" ", col("sale_date"), concat_ws(":", lpad(col("sale_hour").cast("string"), 2, "0"), lpad(col("sale_minute").cast("string"), 2, "0"), lit("00"))))
    .withColumn("payment_method", element_at(array(*[lit(p) for p in payment_methods]), (floor(rand() * len(payment_methods)) + 1).cast("int")))
    .select("sale_id", "customer_id", "store_id", "sale_datetime", "payment_method")
)

# 売上明細（1売上あたり1〜6アイテム）
df_sale_items = (
    df_sales_base
    .withColumn("num_items", (floor(rand() * 5) + 1).cast("int"))
    .withColumn("item_idx", explode(sequence(lit(1), col("num_items"))))
    .withColumn("sale_item_id", monotonically_increasing_id() + 1)
    .withColumn("product_id", (floor(rand() * NUM_PRODUCTS) + 1).cast("int"))
    .withColumn("quantity", (floor(rand() * 5) + 1).cast("int"))
    .select("sale_item_id", "sale_id", "product_id", "quantity")
)

# 商品単価を結合して小計計算
df_product_prices = spark.createDataFrame(df_products[["product_id", "unit_price"]]).selectExpr("product_id AS p_id", "unit_price")
df_sale_items_priced = (
    df_sale_items
    .join(df_product_prices, df_sale_items["product_id"] == df_product_prices["p_id"], "left")
    .withColumn("subtotal", col("quantity") * col("unit_price"))
    .select("sale_item_id", "sale_id", "product_id", "quantity", "unit_price", "subtotal")
)

# 売上テーブルに合計金額・合計数量を付与
df_sale_totals = (
    df_sale_items_priced
    .groupBy("sale_id")
    .agg(
        _sum("subtotal").alias("total_amount"),
        _sum("quantity").alias("total_quantity")
    )
)

df_sales = (
    df_sales_base
    .join(df_sale_totals, "sale_id", "left")
    .select("sale_id", "customer_id", "store_id", "sale_datetime", "total_amount", "total_quantity", "payment_method")
)

# CSV書き出し
df_sales.toPandas().to_csv(f"{VOLUME_PATH}/sales.csv", index=False)
df_sale_items_priced.toPandas().to_csv(f"{VOLUME_PATH}/sale_items.csv", index=False)

print(f"✅ sales: {df_sales.count():,} 件")
print(f"✅ sale_items: {df_sale_items_priced.count():,} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 天気データ

# COMMAND ----------

# DBTITLE 1,天気データ生成
weather_types = ["晴れ", "曇り", "雨", "雪"]
target_prefs = list(set(store_prefs))

weather_rows = []
d = SALES_START
while len(weather_rows) < 10000:
    for pref in target_prefs:
        if len(weather_rows) >= 10000:
            break
        month = d.month
        # 季節に応じた気温
        base_temp = {1: 3, 2: 4, 3: 9, 4: 15, 5: 20, 6: 23, 7: 28, 8: 30, 9: 25, 10: 18, 11: 12, 12: 6}[month]
        if pref == "北海道":
            base_temp -= 8
        elif pref in ("鹿児島県", "熊本県", "福岡県"):
            base_temp += 3
        max_temp = base_temp + random.randint(2, 8)
        min_temp = base_temp - random.randint(2, 6)
        # 季節に応じた天気
        if month in (6, 7, 9):
            weather = random.choices(weather_types, weights=[30, 30, 35, 5])[0]
        elif month in (12, 1, 2) and pref in ("北海道", "新潟県", "長野県", "石川県"):
            weather = random.choices(weather_types, weights=[20, 30, 20, 30])[0]
        else:
            weather = random.choices(weather_types, weights=[50, 30, 18, 2])[0]
        precip = _round(random.uniform(0, 30), 1) if weather in ("雨", "雪") else 0.0
        humidity = random.randint(40, 95) if weather in ("雨", "曇り") else random.randint(30, 70)
        weather_rows.append((str(d), pref, weather, max_temp, min_temp, precip, humidity))
    d += timedelta(days=1)

weather_schema = StructType([
    StructField("date", StringType()), StructField("prefecture", StringType()),
    StructField("weather", StringType()), StructField("max_temp", IntegerType()),
    StructField("min_temp", IntegerType()), StructField("precipitation", DoubleType()),
    StructField("humidity", IntegerType()),
])
df_weather = pd.DataFrame(weather_rows[:10000], columns=[f.name for f in weather_schema.fields])
df_weather.to_csv(f"{VOLUME_PATH}/weather.csv", index=False)
print(f"✅ weather: {len(df_weather):,} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 周辺施設データ

# COMMAND ----------

# DBTITLE 1,周辺施設データ生成
facility_types = ["競合店", "駅", "学校", "病院", "公園", "ショッピングモール", "コンビニ", "ガソリンスタンド", "飲食店", "スーパー"]
facility_names = {
    "競合店": ["ホームセンターX", "DIYストアY", "建材センターZ"],
    "駅": ["中央駅", "北口駅", "南口駅", "東口駅"],
    "学校": ["第一小学校", "中央中学校", "県立高校"],
    "病院": ["総合病院", "クリニック", "医療センター"],
    "公園": ["中央公園", "市民公園", "運動公園"],
    "ショッピングモール": ["モールA", "プラザB", "タウンC"],
    "コンビニ": ["コンビニA", "コンビニB", "コンビニC"],
    "ガソリンスタンド": ["GSアルファ", "GSベータ", "GSガンマ"],
    "飲食店": ["レストランX", "ファミレスY", "カフェZ"],
    "スーパー": ["スーパーA", "フードマートB", "生鮮市場C"],
}

facilities_rows = []
for i in range(1, NUM_FACILITIES + 1):
    ftype = random.choice(facility_types)
    fname = random.choice(facility_names[ftype])
    store_idx = random.randint(0, NUM_STORES - 1)
    base_lat = store_locations[store_idx][4]
    base_lon = store_locations[store_idx][5]
    # 店舗から半径5km以内にランダム配置
    f_lat = base_lat + random.uniform(-0.045, 0.045)
    f_lon = base_lon + random.uniform(-0.045, 0.045)
    distance_km = _round(random.uniform(0.1, 5.0), 2)
    pref = store_locations[store_idx][1]
    city = store_locations[store_idx][2]
    facilities_rows.append((i, ftype, fname, pref, city, _round(f_lat, 6), _round(f_lon, 6), store_idx + 1, distance_km))

facilities_schema = StructType([
    StructField("facility_id", IntegerType()), StructField("facility_type", StringType()),
    StructField("facility_name", StringType()), StructField("prefecture", StringType()),
    StructField("city", StringType()), StructField("lat", DoubleType()),
    StructField("lon", DoubleType()), StructField("nearest_store_id", IntegerType()),
    StructField("distance_km", DoubleType()),
])
df_facilities = pd.DataFrame(facilities_rows, columns=[f.name for f in facilities_schema.fields])
df_facilities.to_csv(f"{VOLUME_PATH}/facilities.csv", index=False)
print(f"✅ facilities: {len(df_facilities)} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume内ファイル確認

# COMMAND ----------

# DBTITLE 1,Volume内のファイル一覧
import os
for f in sorted(os.listdir(VOLUME_PATH)):
    size = os.path.getsize(f"{VOLUME_PATH}/{f}")
    print(f"📁 {f}  ({size:,} bytes)")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ データ準備完了</strong><br>
# MAGIC 以下のCSVデータがVolumeに格納されました:
# MAGIC <ul>
# MAGIC   <li><strong>stores</strong> — 店舗マスタ（30件）</li>
# MAGIC   <li><strong>products</strong> — 商品マスタ（500件）</li>
# MAGIC   <li><strong>customers</strong> — 会員マスタ（5,000件）</li>
# MAGIC   <li><strong>sales</strong> — 売上トランザクション（200,000件）</li>
# MAGIC   <li><strong>sale_items</strong> — 売上明細（約600,000件）</li>
# MAGIC   <li><strong>weather</strong> — 天気データ（10,000件）</li>
# MAGIC   <li><strong>facilities</strong> — 周辺施設データ（500件）</li>
# MAGIC </ul>
# MAGIC 次のノートブック（02_データ取り込み）で、これらをDeltaテーブルとして取り込みます。
# MAGIC </div>
