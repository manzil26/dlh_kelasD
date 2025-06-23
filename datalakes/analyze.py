import os
from datetime import datetime
from ingest import move_files_by_type
import matplotlib.pyplot as plt
import psycopg2
from psycopg2.extras import execute_values
from wordcloud import WordCloud  
import pdfplumber 

# aktifkan jika ingin pakai WordCloud
# import pdfplumber  # aktifkan jika ingin pakai extract_pdf_data

# # =====================================================
# # DATABASE CONNECTION
# # =====================================================
def get_staging_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="staggingDB",
        user="postgres",
        password="dataEngginer"
    )

# =====================================================
# TEXT ANALYSIS SECTION
# =====================================================
def extract_sentiment(message):
    text = message.lower()
    negative_phrases = [
        "tidak tahan lama", "kurang bagus", "gak bagus", "tidak bagus",
        "mengecewakan", "buruk", "rusak", "jelek", "lemah"
    ]
    positive_phrases = [
        "tahan lama", "keren", "kuat", "enteng", "rekomendasi",
        "mantap", "bagus", "desainnya keren", "cocok untuk", "tetap bagus"
    ]

    negative_score = sum(1 for phrase in negative_phrases if phrase in text)
    positive_score = sum(1 for phrase in positive_phrases if phrase in text)

    if negative_score > positive_score:
        return "Negatif"
    elif positive_score > negative_score:
        return "Positif"
    else:
        return "Netral"

def detect_product(msg):
    products = [
      "Touring-1000 Yellow", "Touring-1000 Yellow"
"Road Tire Tube", "Road Tire Tube"
"Men's Sports Shorts", "Men's Sports Shorts"
"LL Touring Frame - Blue", "LL Touring Frame - Blue"
"Mountain-100 Silver", "Mountain-100 Silver"
"Road-250 Red", "Road-250 Red"
"ML Road Rear Wheel", "ML Road Rear Wheel"
"LL Road Frame - Black", "LL Road Frame - Black"
"LL Touring Handlebars", "LL Touring Handlebars"
"Road-450 Red", "Road-450 Red"
"Mountain-100 Black", "Mountain-100 Black"
"Half-Finger Gloves", "Half-Finger Gloves"
"HL Road Frame - Black", "HL Road Frame - Black"
"Road-150 Red", "Road-150 Red"
"Road-550-W Yellow", "Road-550-W Yellow"
"LL Touring Frame - Blue", "LL Touring Frame - Blue"
"ML Road Frame - Red", "ML Road Frame - Red"
"Touring-1000 Yellow", "Touring-1000 Yellow"
"Touring-1000 Yellow", "Touring-1000 Yellow"
"LL Touring Frame - Blue", "LL Touring Frame - Blue"
"ML Road Pedal", "ML Road Pedal"
"Short-Sleeve Classic Jersey", "Short-Sleeve Classic Jersey"
"HL Road Tire", "HL Road Tire"
"Road-450 Red", "Road-450 Red"
"LL Road Frame - Black", "LL Road Frame - Black"
"Road-750 Black", "Road-750 Black"
"ML Mountain Front Wheel", "ML Mountain Front Wheel"
"LL Road Frame - Red", "LL Road Frame - Red"
"Mountain-500 Black", "Mountain-500 Black"
"Road-650 Black", "Road-650 Black"
"Front Brakes", "Front Brakes"
"Women's Tights", "Women's Tights"
"Mountain-200 Silver", "Mountain-200 Silver"
"HL Touring Frame - Blue", "HL Touring Frame - Blue"
"Road-750 Black", "Road-750 Black"
"HL Mountain Frame - Black", "HL Mountain Frame - Black"
"Mountain-100 Silver", "Mountain-100 Silver"
"Mountain-200 Black", "Mountain-200 Black"
"Touring-3000 Blue", "Touring-3000 Blue"
"Touring-3000 Blue", "Touring-3000 Blue"
"Mountain-400-W Silver", "Mountain-400-W Silver"
"Racing Socks", "Racing Socks"
"ML Mountain Seat/Saddle", "ML Mountain Seat/Saddle"
"Touring-3000 Yellow", "Touring-3000 Yellow"
"LL Mountain Rear Wheel", "LL Mountain Rear Wheel"
"Women's Mountain Shorts", "Women's Mountain Shorts"
"Men's Sports Shorts", "Men's Sports Shorts"
"LL Touring Frame - Yellow", "LL Touring Frame - Yellow"
"Mountain-300 Black", "Mountain-300 Black"
"LL Mountain Tire", "LL Mountain Tire"
"HL Road Front Wheel", "HL Road Front Wheel"
"LL Touring Frame - Yellow", "LL Touring Frame - Yellow"
"LL Touring Seat/Saddle", "LL Touring Seat/Saddle"
"HL Road Frame - Black", "HL Road Frame - Black"
"Mountain-400-W Silver", "Mountain-400-W Silver"
"ML Road Frame-W - Yellow", "ML Road Frame-W - Yellow"
"ML Fork", "ML Fork"
"Road-650 Black", "Road-650 Black"
"Women's Mountain Shorts", "Women's Mountain Shorts"
"Mountain-500 Black", "Mountain-500 Black"
"Road-150 Red", "Road-150 Red"
"Road-650 Black", "Road-650 Black"
"Mountain-500 Silver", "Mountain-500 Silver"
"HL Road Seat/Saddle", "HL Road Seat/Saddle"
"Touring-1000 Blue", "Touring-1000 Blue"
"LL Crankset", "LL Crankset"
"LL Road Frame - Black", "LL Road Frame - Black"
"Mountain Bottle Cage", "Mountain Bottle Cage"
"Sport-100 Helmet", "Sport-100 Helmet"
"Road-650 Red", "Road-650 Red"
"ML Road Frame-W - Yellow", "ML Road Frame-W - Yellow"
"Touring-2000 Blue", "Touring-2000 Blue"
"LL Fork", "LL Fork"
"Mountain-200 Black", "Mountain-200 Black"
"ML Mountain Frame - Black", "ML Mountain Frame - Black"
"HL Road Frame - Red", "HL Road Frame - Red"
"HL Mountain Frame - Silver", "HL Mountain Frame - Silver"
"All-Purpose Bike Stand", "All-Purpose Bike Stand"
"HL Mountain Rear Wheel", "HL Mountain Rear Wheel"
"Long-Sleeve Logo Jersey", "Long-Sleeve Logo Jersey"
"Short-Sleeve Classic Jersey", "Short-Sleeve Classic Jersey"
"Road-650 Red", "Road-650 Red"
"Hitch Rack - 4-Bike", "Hitch Rack - 4-Bike"
"Touring Front Wheel", "Touring Front Wheel"
"Touring-2000 Blue", "Touring-2000 Blue"
"Mountain-100 Black", "Mountain-100 Black"
"Mountain-500 Silver", "Mountain-500 Silver"
"Road-250 Black", "Road-250 Black"
"Mountain-100 Silver", "Mountain-100 Silver"
"Headlights - Dual-Beam", "Headlights - Dual-Beam"
"LL Road Seat/Saddle", "LL Road Seat/Saddle"
"HL Road Frame - Red", "HL Road Frame - Red"
"Road-250 Black", "Road-250 Black"
"Touring Tire", "Touring Tire"
"Short-Sleeve Classic Jersey", "Short-Sleeve Classic Jersey"
"Hydration Pack - 70 oz.", "Hydration Pack - 70 oz."
"Front Derailleur", "Front Derailleur"
"Sport-100 Helmet", "Sport-100 Helmet"
"Road-550-W Yellow", "Road-550-W Yellow"
"Mountain-500 Silver", "Mountain-500 Silver"
"Short-Sleeve Classic Jersey", "Short-Sleeve Classic Jersey"
"LL Road Frame - Red", "LL Road Frame - Red"
"HL Road Frame - Black", "HL Road Frame - Black"
"Touring-Panniers", "Touring-Panniers"
"ML Road Frame-W - Yellow", "ML Road Frame-W - Yellow"
"Mountain Bike Socks", "Mountain Bike Socks"
"Road-650 Red", "Road-650 Red"
"LL Mountain Frame - Black", "LL Mountain Frame - Black"
"Mountain-100 Black", "Mountain-100 Black"
"LL Mountain Pedal", "LL Mountain Pedal"
"Road-550-W Yellow", "Road-550-W Yellow"
"LL Touring Frame - Blue", "LL Touring Frame - Blue"
"HL Mountain Frame - Black", "HL Mountain Frame - Black"
"Patch Kit/8 Patches", "Patch Kit/8 Patches"
"Rear Derailleur", "Rear Derailleur"
"Full-Finger Gloves", "Full-Finger Gloves"
"Long-Sleeve Logo Jersey", "Long-Sleeve Logo Jersey"
"LL Road Tire", "LL Road Tire"
"ML Mountain Frame - Black", "ML Mountain Frame - Black"
"HL Road Pedal", "HL Road Pedal"
"ML Mountain Frame - Black", "ML Mountain Frame - Black"
"ML Road Tire", "ML Road Tire"
"HL Touring Handlebars", "HL Touring Handlebars"
"Mountain-300 Black", "Mountain-300 Black"
"Road Bottle Cage", "Road Bottle Cage"
"ML Crankset", "ML Crankset"
"Men's Bib-Shorts", "Men's Bib-Shorts"
"HL Mountain Tire", "HL Mountain Tire"
"ML Road Seat/Saddle", "ML Road Seat/Saddle"
"Touring-1000 Blue", "Touring-1000 Blue"
"Road-250 Black", "Road-250 Black"
"HL Mountain Front Wheel", "HL Mountain Front Wheel"
"Touring-3000 Yellow", "Touring-3000 Yellow"
"HL Bottom Bracket", "HL Bottom Bracket"
"Road-650 Red", "Road-650 Red"
"Headlights - Weatherproof", "Headlights - Weatherproof"
"ML Mountain Frame-W - Silver", "ML Mountain Frame-W - Silver"
"HL Touring Frame - Blue", "HL Touring Frame - Blue"
"HL Mountain Frame - Black", "HL Mountain Frame - Black"
"HL Mountain Frame - Silver", "HL Mountain Frame - Silver"
"Road-450 Red", "Road-450 Red"
"LL Road Pedal", "LL Road Pedal"
"HL Mountain Frame - Black", "HL Mountain Frame - Black"
"Men's Sports Shorts", "Men's Sports Shorts"
"Men's Bib-Shorts", "Men's Bib-Shorts"
"LL Mountain Front Wheel", "LL Mountain Front Wheel"
"Mountain-400-W Silver", "Mountain-400-W Silver"
"LL Mountain Seat/Saddle", "LL Mountain Seat/Saddle"
"Classic Vest", "Classic Vest"
"LL Bottom Bracket", "LL Bottom Bracket"
"HL Road Frame - Black", "HL Road Frame - Black"
"ML Road Frame - Red", "ML Road Frame - Red"
"ML Road Front Wheel", "ML Road Front Wheel"
"HL Road Frame - Black", "HL Road Frame - Black"
"LL Road Frame - Black", "LL Road Frame - Black"
"Long-Sleeve Logo Jersey", "Long-Sleeve Logo Jersey"
"Racing Socks", "Racing Socks"
"HL Touring Frame - Yellow", "HL Touring Frame - Yellow"
"Cable Lock", "Cable Lock"
"Taillights - Battery-Powered", "Taillights - Battery-Powered"
"Touring-3000 Blue", "Touring-3000 Blue"
"Road-150 Red", "Road-150 Red"
"ML Road Frame-W - Yellow", "ML Road Frame-W - Yellow"
"Touring-2000 Blue", "Touring-2000 Blue"
"ML Mountain Frame-W - Silver", "ML Mountain Frame-W - Silver"
"Mountain-100 Black", "Mountain-100 Black"
"HL Mountain Handlebars", "HL Mountain Handlebars"
"LL Road Frame - Red", "LL Road Frame - Red"
"ML Mountain Frame-W - Silver", "ML Mountain Frame-W - Silver"
"Touring-1000 Blue", "Touring-1000 Blue"
"Mountain-500 Silver", "Mountain-500 Silver"
"ML Mountain Handlebars", "ML Mountain Handlebars"
"Bike Wash - Dissolver", "Bike Wash - Dissolver"
"Mountain-400-W Silver", "Mountain-400-W Silver"
"Road-350-W Yellow", "Road-350-W Yellow"
"HL Road Handlebars", "HL Road Handlebars"
"Road-250 Red", "Road-250 Red"
"HL Mountain Frame - Silver", "HL Mountain Frame - Silver"
"ML Headset", "ML Headset"
"ML Mountain Rear Wheel", "ML Mountain Rear Wheel"
"HL Road Frame - Red", "HL Road Frame - Red"
"LL Mountain Frame - Silver", "LL Mountain Frame - Silver"
"Touring-1000 Blue", "Touring-1000 Blue"
"ML Road Frame - Red", "ML Road Frame - Red"
"Road-450 Red", "Road-450 Red"
"LL Mountain Frame - Silver", "LL Mountain Frame - Silver"
"Road-350-W Yellow", "Road-350-W Yellow"
"Mountain-500 Black", "Mountain-500 Black"
"HL Road Frame - Red", "HL Road Frame - Red"
"Touring Rear Wheel", "Touring Rear Wheel"
"ML Road Frame - Red", "ML Road Frame - Red"
"HL Mountain Frame - Silver", "HL Mountain Frame - Silver"
"HL Mountain Frame - Silver", "HL Mountain Frame - Silver"
"HL Touring Frame - Yellow", "HL Touring Frame - Yellow"
"Road-650 Red", "Road-650 Red"
"Rear Brakes", "Rear Brakes"
"LL Road Frame - Red", "LL Road Frame - Red"
"Road-250 Black", "Road-250 Black"
"Mountain Pump", "Mountain Pump"
"Mountain-200 Black", "Mountain-200 Black"
"LL Mountain Handlebars", "LL Mountain Handlebars"
"Road-550-W Yellow", "Road-550-W Yellow"
"Full-Finger Gloves", "Full-Finger Gloves"
"Mountain Tire Tube", "Mountain Tire Tube"
"Road-650 Red", "Road-650 Red"
"HL Mountain Pedal", "HL Mountain Pedal"
"Road-150 Red", "Road-150 Red"
"Mountain-100 Silver", "Mountain-100 Silver"
"Touring-2000 Blue", "Touring-2000 Blue"
"ML Road Handlebars", "ML Road Handlebars"
"LL Road Rear Wheel", "LL Road Rear Wheel"
"Fender Set - Mountain", "Fender Set - Mountain"
"Full-Finger Gloves", "Full-Finger Gloves"
"Touring Pedal", "Touring Pedal"
"Touring-3000 Blue", "Touring-3000 Blue"
"Half-Finger Gloves", "Half-Finger Gloves"
"Half-Finger Gloves", "Half-Finger Gloves"
"Men's Bib-Shorts", "Men's Bib-Shorts"
"LL Road Frame - Black", "LL Road Frame - Black"
"ML Mountain Pedal", "ML Mountain Pedal"
"Road-650 Black", "Road-650 Black"
"Touring-3000 Yellow", "Touring-3000 Yellow"
"Mountain-500 Silver", "Mountain-500 Silver"
"HL Touring Frame - Blue", "HL Touring Frame - Blue"
"Road-750 Black", "Road-750 Black"
"Mountain-500 Black", "Mountain-500 Black"
"LL Mountain Frame - Black", "LL Mountain Frame - Black"
"Mountain Bike Socks", "Mountain Bike Socks"
"HL Touring Frame - Blue", "HL Touring Frame - Blue"
"LL Mountain Frame - Silver", "LL Mountain Frame - Silver"
"ML Mountain Tire", "ML Mountain Tire"
"Mountain-300 Black", "Mountain-300 Black"
"HL Touring Frame - Yellow", "HL Touring Frame - Yellow"
"LL Touring Frame - Yellow", "LL Touring Frame - Yellow"
"ML Bottom Bracket", "ML Bottom Bracket"
"ML Touring Seat/Saddle", "ML Touring Seat/Saddle"
"Road-650 Black", "Road-650 Black"
"Mountain-500 Black", "Mountain-500 Black"
"LL Headset", "LL Headset"
"Men's Sports Shorts", "Men's Sports Shorts"
"Women's Mountain Shorts", "Women's Mountain Shorts"
"Road-150 Red", "Road-150 Red"
"Touring-1000 Yellow", "Touring-1000 Yellow"
"LL Road Frame - Black", "LL Road Frame - Black"
"LL Touring Frame - Yellow", "LL Touring Frame - Yellow"
"Mountain-200 Silver", "Mountain-200 Silver"
"Mountain-200 Silver", "Mountain-200 Silver"
"HL Headset", "HL Headset"
"HL Fork", "HL Fork"
"Mountain-300 Black", "Mountain-300 Black"
"Long-Sleeve Logo Jersey", "Long-Sleeve Logo Jersey"
"ML Road Frame - Red", "ML Road Frame - Red"
"HL Mountain Seat/Saddle", "HL Mountain Seat/Saddle"
"ML Mountain Frame-W - Silver", "ML Mountain Frame-W - Silver"
"Touring-3000 Blue", "Touring-3000 Blue"
"Road-350-W Yellow", "Road-350-W Yellow"
"HL Touring Frame - Yellow", "HL Touring Frame - Yellow"
"Touring-3000 Yellow", "Touring-3000 Yellow"
"Classic Vest", "Classic Vest"
"LL Mountain Frame - Black", "LL Mountain Frame - Black"
"Touring Tire Tube", "Touring Tire Tube"
"Road-550-W Yellow", "Road-550-W Yellow"
"Classic Vest", "Classic Vest"
"HL Crankset", "HL Crankset"
"LL Mountain Frame - Silver", "LL Mountain Frame - Silver"
"Touring-3000 Yellow", "Touring-3000 Yellow"
"Road-250 Red", "Road-250 Red"
"AWC Logo Cap", "AWC Logo Cap"
"LL Road Frame - Red", "LL Road Frame - Red"
"HL Road Frame - Red", "HL Road Frame - Red"
"Sport-100 Helmet", "Sport-100 Helmet"
"LL Mountain Frame - Black", "LL Mountain Frame - Black"
"HL Mountain Frame - Black", "HL Mountain Frame - Black"
"ML Mountain Frame - Black", "ML Mountain Frame - Black"
"Women's Tights", "Women's Tights"
"HL Road Rear Wheel", "HL Road Rear Wheel"
"LL Road Frame - Red", "LL Road Frame - Red"
"Road-250 Red", "Road-250 Red"
"Road-650 Black", "Road-650 Black"
"LL Mountain Frame - Silver", "LL Mountain Frame - Silver"
"Minipump", "Minipump"
"HL Touring Seat/Saddle", "HL Touring Seat/Saddle"
"Road-750 Black", "Road-750 Black"
"LL Road Handlebars", "LL Road Handlebars"
"Water Bottle - 30 oz.", "Water Bottle - 30 oz."
"LL Touring Frame - Yellow", "LL Touring Frame - Yellow"
"ML Road Frame-W - Yellow", "ML Road Frame-W - Yellow"
"Road-450 Red", "Road-450 Red"
"Chain", "Chain"
"Women's Tights", "Women's Tights"
"LL Road Front Wheel", "LL Road Front Wheel"
"LL Touring Frame - Blue", "LL Touring Frame - Blue"
"HL Road Frame - Red", "HL Road Frame - Red"
"LL Mountain Frame - Black", "LL Mountain Frame - Black"
"Road-350-W Yellow", "Road-350-W Yellow"
    ]
    for prod in products:
        if prod.lower() in msg.lower():
            return prod
    return None

def get_subcategory(prod):
    if prod is None:
        return None
    if "Road" in prod or "Mountain" in prod:
        return "Bikes"
    return "Components"

def analyze_txt():
    txt_folder = move_files_by_type()
    folder = txt_folder["txt"]
    files = [f for f in os.listdir(folder) if f.endswith(".txt")]
    data = []

    for file in files:
        with open(os.path.join(folder, file), "r", encoding="utf-8") as f:
            for line in f:
                parts = [p.strip() for p in line.strip().split(",", 2)]
                if len(parts) < 3:
                    continue
                posted_at = datetime.strptime(parts[0], "%Y-%m-%d")
                platform = parts[1]
                message = parts[2]
                username = "anon"
                product = detect_product(message)
                subcategory = get_subcategory(product)
                sentiment = extract_sentiment(message)
                post_count = 1
                data.append((
                    posted_at, platform, username, message,
                    product, subcategory, sentiment, post_count
                ))

    insert_to_staging(data)

def insert_to_staging(data):
    conn = get_staging_connection()
    cur = conn.cursor()
    for row in data:
        cur.execute("""
            INSERT INTO public.stg_social_posts (
                posted_at, platform, username, message,
                product_mention, productsubcategory, sentiment_label, post_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, row)
    conn.commit()
    cur.close()
    conn.close()
    print("Data inserted to staging.")

# =====================================================
# WORDCLOUD (optional)
# =====================================================
def generate_wordcloud():
    conn = get_staging_connection()
    cur = conn.cursor()
    cur.execute("SELECT message FROM public.stg_social_posts")
    messages = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    text = " ".join(messages)
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.tight_layout()
    plt.show()

# =====================================================
# PDF EXTRACTION
# =====================================================
def extract_pdf_data():
    records = []
    folder = move_files_by_type()["pdf"]
    pdf_files = [f for f in os.listdir(folder) if f.endswith(".pdf")]

    for filename in pdf_files:
        file_path = os.path.join(folder, filename)
        print(f"Processing file: {file_path}")
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if row[0] == "Company Name":
                            continue  # skip header
                        try:
                            company_name = row[0].strip()
                            date_str = row[1].strip()
                            revenue = float(row[2].replace(',', '').strip())
                            expense = float(row[3].replace(',', '').strip())
                            market_segment = row[4].strip()
                            notes = row[5].strip()
                            country_region = row[6].strip()
                            province_name = row[7].strip()
                            best_selling_product = row[8].strip()
                            product_category = row[9].strip()
                            report_date = datetime.strptime(date_str, '%d-%m-%Y').date()
                            records.append((
                                company_name, report_date, revenue, expense,
                                market_segment, notes, country_region,
                                province_name, best_selling_product, product_category
                            ))
                        except Exception as e:
                            print(f"Skipping row due to error: {e}")
    return records

def calculate_and_update_profit():
    conn = get_staging_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE financial_report
                SET profit = revenue - expense
                WHERE profit IS NULL;
            """)
    conn.close()
    print("Profit column updated successfully.")
    conn = get_staging_connection()
    with conn:
        with conn.cursor() as cur:
            # Update profit = revenue - expense
            cur.execute("""
                UPDATE financial_report
                SET profit = revenue - expense
                WHERE profit IS NULL;
            """)
    conn.close()
    print("Profit column updated successfully.")

def insert_to_staging_pdf(records):
    if not records:
        print("No valid records found.")
        return

    conn = get_staging_connection()
    insert_query = """
        INSERT INTO financial_report (
            company_name, report_date, revenue, expense,
            market_segment, notes, country_region,
            province_name, best_selling_product, product_category
        ) VALUES %s
    """
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, records)
    conn.close()
    print(f"{len(records)} records inserted successfully.")


# csv 
def load_raw_csv_to_staging(csv_base_dir):
    import os
    import csv
    import psycopg2

    conn = get_staging_connection()
    cur = conn.cursor()

    # 1. temperature_readings.csv
    with open(os.path.join(csv_base_dir, "temperature", "temperature_readings.csv"), encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            cur.execute("""
                INSERT INTO stg_temperature_readings
                (date, warehouse_id, warehouse_name, temperature_c, humidity, city)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, row)

    # 2. warehouse_inventory_conditions.csv
    with open(os.path.join(csv_base_dir, "warehouse", "warehouse_inventory_conditions.csv"), encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute("""
                INSERT INTO stg_warehouse_inventory_conditions
                (warehouse_id, date, item_type, damaged_units, total_units, storage_temp_c)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, row)

    # 3. energy_consumption.csv
    with open(os.path.join(csv_base_dir, "energy", "energy_consumption.csv"), encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute("""
                INSERT INTO stg_energy_consumption
                (date, warehouse_id, energy_used_kwh, avg_temp_c)
                VALUES (%s, %s, %s, %s)
            """, row)

    conn.commit()
    cur.close()
    conn.close()
    print(" Data CSV mentah berhasil dimasukkan ke tabel staging.")


def transform_staging_data():
    
    conn = get_staging_connection()
    cur = conn.cursor()

    # 1. Konversi semua suhu Fahrenheit ke Celsius (fix ROUND)
    cur.execute("""
        UPDATE stg_temperature_readings
        SET temperature_c = ROUND(((temperature_c - 32) * 5.0 / 9.0)::numeric, 2)
    """)

    cur.execute("""
        UPDATE stg_warehouse_inventory_conditions
        SET storage_temp_c = ROUND(((storage_temp_c - 32) * 5.0 / 9.0)::numeric, 2)
    """)

    cur.execute("""
        UPDATE stg_energy_consumption
        SET avg_temp_c = ROUND(((avg_temp_c - 32) * 5.0 / 9.0)::numeric, 2)
    """)

    # 2. Pembersihan teks: TRIM + NULLIF
    cur.execute("""
        UPDATE stg_temperature_readings
        SET 
            warehouse_name = NULLIF(TRIM(warehouse_name), ''),
            city = NULLIF(TRIM(city), '')
    """)

    cur.execute("""
        UPDATE stg_warehouse_inventory_conditions
        SET item_type = NULLIF(TRIM(item_type), '')
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Transformasi data berhasil: konversi suhu & pembersihan teks.")


