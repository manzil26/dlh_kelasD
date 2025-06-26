import os
from datetime import datetime
from ingest import move_files_by_type
import matplotlib.pyplot as plt
import psycopg2
from psycopg2.extras import execute_values
from wordcloud import WordCloud  
import pdfplumber 
import os
import csv
import psycopg2

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
        "Mountain-100 Black",
        "Flat Washer 6",
        "Thin-Jam Lock Nut 4",
        "Hex Nut 7",
        "ML Road Frame - Red, 52",
        "ComfortPro",
        "Pedal Clipless XC",
        "SpeedFit",
        "Jersey SpeedFit",
        "Saddle ComfortPro",
        "Clipless Pedal",
        "Mountain-100 Silver",
        "Mountain-500 Black",
        "Mountain-200 Silver",
        "Mountain-300 Black",
        "Mountain-400-W Silver",
        "Mountain-500 Silver",
        "Touring-1000 Yellow",
        "Touring-3000 Blue",
        "Touring-1000 Blue",
        "Touring-3000 Yellow",
        "Touring-2000 Blue",
        "Road-450 Red",
        "Road-650 Red",
        "Road-550-W Yellow",
        "Road-250 Black",
        "Road-150 Red",
        "Men's Sports Shorts",
        "Men's Bib-Shorts",
        "Women's Tights",
        "Women's Mountain Shorts",
        "Half-Finger Gloves",
        "Full-Finger Gloves",
        "Classic Vest",
        "Short-Sleeve Classic Jersey",
        "Long-Sleeve Logo Jersey",
        "Sport-100 Helmet",
        "Mountain Bike Socks",
        "Hydration Pack - 70 oz.",
        "Bike Wash - Dissolver",
        "Headlights - Dual-Beam",
        "Headlights - Weatherproof",
        "Taillights - Battery-Powered",
        "AWC Logo Cap",
        "Water Bottle - 30 oz.",
        "Patch Kit/8 Patches",
        "Cable Lock",
        "Chain",
        "Minipump",
        "ML Mountain Frame - Black",
        "ML Mountain Frame-W - Silver",
        "ML Road Frame - Red",
        "ML Road Frame-W - Yellow",
        "HL Mountain Frame - Black",
        "HL Mountain Frame - Silver",
        "HL Road Frame - Black",
        "HL Road Frame - Red",
        "LL Road Frame - Black",
        "LL Road Frame - Red",
        "LL Mountain Frame - Black",
        "LL Mountain Frame - Silver",
        "HL Touring Frame - Blue",
        "HL Touring Frame - Yellow",
        "LL Touring Frame - Blue",
        "LL Touring Frame - Yellow",
        "ML Road Pedal",
        "HL Road Pedal",
        "LL Road Pedal",
        "HL Mountain Pedal",
        "ML Mountain Pedal",
        "Touring Pedal",
        "HL Road Tire",
        "LL Road Tire",
        "Mountain Tire Tube",
        "Road Tire Tube",
        "Road Bottle Cage",
        "Mountain Bottle Cage",
        "Fender Set - Mountain",
        "ML Road Seat/Saddle",
        "LL Road Seat/Saddle",
        "ML Mountain Seat/Saddle",
        "LL Mountain Seat/Saddle",
        "HL Touring Seat/Saddle",
        "ML Touring Seat/Saddle",
        "LL Touring Seat/Saddle",
        "ML Crankset",
        "LL Crankset",
        "HL Crankset",
        "ML Fork",
        "HL Fork",
        "LL Fork",
        "ML Headset",
        "LL Headset",
        "HL Headset",
        "ML Road Handlebars",
        "LL Road Handlebars",
        "HL Road Handlebars",
        "HL Mountain Handlebars",
        "LL Mountain Handlebars",
        "HL Touring Handlebars",
        "LL Touring Handlebars",
        "Front Brakes",
        "Rear Brakes",
        "Front Derailleur",
        "Rear Derailleur",
        "ML Mountain Rear Wheel",
        "LL Mountain Rear Wheel",
        "HL Road Rear Wheel",
        "LL Road Rear Wheel",
        "HL Road Front Wheel",
        "ML Road Front Wheel",
        "Touring Front Wheel",
        "Touring Rear Wheel",
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


