import psycopg2

# load_to_dw.py
import psycopg2
from datetime import date
from datetime import datetime
from psycopg2.errors import UniqueViolation
from analyze import get_staging_connection

# from psycopg2.extras import execute_values

def get_connection_dw():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="adventureworksDw",
        user="postgres",
        password="dataEngginer"
    )

# #TXT 
def insert_time(cur, posted_at):
    cur.execute("""
        INSERT INTO public.dim_time_txt (day, month, year)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING time_id
    """, (posted_at.day, posted_at.month, posted_at.year))
    result = cur.fetchone()
    if result:
        return result[0]
    
    cur.execute("""
        SELECT time_id 
        FROM public.dim_time_txt 
        WHERE day=%s AND month=%s AND year=%s
    """, (posted_at.day, posted_at.month, posted_at.year))
    return cur.fetchone()[0]


def insert_platform(cur, name):
    cur.execute("""
        INSERT INTO public.dim_platform (platform_name)
        VALUES (%s)
        ON CONFLICT (platform_name) DO NOTHING
        RETURNING platform_key
    """, (name,))
    result = cur.fetchone()
    if result:
        return result[0]
    
    cur.execute("SELECT platform_key FROM public.dim_platform WHERE platform_name=%s", (name,))
    return cur.fetchone()[0]


def calculate_score(label):
    return 1 if label.lower() == "positif" else -1 if label.lower() == "negatif" else 0


def insert_sentiment(cur, product_name, subcategory, sentiment_label):
    norm_product_name = product_name.strip()
    norm_subcategory = subcategory.strip()
    norm_label = sentiment_label.strip().capitalize()

    # Cek apakah sudah ada data aktif
    cur.execute("""
        SELECT sentiment_key, sentiment_label 
        FROM dim_sentiment 
        WHERE product_name = %s 
          AND product_subcategory = %s 
          AND is_current = TRUE
    """, (norm_product_name, norm_subcategory))
    
    result = cur.fetchone()

    if result:
        existing_key, existing_label = result
        existing_label = existing_label.strip().capitalize()

        if existing_label == norm_label:
            return existing_key
        else:
            cur.execute("""
                UPDATE dim_sentiment
                SET valid_to = CURRENT_DATE, is_current = FALSE
                WHERE sentiment_key = %s
            """, (existing_key,))
    
    # Insert baru
    cur.execute("""
        INSERT INTO dim_sentiment (
            product_name, product_subcategory, sentiment_label, valid_from, is_current
        ) VALUES (%s, %s, %s, %s, TRUE)
        RETURNING sentiment_key
    """, (norm_product_name, norm_subcategory, norm_label, date.today()))
    
    return cur.fetchone()[0]


def insert_fact(cur, time_id, platform_key, sentiment_key, post_count):
    cur.execute("""
        INSERT INTO public.fact_social_sentiment (time_id, platform_key, sentiment_key, post_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (time_id, platform_key, sentiment_key) DO NOTHING
    """, (time_id, platform_key, sentiment_key, post_count))

def run_structure():
    # Ambil data dari staging
    staging_conn = get_staging_connection()
    staging_cur = staging_conn.cursor()
    staging_cur.execute("""
        SELECT posted_at, platform, product_mention, 
               productsubcategory, sentiment_label, post_count 
        FROM public.stg_social_posts
    """)
    rows = staging_cur.fetchall()
    staging_cur.close()
    staging_conn.close()

    # Proses ke data warehouse
    dw_conn = get_connection_dw()
    dw_cur = dw_conn.cursor()

    for row in rows:
        posted_at, platform, product, subcategory, sentiment, post_count = row

        # Skip jika ada field yang penting bernilai None
        if product is None or subcategory is None or sentiment is None:
            continue

        time_id = insert_time(dw_cur, posted_at)
        platform_key = insert_platform(dw_cur, platform)
        sentiment_key = insert_sentiment(dw_cur, product, subcategory, sentiment)
        insert_fact(dw_cur, time_id, platform_key, sentiment_key, post_count)

    dw_conn.commit()
    dw_cur.close()
    dw_conn.close()
    print("Data inserted to fact_social_sentiment.")

##pdf 

def load_fact():
    staging_conn = get_staging_connection()
    dw_conn = get_connection_dw()

    with staging_conn.cursor() as stg_cur, dw_conn.cursor() as dw_cur:
        stg_cur.execute("""
            SELECT company_name, report_date, revenue, expense, market_segment,
                   notes, country_region, province_name, best_selling_product,
                   product_category
            FROM financial_report
        """)
        rows = stg_cur.fetchall()

        for row in rows:
            (company_name, report_date, revenue, expense, market_segment,
             notes, country_region, province_name, best_selling_product,
             product_category) = row

            profit = revenue - expense

            # ========== dim_company (SCD Type 2) ==========
            dw_cur.execute("""
                SELECT company_id, notes FROM dim_company
                WHERE company_name = %s AND is_current = TRUE
            """, (company_name,))
            existing_company = dw_cur.fetchone()

            if existing_company:
                current_company_id, current_notes = existing_company
                if current_notes != notes:
                    dw_cur.execute("""
                        UPDATE dim_company
                        SET valid_to = %s, is_current = FALSE
                        WHERE company_id = %s
                    """, (datetime.now().date(), current_company_id))
                    dw_cur.execute("""
                        INSERT INTO dim_company (company_name, notes, valid_from, is_current)
                        VALUES (%s, %s, %s, TRUE)
                        RETURNING company_id
                    """, (company_name, notes, datetime.now().date()))
                    company_id = dw_cur.fetchone()[0]
                else:
                    company_id = current_company_id
            else:
                dw_cur.execute("""
                    INSERT INTO dim_company (company_name, notes, valid_from, is_current)
                    VALUES (%s, %s, %s, TRUE)
                    RETURNING company_id
                """, (company_name, notes, datetime.now().date()))
                company_id = dw_cur.fetchone()[0]

            # ========== dim_date (tanpa report_date) ==========
            day, month, year = report_date.day, report_date.month, report_date.year
            dw_cur.execute("""
                INSERT INTO dim_date (day, month, year)
                VALUES (%s, %s, %s)
                ON CONFLICT (day, month, year) DO NOTHING
                RETURNING date_id
            """, (day, month, year))

            date_id = dw_cur.fetchone()[0] if dw_cur.rowcount > 0 else None
            if not date_id:
                dw_cur.execute("""
                    SELECT date_id FROM dim_date 
                    WHERE day = %s AND month = %s AND year = %s
                """, (day, month, year))
                date_id = dw_cur.fetchone()[0]

            # ========== dim_product_competitor (SCD Type 2) ==========
            dw_cur.execute("""
                SELECT product_id, product_category FROM dim_product_competitor
                WHERE best_selling_product = %s AND is_current = TRUE
            """, (best_selling_product,))
            existing_product = dw_cur.fetchone()

            if existing_product:
                current_product_id, current_category = existing_product
                if current_category != product_category:
                    dw_cur.execute("""
                        UPDATE dim_product_competitor
                        SET valid_to = %s, is_current = FALSE
                        WHERE product_id = %s
                    """, (datetime.now().date(), current_product_id))
                    dw_cur.execute("""
                        INSERT INTO dim_product_competitor (best_selling_product, product_category, valid_from, is_current)
                        VALUES (%s, %s, %s, TRUE)
                        RETURNING product_id
                    """, (best_selling_product, product_category, datetime.now().date()))
                    product_id = dw_cur.fetchone()[0]
                else:
                    product_id = current_product_id
            else:
                dw_cur.execute("""
                    INSERT INTO dim_product_competitor (best_selling_product, product_category, valid_from, is_current)
                    VALUES (%s, %s, %s, TRUE)
                    RETURNING product_id
                """, (best_selling_product, product_category, datetime.now().date()))
                product_id = dw_cur.fetchone()[0]

            # ========== dim_location ==========
            dw_cur.execute("""
                SELECT location_id FROM dim_location
                WHERE country_region = %s AND province_name = %s
            """, (country_region, province_name))
            location_result = dw_cur.fetchone()
            if location_result:
                location_id = location_result[0]
            else:
                dw_cur.execute("""
                    INSERT INTO dim_location (country_region, province_name)
                    VALUES (%s, %s)
                    RETURNING location_id
                """, (country_region, province_name))
                location_id = dw_cur.fetchone()[0]

            # ========== dim_segment ==========
            dw_cur.execute("""
                SELECT segment_id FROM dim_segment WHERE market_segment = %s
            """, (market_segment,))
            segment_result = dw_cur.fetchone()
            if segment_result:
                segment_id = segment_result[0]
            else:
                dw_cur.execute("""
                    INSERT INTO dim_segment (market_segment)
                    VALUES (%s)
                    RETURNING segment_id
                """, (market_segment,))
                segment_id = dw_cur.fetchone()[0]

            # ========== Insert ke fact ==========
            dw_cur.execute("""
                INSERT INTO fact_financial_report (
                    company_id, date_id, product_id, location_id, segment_id,
                    revenue, expense, profit
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                company_id, date_id, product_id, location_id, segment_id,
                revenue, expense, profit
            ))

    dw_conn.commit()
    staging_conn.close()
    dw_conn.close()
    print("Load fact selesai dan sukses.")


#csv 
def load_fact_to_dw():
    from datetime import date

    conn_stg = get_staging_connection()
    cur_stg = conn_stg.cursor()

    conn_dw = get_connection_dw()
    cur_dw = conn_dw.cursor()

    # ===== 1. Load dim_warehouse (SCD Type 2) dari staging inventory =====
    cur_stg.execute("SELECT DISTINCT warehouse_id, warehouse_name, city FROM stg_temperature_readings")
    for wh_id, wh_name, city in cur_stg.fetchall():
        cur_dw.execute("""
            SELECT warehouse_sk, warehouse_name, location
            FROM dim_warehouse
            WHERE warehouse_id = %s AND is_current = TRUE
        """, (wh_id,))
        current = cur_dw.fetchone()

        if current:
            warehouse_sk, curr_name, curr_loc = current
            if curr_name != wh_name or curr_loc != city:
                cur_dw.execute("""
                    UPDATE dim_warehouse
                    SET valid_to = %s, is_current = FALSE
                    WHERE warehouse_sk = %s
                """, (date.today(), warehouse_sk))
                cur_dw.execute("""
                    INSERT INTO dim_warehouse (warehouse_id, warehouse_name, location, valid_from, valid_to, is_current)
                    VALUES (%s, %s, %s, %s, NULL, TRUE)
                """, (wh_id, wh_name, city, date.today()))
        else:
            cur_dw.execute("""
                INSERT INTO dim_warehouse (warehouse_id, warehouse_name, location, valid_from, valid_to, is_current)
                VALUES (%s, %s, %s, %s, NULL, TRUE)
            """, (wh_id, wh_name, city, date.today()))

    # ===== 2. Load dim_time_warehouse (pakai day, month, year) =====
    cur_stg.execute("SELECT DISTINCT date FROM stg_warehouse_inventory_conditions")
    for (date_val,) in cur_stg.fetchall():
        day = date_val.day
        month = date_val.month
        year = date_val.year

        cur_dw.execute("""
            INSERT INTO dim_time_warehouse (day, month, year)
            VALUES (%s, %s, %s)
            ON CONFLICT (day, month, year) DO NOTHING
        """, (day, month, year))

    # ===== 3. Load fact_inventory_conditions =====
    cur_stg.execute("SELECT date, warehouse_id, item_type, damaged_units, total_units, storage_temp_c FROM stg_warehouse_inventory_conditions")
    for date_val, wh_id, item_type_raw, damaged, total, temp_c in cur_stg.fetchall():
        item_type = item_type_raw.strip()

        if not total or not item_type:
            continue

        # Ambil day/month/year
        day = date_val.day
        month = date_val.month
        year = date_val.year

        # Ambil time_id
        cur_dw.execute("""
            SELECT time_id FROM dim_time_warehouse
            WHERE day = %s AND month = %s AND year = %s
        """, (day, month, year))
        time_result = cur_dw.fetchone()
        if not time_result:
            continue
        time_id = time_result[0]

        # Ambil warehouse_sk
        cur_dw.execute("""
            SELECT warehouse_sk FROM dim_warehouse
            WHERE warehouse_id = %s AND is_current = TRUE
        """, (wh_id,))
        warehouse_result = cur_dw.fetchone()
        if not warehouse_result:
            continue
        warehouse_sk = warehouse_result[0]

        # Ambil product_key dari dim_product
        cur_dw.execute("""
            SELECT product_key FROM dim_product
            WHERE LOWER(TRIM(productname)) = LOWER(%s)
        """, (item_type,))
        product_result = cur_dw.fetchone()

        if not product_result:
            print(f"[SKIP] Produk '{item_type}' tidak ditemukan di dim_product.")
            continue

        product_key = product_result[0]

        # Insert ke tabel fakta
        cur_dw.execute("""
            INSERT INTO fact_inventory_conditions
            (warehouse_sk, time_id, product_key, damaged_units, total_units, storage_temp_c)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (warehouse_sk, time_id, product_key, damaged, total, temp_c))

    conn_dw.commit()
    cur_stg.close()
    cur_dw.close()
    conn_stg.close()
    conn_dw.close()

    print("Data berhasil dimuat ke fact_inventory_conditions.")
