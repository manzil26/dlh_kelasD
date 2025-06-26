import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_values
from extract_transform import extract_transform
import time

print("=== Loading transformed data to Data Warehouse ===")

# Koneksi ke Data Warehouse
dw_engine = create_engine("postgresql+psycopg2://postgres:dataEngginer@localhost:5432/adventureworksDw")

# Fungsi UPSERT cepat menggunakan psycopg2.execute_values
def fast_upsert_psycopg2(df, table_name, unique_key, engine):
    if df.empty:
        print(f"Skip {table_name}: DataFrame kosong")
        return

    records = df.to_dict('records')
    columns = df.columns.tolist()

    insert_stmt = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT ({unique_key}) DO UPDATE SET
        {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != unique_key])}
    """

    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        try:
            execute_values(cur, insert_stmt, [tuple(row[col] for col in columns) for row in records])
        finally:
            cur.close()
        conn.commit()
        print(f"{table_name}: {len(df)} baris berhasil di-upsert.")
    finally:
        conn.close()

# Mulai stopwatch
start = time.time()

# Jalankan extract & transform
print("Transformasi dimulai...")
df_customer, df_product, df_territory, df_time, df_fact = extract_transform()
print("Transformasi selesai.")

# Load ke Data Warehouse
fast_upsert_psycopg2(df_customer[["customer_key", "customerid", "customername"]], "dim_customer", "customer_key", dw_engine)
fast_upsert_psycopg2(df_product[["product_key", "productid", "productsubcategory", "productname"]], "dim_product", "product_key", dw_engine)
fast_upsert_psycopg2(df_territory[["territory_key", "territoryid", "provincename", "countryregion"]], "dim_territory", "territory_key", dw_engine)
fast_upsert_psycopg2(df_time[["time_key", "year", "month", "day"]], "dim_time", "time_key", dw_engine)
fast_upsert_psycopg2(df_fact, "fact_sales", "sales_id", dw_engine)

print("Load selesai. Data berhasil dimasukkan ke Data Warehouse.")
print("Waktu total:", round(time.time() - start, 2), "detik")
