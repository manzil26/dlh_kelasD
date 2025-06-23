import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert

from extract_transform import extract_transform  # import fungsi

print("=== Loading transformed data to Data Warehouse ===")

# Koneksi ke DW
dw_engine = create_engine("postgresql+psycopg2://postgres:dataEngginer@localhost:5432/adventureworksDw")

# Fungsi upsert
def fast_upsert(df, table_name, unique_key, engine):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables[table_name]

    insert_stmt = insert(table).values(df.to_dict(orient='records'))
    update_stmt = {
        col: insert_stmt.excluded[col]
        for col in df.columns if col != unique_key
    }

    on_conflict_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[unique_key],
        set_=update_stmt
    )

    with engine.begin() as conn:
        conn.execute(on_conflict_stmt)

# Jalankan fungsi extract_transform()
df_customer, df_product, df_territory, df_time, df_fact = extract_transform()

# Load ke DW
fast_upsert(df_customer[["customer_key", "customerid", "customername"]], "dim_customer", "customer_key", dw_engine)
fast_upsert(df_product[["product_key", "productid", "productsubcategory", "productname"]], "dim_product", "product_key", dw_engine)
fast_upsert(df_territory[["territory_key", "territoryid", "provincename", "countryregion"]], "dim_territory", "territory_key", dw_engine)
fast_upsert(df_time[["time_key", "year", "month", "day"]], "dim_time", "time_key", dw_engine)
fast_upsert(df_fact, "fact_sales", "sales_id", dw_engine)

print("Load selesai. Data berhasil dimasukkan ke data warehouse.")
