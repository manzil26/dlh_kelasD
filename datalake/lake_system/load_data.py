import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError

schema_name = "datalake"
staging_uri = "postgresql+psycopg2://postgres:dataEngginer@localhost:5432/staggingDB"
dw_uri = "postgresql+psycopg2://postgres:dataEngginer@localhost:5432/adventureworksDw"

def load_table(table_name_staging, table_name_dw):
    staging_engine = create_engine(staging_uri)
    dw_engine = create_engine(dw_uri)
    try:
        print(f"[LOAD] Memuat data dari `{schema_name}.{table_name_staging}`...")

        # Ambil data dari staging
        df = pd.read_sql(f'SELECT * FROM {schema_name}.{table_name_staging}', staging_engine)

        # Cek apakah tabel tujuan sudah ada di DW
        inspector = inspect(dw_engine)
        existing_tables = inspector.get_table_names(schema=schema_name)

        if table_name_dw in existing_tables:
            df.to_sql(table_name_dw, dw_engine, if_exists='replace', index=False, schema=schema_name)
            print(f"[REPLACE] Tabel `{schema_name}.{table_name_dw}` berhasil di-replace di Data Warehouse.")
        else:
            df.to_sql(table_name_dw, dw_engine, if_exists='fail', index=False, schema=schema_name)
            print(f"[LOAD] Tabel `{schema_name}.{table_name_dw}` berhasil di-load ke Data Warehouse.")

    except SQLAlchemyError as e:
        print(f"[ERROR] Gagal load dari staging ke DW: {e}")

if __name__ == "__main__":
    load_table("staging_words", "dw_words")
