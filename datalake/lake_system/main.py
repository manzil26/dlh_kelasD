import os
from collections import Counter
from ingest import organize_file
from structure import extract_words
from analyze import generate_wordcloud
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

schema_name = "datalake"

# DB config
staging_uri = "postgresql+psycopg2://postgres:dataEngginer@localhost:5432/staggingDB"
dw_uri = "postgresql+psycopg2://postgres:dataEngginer@localhost:5432/adventureworksDw"


def create_schema_and_table(engine, schema_name, table_name):
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                word VARCHAR PRIMARY KEY,
                frequency INT
            )
        """))
        conn.commit()


def insert_words_to_table(engine, schema_name, table_name, words):
    # words is a dict {word: frequency}
    with engine.begin() as conn:
        for word, freq in words.items():
            conn.execute(text(f"""
                INSERT INTO {schema_name}.{table_name} (word, frequency) VALUES (:word, :freq)
                ON CONFLICT (word) DO UPDATE SET frequency = EXCLUDED.frequency
            """), {"word": word, "freq": freq})


def main():
    print("[MAIN] Memulai proses pipeline...")

    source_path = r"C:\Users\ASUS\Downloads\ulasan_penjualan.txt"

    # Step 1: Organize file
    organized_path = organize_file(source_path)
    print(f"[MAIN] File sudah diorganize di: {organized_path}")

    # Step 2: Extract words (hasilnya list)
    words_list = extract_words(organized_path)

    if not words_list:
        print("[WARNING] Tidak ada kata yang berhasil diekstrak!")
        return

    # Hitung frekuensi kata dari list
    words = dict(Counter(words_list))

    print(f"[MAIN] Total kata unik yang diekstrak: {len(words)}")

    # Step 3: Generate wordcloud
    generate_wordcloud(words)
    print("[MAIN] Wordcloud berhasil dibuat.")

    # Step 4: Insert to stagingDB
    try:
        staging_engine = create_engine(staging_uri)
        create_schema_and_table(staging_engine, schema_name, "staging_words")
        insert_words_to_table(staging_engine, schema_name, "staging_words", words)
        print("[MAIN] Data berhasil dimasukkan ke stagingDB.")
    except SQLAlchemyError as e:
        print(f"[ERROR] Gagal memasukkan data ke stagingDB: {e}")
        return

    # Step 5: Insert to data warehouse
    try:
        dw_engine = create_engine(dw_uri)
        create_schema_and_table(dw_engine, schema_name, "dw_words")
        insert_words_to_table(dw_engine, schema_name, "dw_words", words)
        print("[MAIN] Data berhasil dimasukkan ke adventureworksDw.")
    except SQLAlchemyError as e:
        print(f"[ERROR] Gagal memasukkan data ke adventureworksDw: {e}")
        return


if __name__ == "__main__":
    main()
