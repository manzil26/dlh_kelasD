from sqlalchemy import text

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
    # Asumsikan words = dict {word: frequency}
    with engine.begin() as conn:
        for word, freq in words.items():
            conn.execute(text(f"""
                INSERT INTO {schema_name}.{table_name} (word, frequency) VALUES (:word, :freq)
                ON CONFLICT (word) DO UPDATE SET frequency = EXCLUDED.frequency
            """), {"word": word, "freq": freq})
