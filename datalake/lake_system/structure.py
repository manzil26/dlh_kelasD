def extract_words(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        words = text.split()
        print(f"[STRUCTURE] Jumlah kata: {len(words)}")
        return words
    except Exception as e:
        print(f"[STRUCTURE] Error membaca file: {e}")
        return []
