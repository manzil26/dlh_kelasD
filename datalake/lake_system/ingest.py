import os
import shutil

def organize_file(source_path, base_folder='input_folder'):
    os.makedirs(base_folder, exist_ok=True)
    subfolders = ['csv', 'pdf', 'txt']
    
    for folder in subfolders:
        os.makedirs(os.path.join(base_folder, folder), exist_ok=True)

    ext = os.path.splitext(source_path)[1].lower()

    if ext == '.txt':
        dest_folder = 'txt'
    elif ext == '.csv':
        dest_folder = 'csv'
    elif ext == '.pdf':
        dest_folder = 'pdf'
    else:
        raise ValueError(f"Ekstensi file tidak dikenali: {ext}")

    dest_path = os.path.join(base_folder, dest_folder, os.path.basename(source_path))
    shutil.copy2(source_path, dest_path)
    print(f"[INGEST] File dipindahkan ke: {dest_path}")
    return dest_path
