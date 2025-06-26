import os
import shutil
import os
import shutil
import pandas as pd


def move_files_by_type():
    source_dir = "D:/dlh_kelasD/datalakes/bisnis"
    target_base = "D:/dlh_kelasD/datalakes/input_folder"

    # Path berdasarkan ekstensi
    file_paths = {
        "pdf": os.path.join(target_base, "pdf"),
        "csv": os.path.join(target_base, "csv"),
        "txt": os.path.join(target_base, "txt"),
    }

    # Membuat Folder Tujuan Jika Belum Ada
    for folder in file_paths.values():
        os.makedirs(folder, exist_ok=True)

    files_moved = False
# Proses Pemindahan File
    for filename in os.listdir(source_dir):
        ext = os.path.splitext(filename)[1].lower()
        if ext in [".pdf", ".csv", ".txt"]:
            subfolder = ext[1:]
            target_dir = file_paths[subfolder]

            # Jika CSV, buat subfolder berdasarkan jenis file
            if ext == ".csv":
                if "temperature" in filename:
                    target_dir = os.path.join(file_paths["csv"], "temperature")
                elif "warehouse" in filename:
                    target_dir = os.path.join(file_paths["csv"], "warehouse")
                elif "energy" in filename:
                    target_dir = os.path.join(file_paths["csv"], "energy")
                os.makedirs(target_dir, exist_ok=True)

            shutil.move(os.path.join(source_dir, filename), os.path.join(target_dir, filename))
            print(f"Moved '{filename}' to '{target_dir}'")
            files_moved = True

    if not files_moved:
        print("Tidak ada file baru yang dipindahkan dari folder 'bisnis'.")

    return file_paths



