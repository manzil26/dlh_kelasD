



#import txt 
from ingest import move_files_by_type
from analyze import analyze_txt
from analyze import generate_wordcloud
from structure import run_structure


# # import pdf 
from analyze import extract_pdf_data
from analyze import insert_to_staging_pdf
from analyze import calculate_and_update_profit
from structure import load_fact

# import csv 
from analyze import load_raw_csv_to_staging 
from analyze import transform_staging_data 
from structure import load_fact_to_dw
from ingest import move_files_by_type 

# # # 1. Pindahkan file ke folder input
move_files_by_type()



# # # Analisis dan insert ke satgging 
# #txt 
analyze_txt() 
generate_wordcloud()
# #pdf 
# records = extract_pdf_data()
# insert_to_staging_pdf(records)
# calculate_and_update_profit()
# #csv 
# file_paths = move_files_by_type()
# csv_base_dir = file_paths["csv"]
# load_raw_csv_to_staging(csv_base_dir)
# transform_staging_data()


# # # #Proses  ke Data Warehouse
run_structure()
# # #pdf 
load_fact()
# #csv 
load_fact_to_dw()  


