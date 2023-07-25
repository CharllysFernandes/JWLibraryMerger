import os
import shutil
import sqlite3
import random
import zipfile
import datetime
import socket
import json

from src.utils import count_db
from src.merge_table_tag import merge_table_tag
from src.merge_table_independent_media import merge_table_independent_media
from src.merge_table_playlist_item import merge_table_playlist_item
from src.merge_table_playlist_item_marker import merge_table_playlist_item_marker
from src.merge_table_playlist_item_marker_bible_verse_map import merge_table_playlist_item_marker_bible_verse_map
from src.merge_table_playlist_item_location_map import merge_table_playlist_item_location_map
from src.merge_table_playlist_item_independent_media_map import merge_table_playlist_item_independent_media_map
from src.merge_table_tag_map import merge_table_tag_map
from src.merge_table_location import merge_table_location
from src.merge_table_last_modified import update_last_modified
from src.insert_into_playlist_item_accuracy import insert_into_playlist_item_accuracy
from src.create_manifest_json import create_update_manifest_file
from src.zip_merged_folder import zip_merged_folder

def find_jwlibrary_files():
    # Get the root directory of the program
    root_dir = os.path.dirname(os.path.abspath(__file__))

    # List all ".jwlibrary" files in the root directory
    jwlibrary_files = [file for file in os.listdir(root_dir) if file.endswith(".jwlibrary")]

    if jwlibrary_files:
        print("Arquivos .jwlibrary encontrados na raiz do programa:")
        for file in jwlibrary_files:
            print(file)

        # Extract the contents of the ".jwlibrary" files
        extract_jwlibrary_files([os.path.join(root_dir, file) for file in jwlibrary_files])
    else:
        print("Nenhum arquivo .jwlibrary encontrado na raiz do programa.")

def extract_jwlibrary_files(file_paths):
    # Get the root directory of the program
    root_dir = os.path.dirname(os.path.abspath(__file__))

    # Directories to store extracted database files and merged files
    db_dir = os.path.join(root_dir, "DB")
    merged_dir = os.path.join(root_dir, "merged")
    os.makedirs(db_dir, exist_ok=True)
    os.makedirs(merged_dir, exist_ok=True)

    for file_path in file_paths:
        if not os.path.exists(file_path):
            print(f"Arquivo não encontrado: {file_path}")
            continue

        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                file_name = os.path.basename(file_info.filename)

                # Extract and organize the files based on their types
                if file_name == "userData.db":
                    count = 1
                    while True:
                        base_name, ext = os.path.splitext(file_name)
                        extracted_path = os.path.join(db_dir, f"{base_name}({count}).db")
                        if not os.path.exists(extracted_path):
                            break
                        count += 1
                elif file_name.endswith(".db"):
                    extracted_path = os.path.join(db_dir, file_name)
                elif not file_name.endswith(".json"):
                    extracted_path = os.path.join(merged_dir, file_name)
                else:
                    continue

                with zip_ref.open(file_info) as source, open(extracted_path, "wb") as target:
                    # Copy the contents of the files to their respective locations
                    shutil.copyfileobj(source, target)

if __name__ == "__main__":
    # Step 1: Find JW Library files in the root directory and extract their contents
    find_jwlibrary_files()

    # Step 2: Copy "userData.db" from the "src" folder to the "merged" folder
    root_dir = os.path.dirname(os.path.abspath(__file__))
    src_user_data_db = os.path.join(root_dir, "src", "userData.db")
    merged_user_data_db = os.path.join(root_dir, "merged", "userData.db")
    shutil.copy(src_user_data_db, merged_user_data_db)
    
    # Step 3: Define the paths for the "DB" and "merged" directories
    pasta_db = "DB"
    num_arquivos_db = count_db(pasta_db)
    pasta_mesclada = "merged"
    os.makedirs(pasta_mesclada, exist_ok=True)

    # Step 4: Merge various tables from the "DB" directory into the "merged" directory
    merge_table_playlist_item(pasta_db, pasta_mesclada)
    merge_table_tag(pasta_db, pasta_mesclada)
    merge_table_tag_map(pasta_db, pasta_mesclada)
    merge_table_independent_media(pasta_db, pasta_mesclada)
    merge_table_playlist_item_marker(pasta_db, pasta_mesclada)
    merge_table_playlist_item_marker_bible_verse_map(pasta_db, pasta_mesclada)
    merge_table_playlist_item_location_map(pasta_db, pasta_mesclada)
    merge_table_playlist_item_independent_media_map(pasta_db, pasta_mesclada)
    merge_table_location(pasta_db, pasta_mesclada)

    # Step 5: Update the "LastModified" column in the "LastModified" table
    update_last_modified(pasta_mesclada)

    # Step 6: Insert data into the "PlaylistItemAccuracy" table
    insert_into_playlist_item_accuracy(pasta_mesclada)

    # Step 7: Create or update the "manifest.json" file
    create_update_manifest_file(pasta_mesclada)

    # Step 8: Zip the "merged" folder to create the final ".jwlibrary" file
    zip_merged_folder(pasta_mesclada)

    # Step 9: Cleanup - remove the "DB" and "merged" directories after merging and zipping
    shutil.rmtree("DB")
    shutil.rmtree("merged")
