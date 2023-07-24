import os
import shutil
import sqlite3
import random
import zipfile

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
    root_dir = os.path.dirname(os.path.abspath(__file__))
    jwlibrary_files = [file for file in os.listdir(root_dir) if file.endswith(".jwlibrary")]

    if jwlibrary_files:
        print("Arquivos .jwlibrary encontrados na raiz do programa:")
        for file in jwlibrary_files:
            print(file)

        extract_jwlibrary_files([os.path.join(root_dir, file) for file in jwlibrary_files])
    else:
        print("Nenhum arquivo .jwlibrary encontrado na raiz do programa.")

def extract_jwlibrary_files(file_paths):
    root_dir = os.path.dirname(os.path.abspath(__file__))
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
                    shutil.copyfileobj(source, target)

if __name__ == "__main__":
    find_jwlibrary_files()

    # Copiar o arquivo ".userData.db" da pasta "src" para a pasta "merged"
    root_dir = os.path.dirname(os.path.abspath(__file__))
    src_user_data_db = os.path.join(root_dir, "src", "userData.db")
    merged_user_data_db = os.path.join(root_dir, "merged", "userData.db")
    shutil.copy(src_user_data_db, merged_user_data_db)
    
    pasta_db = "DB"
    num_arquivos_db = count_db(pasta_db)

    pasta_mesclada = "merged"
    os.makedirs(pasta_mesclada, exist_ok=True)

    merge_table_playlist_item(pasta_db, pasta_mesclada)
    merge_table_tag(pasta_db, pasta_mesclada)
    merge_table_tag_map(pasta_db, pasta_mesclada)
    merge_table_independent_media(pasta_db, pasta_mesclada)
    merge_table_playlist_item_marker(pasta_db, pasta_mesclada)
    merge_table_playlist_item_marker_bible_verse_map(pasta_db, pasta_mesclada)
    merge_table_playlist_item_location_map(pasta_db, pasta_mesclada)
    merge_table_playlist_item_independent_media_map(pasta_db, pasta_mesclada)
    merge_table_location(pasta_db, pasta_mesclada)
    update_last_modified(pasta_mesclada)
    insert_into_playlist_item_accuracy(pasta_mesclada)
    # create_update_manifest_file()
    create_update_manifest_file(pasta_mesclada)
    zip_merged_folder(pasta_mesclada)


    # Excluir a pasta "DB" após a mesclagem
    shutil.rmtree("DB")
    shutil.rmtree("merged")
