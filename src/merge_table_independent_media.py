import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_independent_media(db_folder_path, merged_folder_path):
    """
    Merge the "IndependentMedia" table from all the databases found in the DB folder
    and combine it with the "userData.db" file in the merged folder.

    For each database file found in the DB folder, this function performs the following:
    - Connects to the "userData.db" in the merged folder.
    - Creates the "IndependentMedia" table in the merged database if it does not already exist.
    - Reads the records from the "IndependentMedia" table in the current database.
    - Checks if each record already exists in the merged database based on the value of the "IndependentMediaId" column.
    - If the record does not exist in the merged database, it is inserted directly.
    - If the record already exists in the merged database, a new random number is generated for the "IndependentMediaId".
      The new "IndependentMediaId" is concatenated with the original value to avoid duplications.
      The record is then updated with the new "IndependentMediaId" in the current database and inserted into the merged database.

    Parameters:
        db_folder_path (str): Path to the folder containing the database files to be merged.
        merged_folder_path (str): Path to the folder where the "userData.db" file is located.

    Returns:
        None. The function only merges the records from the "IndependentMedia" table in all the databases.

    Example of use:
        merge_table_independent_media("path_to_DB_folder", "path_to_merged_folder")
    """
    # Connect to the "userData.db" in the merged folder
    merged_db_path = os.path.join(merged_folder_path, "userData.db")
    merged_conn = sqlite3.connect(merged_db_path)
    merged_cursor = merged_conn.cursor()

    # Create the "IndependentMedia" table in the merged database if it does not exist
    merged_cursor.execute("CREATE TABLE IF NOT EXISTS IndependentMedia (IndependentMediaId INTEGER PRIMARY KEY, OriginalFilename TEXT, FilePath TEXT, MimeType TEXT, Hash TEXT)")

    for db_file in os.listdir(db_folder_path):
        if db_file.endswith(".db"):
            db_path = os.path.join(db_folder_path, db_file)
            print(f"Connecting to file: {db_file}")

            # Connect to the current database file
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Read the records from the "IndependentMedia" table in the current database
            cursor.execute("SELECT * FROM IndependentMedia")
            records = cursor.fetchall()

            # Check if the record already exists in the merged database based on the value of "IndependentMediaId" column
            for record in records:
                independent_media_id = record[0]
                original_filename = record[1]
                file_path = record[2]
                mime_type = record[3]
                hash_value = record[4]

                try:
                    merged_cursor.execute("INSERT INTO IndependentMedia (IndependentMediaId, OriginalFilename, FilePath, MimeType, Hash) VALUES (?, ?, ?, ?, ?)", (independent_media_id, original_filename, file_path, mime_type, hash_value))
                    merged_conn.commit()
                except sqlite3.IntegrityError:
                    print(f"Record with IndependentMediaId {independent_media_id} already exists in the merged database. Generating a new random number...")
                    new_independent_media_id = f"{independent_media_id}{random_id()}"
                    print(f"New value for IndependentMediaId: {new_independent_media_id}")
                    update_database(db_path, "IndependentMediaId", independent_media_id, new_independent_media_id)
                    merged_cursor.execute("INSERT INTO IndependentMedia (IndependentMediaId, OriginalFilename, FilePath, MimeType, Hash) VALUES (?, ?, ?, ?, ?)", (new_independent_media_id, original_filename, file_path, mime_type, hash_value))
                    merged_conn.commit()

            cursor.close()
            conn.close()
            print(f"IndependentMedia table merged successfully in {db_file}!")

    merged_cursor.close()
    merged_conn.close()
