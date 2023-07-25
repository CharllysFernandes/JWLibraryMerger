import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def get_unique_location_ids(database_path):
    """
    Get the unique LocationIds from the 'PlaylistItemLocationMap' table in the specified database.

    Parameters:
        database_path (str): Path to the database file.

    Returns:
        list: A list of unique LocationIds present in the 'PlaylistItemLocationMap' table.
    """
    # Connect to the database
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Fetch the unique values of LocationId from the PlaylistItemLocationMap table
    cursor.execute("SELECT DISTINCT LocationId FROM PlaylistItemLocationMap")
    location_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return location_ids

def merge_table_location(db_folder, merged_folder):
    """
    Merge the 'Location' table from all the databases found in the 'db_folder'
    and combine it with the 'userData.db' file in the 'merged_folder'.

    For each database file found in the 'db_folder', this function performs the following steps:
    - Connects to the 'userData.db' in the 'merged_folder'.
    - Creates the 'Location' table in the merged database if it does not already exist.
    - Reads the records from the 'Location' table in the current database.
    - Verifies if each record already exists in the merged database based on the value of the 'LocationId' column.
    - If the record does not exist in the merged database, it is inserted directly.
    - If the record already exists in the merged database, a new random number is generated for 'LocationId'.
      The new 'LocationId' is concatenated to the original value to avoid duplicates.
      The record is then updated with the new 'LocationId' in the current database and inserted into the merged database.

    Parameters:
        db_folder (str): Path to the folder containing the database files to be merged.
        merged_folder (str): Path to the folder where the "userData.db" file is located.

    Returns:
        None. The function only merges the records of the 'Location' table in all databases.

    Example of usage:
        merge_table_location("path_to_db_folder", "path_to_merged_folder")
    """
    # Connect to "userData.db" in the merged folder
    merged_db_path = os.path.join(merged_folder, "userData.db")
    merged_conn = sqlite3.connect(merged_db_path)
    merged_cursor = merged_conn.cursor()

    for db_file in os.listdir(db_folder):
        if db_file.endswith(".db"):
            db_path = os.path.join(db_folder, db_file)
            print(f"Connecting to file: {db_file}")

            # Connect to the current database file
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Get the unique LocationIds from the PlaylistItemLocationMap table in the current database
            location_ids = get_unique_location_ids(db_path)

            # Verify each LocationId in the Location table of the merged database
            for location_id in location_ids:
                # Read the record with the current LocationId in the current database
                cursor.execute("SELECT * FROM Location WHERE LocationId=?", (location_id,))
                record = cursor.fetchone()

                if record:
                    # Merge the record to the merged database
                    try:
                        merged_cursor.execute("INSERT INTO Location (LocationId, BookNumber, ChapterNumber, DocumentId, Track, IssueTagNumber, KeySymbol, MepsLanguage, Type, Title) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", record)
                        merged_conn.commit()
                        print(f"Record with LocationId {location_id} merged successfully.")
                    except sqlite3.IntegrityError:
                        print(f"Record with LocationId {location_id} already exists in the merged database. Generating a new random number...")
                        new_location_id = f"{location_id}{random_id()}"
                        print(f"New value for LocationId: {new_location_id}")
                        update_database(db_path, "LocationId", location_id, new_location_id)
                        merged_cursor.execute("INSERT INTO Location (LocationId, BookNumber, ChapterNumber, DocumentId, Track, IssueTagNumber, KeySymbol, MepsLanguage, Type, Title) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (new_location_id, record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9]))
                        merged_conn.commit()

            cursor.close()
            conn.close()
            print(f"Table 'Location' merged successfully in {db_file}!")

    merged_cursor.close()
    merged_conn.close()
