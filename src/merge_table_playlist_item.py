import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_playlist_item(db_folder, merged_folder):
    """
    Merge the 'PlaylistItem' table from all the databases found in the 'db_folder' and merge it
    with the 'userData.db' file in the 'merged_folder'.

    For each database file found in the 'db_folder', this function performs the following steps:
    - Connects to the 'userData.db' in the 'merged_folder'.
    - Creates the 'PlaylistItem' table in the merged database if it does not already exist.
    - Reads records from the 'PlaylistItem' table in the current database.
    - Checks if each record already exists in the merged database based on the value of the 'PlaylistItemId' column.
    - If the record does not exist in the merged database, it is inserted directly.
    - If the record already exists in the merged database, a new random number is generated for the 'PlaylistItemId'.
      The new 'PlaylistItemId' is concatenated to the original value to avoid duplications.
      The record is then updated with the new 'PlaylistItemId' in the current database and inserted into the merged database.

    Parameters:
        db_folder (str): Path to the folder containing the database files to be merged.
        merged_folder (str): Path to the folder where the 'userData.db' file is located.

    Returns:
        Nothing. The function only merges the records from the 'PlaylistItem' table in all the databases.

    Example usage:
        merge_table_playlist_item("path_to_db_folder", "path_to_merged_folder")
    """
    # Connect to 'userData.db' in the merged folder
    merged_db_path = os.path.join(merged_folder, "userData.db")
    merged_conn = sqlite3.connect(merged_db_path)
    merged_cursor = merged_conn.cursor()

    # Create the 'PlaylistItem' table in the merged database if it doesn't exist
    merged_cursor.execute("CREATE TABLE IF NOT EXISTS PlaylistItem (PlaylistItemId INTEGER PRIMARY KEY, Label TEXT, StartTrimOffsetTicks INTEGER, EndTrimOffSetTicks INTEGER, Accuracy INTEGER, EndAction INTEGER, ThumbnailFilePath TEXT)")

    for db_file in os.listdir(db_folder):
        if db_file.endswith(".db"):
            db_path = os.path.join(db_folder, db_file)
            print(f"Connecting to file: {db_file}")

            # Connect to the current database file
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Read records from the 'PlaylistItem' table in the current database
            cursor.execute("SELECT * FROM PlaylistItem")
            records = cursor.fetchall()

            # Check if the record already exists in the merged database based on the 'PlaylistItemId' column
            for record in records:
                playlist_item_id = record[0]
                label = record[1]
                start_trim_offset_ticks = record[2]
                end_trim_offset_ticks = record[3]
                accuracy = record[4]
                end_action = record[5]
                thumbnail_file_path = record[6]

                try:
                    merged_cursor.execute("INSERT INTO PlaylistItem (PlaylistItemId, Label, StartTrimOffsetTicks, EndTrimOffSetTicks, Accuracy, EndAction, ThumbnailFilePath) VALUES (?, ?, ?, ?, ?, ?, ?)", (playlist_item_id, label, start_trim_offset_ticks, end_trim_offset_ticks, accuracy, end_action, thumbnail_file_path))
                    merged_conn.commit()
                except sqlite3.IntegrityError:
                    print(f"Record with PlaylistItemId {playlist_item_id} already exists in the merged database. Generating a new random number...")
                    new_playlist_item_id = f"{playlist_item_id}{random_id()}"
                    print(f"New value for PlaylistItemId: {new_playlist_item_id}")
                    update_database(db_path, "PlaylistItemId", playlist_item_id, new_playlist_item_id)
                    merged_cursor.execute("INSERT INTO PlaylistItem (PlaylistItemId, Label, StartTrimOffsetTicks, EndTrimOffSetTicks, Accuracy, EndAction, ThumbnailFilePath) VALUES (?, ?, ?, ?, ?, ?, ?)", (new_playlist_item_id, label, start_trim_offset_ticks, end_trim_offset_ticks, accuracy, end_action, thumbnail_file_path))
                    merged_conn.commit()

            cursor.close()
            conn.close()
            print(f"'PlaylistItem' table successfully merged in {db_file}!")

    merged_cursor.close()
    merged_conn.close()

