import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_playlist_item_marker_bible_verse_map(db_folder, merged_folder):
    """
    Merge the 'PlaylistItemMarkerBibleVerseMap' table from all the databases found in the 'db_folder'
    and combine it with the 'userData.db' file in the 'merged_folder'.

    For each database file found in the 'db_folder', this function performs the following steps:
    - Connects to the 'userData.db' in the 'merged_folder'.
    - Creates the 'PlaylistItemMarkerBibleVerseMap' table in the merged database if it does not already exist.
    - Reads the records from the 'PlaylistItemMarkerBibleVerseMap' table in the current database.
    - Checks if each record already exists in the merged database based on the values of the columns 'PlaylistItemMarkerId' and 'VerseId'.
    - If the record does not exist in the merged database, it is inserted directly.
    - If the record already exists in the merged database, new random numbers are generated for 'PlaylistItemMarkerId' and 'VerseId'.
      The new values are concatenated to the original values to avoid duplicates.
      The record is then updated with the new values in the current database and inserted into the merged database.

    Parameters:
        db_folder (str): Path to the folder containing the database files to be merged.
        merged_folder (str): Path to the folder where the "userData.db" file is located.

    Returns:
        None. The function only merges the records of the "PlaylistItemMarkerBibleVerseMap" table in all databases.

    Example of usage:
        merge_table_playlist_item_marker_bible_verse_map("path_to_db_folder", "path_to_merged_folder")
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

            # Read the records from the "PlaylistItemMarkerBibleVerseMap" table in the current database
            cursor.execute("SELECT * FROM PlaylistItemMarkerBibleVerseMap")
            records = cursor.fetchall()

            # Check if the record already exists in the merged database based on the values of "PlaylistItemMarkerId" and "VerseId" columns
            for record in records:
                playlist_item_marker_id = record[0]
                verse_id = record[1]

                try:
                    merged_cursor.execute("INSERT INTO PlaylistItemMarkerBibleVerseMap (PlaylistItemMarkerId, VerseId) VALUES (?, ?)", (playlist_item_marker_id, verse_id))
                    merged_conn.commit()
                except sqlite3.IntegrityError:
                    print(f"Record with PlaylistItemMarkerId {playlist_item_marker_id} and VerseId {verse_id} already exists in the merged database. Generating new random numbers...")
                    new_playlist_item_marker_id = f"{playlist_item_marker_id}{random_id()}"
                    new_verse_id = f"{verse_id}{random_id()}"
                    print(f"New values for PlaylistItemMarkerId: {new_playlist_item_marker_id} and VerseId: {new_verse_id}")
                    update_database(db_path, "PlaylistItemMarkerId", playlist_item_marker_id, new_playlist_item_marker_id)
                    update_database(db_path, "VerseId", verse_id, new_verse_id)
                    merged_cursor.execute("INSERT INTO PlaylistItemMarkerBibleVerseMap (PlaylistItemMarkerId, VerseId) VALUES (?, ?)", (new_playlist_item_marker_id, new_verse_id))
                    merged_conn.commit()

            cursor.close()
            conn.close()
            print(f"Table 'PlaylistItemMarkerBibleVerseMap' merged successfully in {db_file}!")

    merged_cursor.close()
    merged_conn.close()
