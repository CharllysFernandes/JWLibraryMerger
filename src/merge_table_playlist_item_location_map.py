import sqlite3
import os

from .update_databases import update_database
from .utils import random_id

def merge_table_playlist_item_location_map(db_folder, merged_folder):
    """
    Merge the 'PlaylistItemLocationMap' table from all the databases found in the 'db_folder'
    and combine it with the 'userData.db' file in the 'merged_folder'.

    For each database file found in the 'db_folder', this function performs the following steps:
    - Connects to the 'userData.db' in the 'merged_folder'.
    - Creates the 'PlaylistItemLocationMap' table in the merged database if it does not already exist.
    - Reads the records from the 'PlaylistItemLocationMap' table in the current database.
    - Checks if each record already exists in the merged database based on the values of the columns 'PlaylistItemId' and 'LocationId'.
    - If the record does not exist in the merged database, it is inserted directly.
    - If the record already exists in the merged database, new random numbers are generated for 'PlaylistItemId' and 'LocationId'.
      The new values are concatenated to the original values to avoid duplicates.
      The record is then updated with the new values in the current database and inserted into the merged database.

    Parameters:
        db_folder (str): Path to the folder containing the database files to be merged.
        merged_folder (str): Path to the folder where the "userData.db" file is located.

    Returns:
        None. The function only merges the records of the "PlaylistItemLocationMap" table in all databases.

    Example of usage:
        merge_table_playlist_item_location_map("path_to_db_folder", "path_to_merged_folder")
    """
    # Connect to "userData.db" in the merged folder
    merged_db_path = os.path.join(merged_folder, "userData.db")
    merged_conn = sqlite3.connect(merged_db_path)
    merged_cursor = merged_conn.cursor()

    # Create the "PlaylistItemLocationMap" table in the merged database if it does not exist
    merged_cursor.execute("CREATE TABLE IF NOT EXISTS PlaylistItemLocationMap (PlaylistItemId INTEGER, LocationId INTEGER, MajorMultimediaType INTEGER, BaseDurationTicks INTEGER, PRIMARY KEY (PlaylistItemId, LocationId))")

    for db_file in os.listdir(db_folder):
        if db_file.endswith(".db"):
            db_path = os.path.join(db_folder, db_file)
            print(f"Connecting to file: {db_file}")

            # Connect to the current database file
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Read the records from the "PlaylistItemLocationMap" table in the current database
            cursor.execute("SELECT * FROM PlaylistItemLocationMap")
            records = cursor.fetchall()

            # Check if the record already exists in the merged database based on the values of "PlaylistItemId" and "LocationId" columns
            for record in records:
                playlist_item_id = record[0]
                location_id = record[1]
                major_multimedia_type = record[2]
                base_duration_ticks = record[3]

                try:
                    merged_cursor.execute("INSERT INTO PlaylistItemLocationMap (PlaylistItemId, LocationId, MajorMultimediaType, BaseDurationTicks) VALUES (?, ?, ?, ?)", (playlist_item_id, location_id, major_multimedia_type, base_duration_ticks))

                    # merge_record_by_location_id(db_path, merged_db_path, location_id)

                    merged_conn.commit()
                except sqlite3.IntegrityError:
                    print(f"Record with PlaylistItemId {playlist_item_id} and LocationId {location_id} already exists in the merged database. Generating new random numbers...")
                    new_playlist_item_id = f"{playlist_item_id}{random_id()}"
                    new_location_id = f"{location_id}{random_id()}"
                    print(f"New values for PlaylistItemId: {new_playlist_item_id} and LocationId: {new_location_id}")
                    update_database(db_path, "PlaylistItemId", playlist_item_id, new_playlist_item_id)
                    update_database(db_path, "LocationId", location_id, new_location_id)
                    merged_cursor.execute("INSERT INTO PlaylistItemLocationMap (PlaylistItemId, LocationId, MajorMultimediaType, BaseDurationTicks) VALUES (?, ?, ?, ?)", (new_playlist_item_id, new_location_id, major_multimedia_type, base_duration_ticks))

                    merged_conn.commit()

            cursor.close()
            conn.close()
            print(f"Table 'PlaylistItemLocationMap' merged successfully in {db_file}!")

    merged_cursor.close()
    merged_conn.close()
