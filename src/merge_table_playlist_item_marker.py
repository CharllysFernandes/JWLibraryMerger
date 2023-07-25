import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_playlist_item_marker(db_folder, merged_folder):
    """
    Merge the 'PlaylistItemMarker' table from all the databases found in the 'db_folder'
    and combine it with the 'userData.db' file in the 'merged_folder'.

    For each database file found in the 'db_folder', this function performs the following steps:
    - Connects to the 'userData.db' in the 'merged_folder'.
    - Creates the 'PlaylistItemMarker' table in the merged database if it does not already exist.
    - Reads the records from the 'PlaylistItemMarker' table in the current database.
    - Checks if each record already exists in the merged database based on the value of the 'PlaylistItemMarkerId' column.
    - If the record does not exist in the merged database, it is inserted directly.
    - If the record already exists in the merged database, a new random number is generated for the 'PlaylistItemMarkerId'.
      The new 'PlaylistItemMarkerId' is concatenated to the original value to avoid duplicates.
      The record is then updated with the new 'PlaylistItemMarkerId' in the current database and inserted into the merged database.

    Parameters:
        db_folder (str): Path to the folder containing the database files to be merged.
        merged_folder (str): Path to the folder where the "userData.db" file is located.

    Returns:
        None. The function only merges the records of the "PlaylistItemMarker" table in all databases.

    Example of usage:
        merge_table_playlist_item_marker("path_to_db_folder", "path_to_merged_folder")
    """
    # Connect to "userData.db" in the merged folder
    merged_db_path = os.path.join(merged_folder, "userData.db")
    merged_conn = sqlite3.connect(merged_db_path)
    merged_cursor = merged_conn.cursor()

    # Create the "PlaylistItemMarker" table in the merged database if it does not exist
    merged_cursor.execute("CREATE TABLE IF NOT EXISTS PlaylistItemMarker (PlaylistItemMarkerId INTEGER PRIMARY KEY, PlaylistItemId INTEGER, Label TEXT, StartTimeTicks INTEGER, DurationTicks INTEGER, EndTransitionDurationTicks INTEGER)")

    for db_file in os.listdir(db_folder):
        if db_file.endswith(".db"):
            db_path = os.path.join(db_folder, db_file)
            print(f"Connecting to file: {db_file}")

            # Connect to the current database file
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Read the records from the "PlaylistItemMarker" table in the current database
            cursor.execute("SELECT * FROM PlaylistItemMarker")
            records = cursor.fetchall()

            # Check if the record already exists in the merged database based on the value of "PlaylistItemMarkerId" column
            for record in records:
                playlist_item_marker_id = record[0]
                playlist_item_id = record[1]
                label = record[2]
                start_time_ticks = record[3]
                duration_ticks = record[4]
                end_transition_duration_ticks = record[5]

                try:
                    merged_cursor.execute("INSERT INTO PlaylistItemMarker (PlaylistItemMarkerId, PlaylistItemId, Label, StartTimeTicks, DurationTicks, EndTransitionDurationTicks) VALUES (?, ?, ?, ?, ?, ?)", (playlist_item_marker_id, playlist_item_id, label, start_time_ticks, duration_ticks, end_transition_duration_ticks))
                    merged_conn.commit()
                except sqlite3.IntegrityError:
                    print(f"Record with PlaylistItemMarkerId {playlist_item_marker_id} already exists in the merged database. Generating a new random number...")
                    new_playlist_item_marker_id = f"{playlist_item_marker_id}{random_id()}"
                    print(f"New value for PlaylistItemMarkerId: {new_playlist_item_marker_id}")
                    update_database(db_path, "PlaylistItemMarkerId", playlist_item_marker_id, new_playlist_item_marker_id)
                    merged_cursor.execute("INSERT INTO PlaylistItemMarker (PlaylistItemMarkerId, PlaylistItemId, Label, StartTimeTicks, DurationTicks, EndTransitionDurationTicks) VALUES (?, ?, ?, ?, ?, ?)", (new_playlist_item_marker_id, playlist_item_id, label, start_time_ticks, duration_ticks, end_transition_duration_ticks))
                    merged_conn.commit()

            cursor.close()
            conn.close()
            print(f"Table 'PlaylistItemMarker' merged successfully in {db_file}!")

    merged_cursor.close()
    merged_conn.close()
