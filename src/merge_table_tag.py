import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_tag(db_folder_path, merged_folder_path):
    """
    Merge the "Tag" table from all the databases found in the DB folder
    and combine it with the "userData.db" file in the merged folder.

    For each database file found in the DB folder, this function performs the following:
    - Connects to the "userData.db" in the merged folder.
    - Creates the "Tag" table in the merged database if it does not already exist.
    - Reads the records from the "Tag" table in the current database.
    - Checks if each record already exists in the merged database based on the value of the "TagId" column.
    - If the record does not exist in the merged database, it is inserted directly.
    - If the record already exists in the merged database, a new random number is generated for the "TagId".
      The new "TagId" is concatenated with the original value to avoid duplications.
      The record is then updated with the new "TagId" in the current database and inserted into the merged database.

    Parameters:
        db_folder_path (str): Path to the folder containing the database files to be merged.
        merged_folder_path (str): Path to the folder where the "userData.db" file is located.

    Returns:
        None. The function only merges the records from the "Tag" table in all the databases.

    Example of use:
        merge_table_tag("path_to_DB_folder", "path_to_merged_folder")
    """
    # Connect to the "userData.db" in the merged folder
    merged_db_path = os.path.join(merged_folder_path, "userData.db")
    merged_conn = sqlite3.connect(merged_db_path)
    merged_cursor = merged_conn.cursor()

    # Create the "Tag" table in the merged database if it does not exist
    merged_cursor.execute("CREATE TABLE IF NOT EXISTS Tag (TagId INTEGER PRIMARY KEY, Type TEXT, Name TEXT)")

    for db_file in os.listdir(db_folder_path):
        if db_file.endswith(".db"):
            db_path = os.path.join(db_folder_path, db_file)
            print(f"Connecting to file: {db_file}")

            # Connect to the current database file
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Read the records from the "Tag" table in the current database
            cursor.execute("SELECT * FROM Tag")
            records = cursor.fetchall()

            # Check if the record already exists in the merged database based on the value of "TagId" column
            for record in records:
                tag_id = record[0]
                type_value = record[1]
                name_value = record[2]

                try:
                    merged_cursor.execute("INSERT INTO Tag (TagId, Type, Name) VALUES (?, ?, ?)", (tag_id, type_value, name_value))
                    merged_conn.commit()
                except sqlite3.IntegrityError:
                    print(f"Record with TagId {tag_id} already exists in the merged database. Generating a new random number...")
                    new_tag_id = f"{tag_id}{random_id()}"
                    print(f"New value for TagId: {new_tag_id}")
                    update_database(db_path, "TagId", tag_id, new_tag_id)
                    merged_cursor.execute("INSERT INTO Tag (TagId, Type, Name) VALUES (?, ?, ?)", (new_tag_id, type_value, name_value))
                    merged_conn.commit()

            cursor.close()
            conn.close()
            print(f"Tag table merged successfully in {db_file}!")

    merged_cursor.close()
    merged_conn.close()
