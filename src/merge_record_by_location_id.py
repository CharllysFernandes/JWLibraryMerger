import sqlite3

def merge_record_by_location_id(original_db_path, merged_db_path, location_id):
    """
    Merge a record with the specified LocationId from the original database to the merged database.

    Parameters:
        original_db_path (str): The path to the original database containing the record to be merged.
        merged_db_path (str): The path to the merged database where the record will be merged.
        location_id (int): The LocationId of the record to be merged.

    Description:
        The function connects to the original and merged databases and searches for the record with the provided LocationId
        in the original database. If the record is found, it is merged into the merged database.
        The new record in the merged database will retain the same LocationId and all other information from the original record.
        If the record is not found in the original database, the function prints an error message.

    Example of use:
        merge_record_by_location_id("original_database_path", "merged_database_path", 1234)
    """
    try:
        # Connect to the original database
        conn_original = sqlite3.connect(original_db_path)
        cursor_original = conn_original.cursor()

        # Connect to the merged database
        conn_merged = sqlite3.connect(merged_db_path)
        cursor_merged = conn_merged.cursor()

        # Search for the record with the provided LocationId in the original database
        cursor_original.execute("SELECT * FROM Location WHERE LocationId=?", (location_id,))
        record = cursor_original.fetchone()

        if record:
            # Merge the record into the merged database
            cursor_merged.execute("INSERT INTO Location (LocationId, BookNumber, ChapterNumber, DocumentId, Track, IssueTagNumber, KeySymbol, MepsLanguage, Type, Title) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", record)
            conn_merged.commit()
            print(f"Record with LocationId {location_id} merged successfully.")
        else:
            print(f"Record with LocationId {location_id} not found in the original database.")

    except sqlite3.Error as e:
        print(f"Error while merging the record using LocationId: {e}")

    finally:
        # Close the connections with the databases
        cursor_original.close()
        conn_original.close()
        cursor_merged.close()
        conn_merged.close()
