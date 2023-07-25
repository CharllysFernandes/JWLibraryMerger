import sqlite3
import os
from datetime import datetime, timezone

def update_last_modified(database_path):
    """
    Update the "LastModified" column in the "LastModified" table of the database with the current date and time in ISO format (UTC).

    This function takes the path to the merged database and performs the following steps:
    - Connects to the database.
    - Gets the current date and time in UTC timezone.
    - Formats the date and time in the "2023-06-21T21:37:13Z" format.
    - Updates the value in the "LastModified" column of the "LastModified" table.

    Parameters:
        database_path (str): Path to the merged database containing the "LastModified" table.

    Returns:
        None. The function only updates the value of the "LastModified" column in the database.
    """
    # Full path to the database file
    merged_db_path = os.path.join(database_path, "userData.db")

    # Connect to the database
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    try:
        # Get the current date and time in ISO format (UTC)
        current_datetime = datetime.now(timezone.utc)

        # Format the date and time in the "2023-06-21T21:37:13Z" format
        new_last_modified = current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Update the LastModified value in the table
        cursor.execute("UPDATE LastModified SET LastModified = ?", (new_last_modified,))
        conn.commit()

        print("LastModified table update successful.")

    except sqlite3.Error as e:
        print(f"Error updating the LastModified table: {e}")

    finally:
        # Close the connection with the database
        cursor.close()
        conn.close()
