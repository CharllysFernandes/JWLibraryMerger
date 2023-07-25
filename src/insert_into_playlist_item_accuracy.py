import sqlite3
import os

def insert_into_playlist_item_accuracy(database_path):
    """
    Insert data into the PlaylistItemAccuracy table of the specified database.

    Parameters:
        database_path (str): The path to the database file where the data will be inserted.

    Description:
        The function connects to the specified database and inserts data into the PlaylistItemAccuracy table.
        The data is inserted into the "PlaylistItemAccuracyId" and "Description" columns of the table.
        The function attempts to insert two records into the table, representing two accuracy descriptions: "Accurate" and "NeedsUserVerification".
        The "INSERT OR IGNORE" clause is used to avoid the duplicate insertion of existing data.
        After inserting the data, the function commits the changes to the database.

    Note:
        The PlaylistItemAccuracy table must exist in the provided database; otherwise, an error will occur.

    Example of use:
        insert_into_playlist_item_accuracy("database_file_path.db")
    """
    # Combine the database path with the name of the "userData.db" file
    db_file_path = os.path.join(database_path, "userData.db")

    # Connect to the database
    conn = sqlite3.connect(db_file_path)

    try:
        # Create a cursor to execute SQL commands on the database
        cursor = conn.cursor()

        # Insert data into the PlaylistItemAccuracy table
        cursor.execute("INSERT OR IGNORE INTO PlaylistItemAccuracy (PlaylistItemAccuracyId, Description) VALUES (?, ?)", (1, "Accurate"))
        cursor.execute("INSERT OR IGNORE INTO PlaylistItemAccuracy (PlaylistItemAccuracyId, Description) VALUES (?, ?)", (2, "NeedsUserVerification"))

        # Commit the changes made to the database
        conn.commit()

        print("Data inserted into the PlaylistItemAccuracy table successfully.")

    except sqlite3.Error as e:
        print(f"Error while inserting data into the PlaylistItemAccuracy table: {e}")

    finally:
        # Close the connection with the database
        conn.close()
