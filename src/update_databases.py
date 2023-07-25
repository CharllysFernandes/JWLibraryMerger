import sqlite3

def update_database(file_path_database, column_to_update, initial_value, updated_value):
    """
    Update records in all tables of a SQLite database where the specified column
    has the initial value provided, replacing it with the updated value.

    Parameters:
        file_path_database (str): Path to the SQLite database file to be updated.
        column_to_update (str): Name of the column to be updated in the tables.
        initial_value (str or int): Initial value in the column to be replaced.
        updated_value (str or int): New value that will replace the initial value.

    Returns:
        None. The function only updates the records in the database.

    Example of use:
        update_database("path_to_your_database.db", "Name", "OldValue", "NewValue")
    """
    # Connect to the database
    conn = sqlite3.connect(file_path_database)
    cursor = conn.cursor()

    # Get the list of tables in the database
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Update the record in each table that contains the specified column
    for table in tables:
        table = table[0]

        try:
            cursor.execute(f"UPDATE {table} SET {column_to_update} = ? WHERE {column_to_update} = ?", (updated_value, initial_value))
            conn.commit()
            print(f"Record updated in '{table}' - '{column_to_update}': {initial_value} -> {updated_value}")

        except sqlite3.Error as e:
            print(f"Error updating the record in '{table}': {e}")
            conn.rollback()

    cursor.close()
    conn.close()

    print(f"{table} in {file_path_database} updated successfully!")
