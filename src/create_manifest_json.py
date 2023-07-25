import json
import datetime
import socket
import os

def create_update_manifest_file(merged_dir):
    """
    Create or update the "manifest.json" file with the provided data in the "merged" folder.

    This function receives the path to the "merged" folder and performs the following:
    - Creates a dictionary object containing the data for the "manifest.json" file.
    - Retrieves the name of the device on which the code is being executed.
    - Gets the current date and time in ISO format (UTC).
    - Writes the dictionary content to the "manifest.json" file with indented formatting.

    Parameters:
        merged_dir (str): Path to the "merged" folder where the "manifest.json" file will be created or updated.

    Returns:
        None. The function only creates or updates the "manifest.json" file in the "merged" folder.
    """
    # Complete file path for the "manifest.json" file in the "merged" folder
    file_path = os.path.join(merged_dir, "manifest.json")

    # Get the name of the device
    device_name = socket.gethostname()

    # Get the current date and time in ISO format (UTC)
    current_datetime = datetime.datetime.now().isoformat()

    # Data for the "manifest.json" file
    manifest_data = {
        "name": "Playlist_Merged.jwlibrary",
        "creationDate": current_datetime,
        "version": 1,
        "type": 0,
        "userDataBackup": {
            "lastModifiedDate": current_datetime,
            "deviceName": device_name,
            "databaseName": "userData.db",
            "hash": "f626f3f2f9622b80182aa5947b62d4fad296e7f9da56c599e177bc6d078c8eab",
            "schemaVersion": 11
        }
    }

    try:
        # Write the data to the "manifest.json" file with indented formatting
        with open(file_path, 'w') as json_file:
            json.dump(manifest_data, json_file, indent=4)

        print("manifest.json file created and updated successfully!")

    except Exception as e:
        print(f"Error creating or updating the manifest.json file: {e}")
