import json
import datetime
import socket
import os

def create_update_manifest_file(merged_dir):
    """
    Cria ou atualiza o arquivo "manifest.json" com os dados fornecidos na pasta "merged".

    Esta função recebe o caminho para a pasta "merged" e realiza o seguinte:
    - Cria um objeto de dicionário contendo os dados para o arquivo "manifest.json".
    - Obtém o nome do dispositivo em que o código está sendo executado.
    - Obtém a data e hora atual em formato ISO (UTC).
    - Escreve o conteúdo do dicionário no arquivo "manifest.json" com uma formatação indentada.

    Parâmetros:
        merged_dir (str): Caminho para a pasta "merged" onde o arquivo "manifest.json" será criado ou atualizado.

    Retorna:
        Nada. A função apenas cria ou atualiza o arquivo "manifest.json" na pasta "merged".
    """
    # Caminho completo para o arquivo "manifest.json" na pasta "merged"
    file_path = os.path.join(merged_dir, "manifest.json")

    # Obter o nome do dispositivo
    device_name = socket.gethostname()

    # Obtém a data e hora atual em formato ISO (UTC)
    current_datetime = datetime.datetime.now().isoformat()

    # Dados para o arquivo "manifest.json"
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
        # Escreve os dados no arquivo "manifest.json" com formatação indentada
        with open(file_path, 'w') as json_file:
            json.dump(manifest_data, json_file, indent=4)

        print("Arquivo manifest.json criado e atualizado com sucesso!")

    except Exception as e:
        print(f"Erro ao criar ou atualizar o arquivo manifest.json: {e}")
