import sqlite3
import os
from .update_databases import update_database
from .merge_record_by_location_id import merge_record_by_location_id
from .utils import random_id

def merge_table_playlist_item_location_map(pasta_db, pasta_mesclada):
    """
    Mescla a tabela "PlaylistItemLocationMap" de todos os bancos de dados encontrados na pasta DB
    e une ao arquivo "userData.db" na pasta file_merged.

    Para cada arquivo de banco de dados encontrado na pasta DB, esta função realiza o seguinte:
    - Conecta-se ao "userData.db" na pasta mesclada.
    - Cria a tabela "PlaylistItemLocationMap" no banco de dados mesclado, caso ela ainda não exista.
    - Lê os registros da tabela "PlaylistItemLocationMap" no banco de dados atual.
    - Verifica se cada registro já existe no banco de dados mesclado com base nos valores das colunas "PlaylistItemId" e "LocationId".
    - Caso o registro não exista no banco de dados mesclado, insere-o diretamente.
    - Se o registro já existir no banco de dados mesclado, gera novos números aleatórios para "PlaylistItemId" e "LocationId".
      Os novos valores são concatenados aos valores originais para evitar duplicações.
      O registro é então atualizado com os novos valores no banco de dados atual e inserido no banco de dados mesclado.

    Parâmetros:
        pasta_db (str): Caminho para a pasta que contém os arquivos de banco de dados a serem mesclados.
        pasta_mesclada (str): Caminho para a pasta onde o arquivo "userData.db" está localizado.

    Retorna:
        Nada. A função apenas mescla os registros da tabela "PlaylistItemLocationMap" em todos os bancos de dados.

    Exemplo de uso:
        merge_table_playlist_item_location_map("caminho_para_pasta_DB", "caminho_para_pasta_file_merged")
    """
    # Conectar ao "userData.db" na pasta mesclada
    caminho_db_mesclado = os.path.join(pasta_mesclada, "userData.db")
    conn_mesclado = sqlite3.connect(caminho_db_mesclado)
    cursor_mesclado = conn_mesclado.cursor()

    # Criar a tabela "PlaylistItemLocationMap" no banco de dados mesclado, caso ainda não exista
    cursor_mesclado.execute("CREATE TABLE IF NOT EXISTS PlaylistItemLocationMap (PlaylistItemId INTEGER, LocationId INTEGER, MajorMultimediaType INTEGER, BaseDurationTicks INTEGER, PRIMARY KEY (PlaylistItemId, LocationId))")

    for db_file in os.listdir(pasta_db):
        if db_file.endswith(".db"):
            caminho_db = os.path.join(pasta_db, db_file)
            print(f"Conectando ao arquivo: {db_file}")

            # Conectar ao arquivo de banco de dados atual
            conn = sqlite3.connect(caminho_db)
            cursor = conn.cursor()

            # Ler os registros da tabela "PlaylistItemLocationMap" no banco de dados atual
            cursor.execute("SELECT * FROM PlaylistItemLocationMap")
            records = cursor.fetchall()

            # Verificar se o registro já existe no banco de dados mesclado com base nos valores das colunas "PlaylistItemId" e "LocationId"
            for record in records:
                playlist_item_id = record[0]
                location_id = record[1]
                major_multimedia_type = record[2]
                base_duration_ticks = record[3]

                try:
                    cursor_mesclado.execute("INSERT INTO PlaylistItemLocationMap (PlaylistItemId, LocationId, MajorMultimediaType, BaseDurationTicks) VALUES (?, ?, ?, ?)", (playlist_item_id, location_id, major_multimedia_type, base_duration_ticks))

                    # merge_record_by_location_id(caminho_db, caminho_db_mesclado, location_id)

                    conn_mesclado.commit()
                except sqlite3.IntegrityError:
                    print(f"Registro com PlaylistItemId {playlist_item_id} e LocationId {location_id} já existe no banco de dados mesclado. Gerando novos números aleatórios...")
                    new_playlist_item_id = f"{playlist_item_id}{random_id()}"
                    new_location_id = f"{location_id}{random_id()}"
                    print(f"Novos valores para PlaylistItemId: {new_playlist_item_id} e LocationId: {new_location_id}")
                    update_database(caminho_db, "PlaylistItemId", playlist_item_id, new_playlist_item_id)
                    update_database(caminho_db, "LocationId", location_id, new_location_id)
                    cursor_mesclado.execute("INSERT INTO PlaylistItemLocationMap (PlaylistItemId, LocationId, MajorMultimediaType, BaseDurationTicks) VALUES (?, ?, ?, ?)", (new_playlist_item_id, new_location_id, major_multimedia_type, base_duration_ticks))

                    # merge_record_by_location_id(pasta_db, pasta_mesclada, new_location_id)
                    
                    conn_mesclado.commit()

            cursor.close()
            conn.close()

    cursor_mesclado.close()
    conn_mesclado.close()
    print(">>>>>>Tabela 'PlaylistItemLocationMap' mesclada com sucesso!")
