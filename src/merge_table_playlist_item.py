import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_playlist_item(pasta_db, pasta_mesclada):
    """
    Mescla a tabela "PlaylistItem" de todos os bancos de dados encontrados na pasta DB
    e une ao arquivo "userData.db" na pasta file_merged.

    Para cada arquivo de banco de dados encontrado na pasta DB, esta função realiza o seguinte:
    - Conecta-se ao "userData.db" na pasta mesclada.
    - Cria a tabela "PlaylistItem" no banco de dados mesclado, caso ela ainda não exista.
    - Lê os registros da tabela "PlaylistItem" no banco de dados atual.
    - Verifica se cada registro já existe no banco de dados mesclado com base no valor da coluna "PlaylistItemId".
    - Caso o registro não exista no banco de dados mesclado, insere-o diretamente.
    - Se o registro já existir no banco de dados mesclado, gera um novo número aleatório para o "PlaylistItemId".
      O novo "PlaylistItemId" é concatenado ao valor original para evitar duplicações.
      O registro é então atualizado com o novo "PlaylistItemId" no banco de dados atual e inserido no banco de dados mesclado.

    Parâmetros:
        pasta_db (str): Caminho para a pasta que contém os arquivos de banco de dados a serem mesclados.
        pasta_mesclada (str): Caminho para a pasta onde o arquivo "userData.db" está localizado.

    Retorna:
        Nada. A função apenas mescla os registros da tabela "PlaylistItem" em todos os bancos de dados.

    Exemplo de uso:
        merge_table_playlist_item("caminho_para_pasta_DB", "caminho_para_pasta_file_merged")
    """
    # Conectar ao "userData.db" na pasta mesclada
    caminho_db_mesclado = os.path.join(pasta_mesclada, "userData.db")
    conn_mesclado = sqlite3.connect(caminho_db_mesclado)
    cursor_mesclado = conn_mesclado.cursor()

    # Criar a tabela "PlaylistItem" no banco de dados mesclado, caso ainda não exista
    cursor_mesclado.execute("CREATE TABLE IF NOT EXISTS PlaylistItem (PlaylistItemId INTEGER PRIMARY KEY, Label TEXT, StartTrimOffsetTicks INTEGER, EndTrimOffSetTicks INTEGER, Accuracy INTEGER, EndAction INTEGER, ThumbnailFilePath TEXT)")

    for db_file in os.listdir(pasta_db):
        if db_file.endswith(".db"):
            caminho_db = os.path.join(pasta_db, db_file)
            print(f"Conectando ao arquivo: {db_file}")

            # Conectar ao arquivo de banco de dados atual
            conn = sqlite3.connect(caminho_db)
            cursor = conn.cursor()

            # Ler os registros da tabela "PlaylistItem" no banco de dados atual
            cursor.execute("SELECT * FROM PlaylistItem")
            records = cursor.fetchall()

            # Verificar se o registro já existe no banco de dados mesclado com base no valor da coluna "PlaylistItemId"
            for record in records:
                playlist_item_id = record[0]
                label = record[1]
                start_trim_offset_ticks = record[2]
                end_trim_offset_ticks = record[3]
                accuracy = record[4]
                end_action = record[5]
                thumbnail_file_path = record[6]

                try:
                    cursor_mesclado.execute("INSERT INTO PlaylistItem (PlaylistItemId, Label, StartTrimOffsetTicks, EndTrimOffSetTicks, Accuracy, EndAction, ThumbnailFilePath) VALUES (?, ?, ?, ?, ?, ?, ?)", (playlist_item_id, label, start_trim_offset_ticks, end_trim_offset_ticks, accuracy, end_action, thumbnail_file_path))
                    conn_mesclado.commit()
                except sqlite3.IntegrityError:
                    print(f"Registro com PlaylistItemId {playlist_item_id} já existe no banco de dados mesclado. Gerando novo número aleatório...")
                    new_playlist_item_id = f"{playlist_item_id}{random_id()}"
                    print(f"Novo valor para PlaylistItemId: {new_playlist_item_id}")
                    update_database(caminho_db, "PlaylistItemId", playlist_item_id, new_playlist_item_id)
                    cursor_mesclado.execute("INSERT INTO PlaylistItem (PlaylistItemId, Label, StartTrimOffsetTicks, EndTrimOffSetTicks, Accuracy, EndAction, ThumbnailFilePath) VALUES (?, ?, ?, ?, ?, ?, ?)", (new_playlist_item_id, label, start_trim_offset_ticks, end_trim_offset_ticks, accuracy, end_action, thumbnail_file_path))
                    conn_mesclado.commit()

            cursor.close()
            conn.close()

    cursor_mesclado.close()
    conn_mesclado.close()
    print(">>>>>Tabela 'PlaylistItem' mesclada com sucesso!")

