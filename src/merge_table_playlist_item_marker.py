import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_playlist_item_marker(pasta_db, pasta_mesclada):
    """
    Mescla a tabela "PlaylistItemMarker" de todos os bancos de dados encontrados na pasta DB
    e une ao arquivo "userData.db" na pasta file_merged.

    Para cada arquivo de banco de dados encontrado na pasta DB, esta função realiza o seguinte:
    - Conecta-se ao "userData.db" na pasta mesclada.
    - Cria a tabela "PlaylistItemMarker" no banco de dados mesclado, caso ela ainda não exista.
    - Lê os registros da tabela "PlaylistItemMarker" no banco de dados atual.
    - Verifica se cada registro já existe no banco de dados mesclado com base no valor da coluna "PlaylistItemMarkerId".
    - Caso o registro não exista no banco de dados mesclado, insere-o diretamente.
    - Se o registro já existir no banco de dados mesclado, gera um novo número aleatório para o "PlaylistItemMarkerId".
      O novo "PlaylistItemMarkerId" é concatenado ao valor original para evitar duplicações.
      O registro é então atualizado com o novo "PlaylistItemMarkerId" no banco de dados atual e inserido no banco de dados mesclado.

    Parâmetros:
        pasta_db (str): Caminho para a pasta que contém os arquivos de banco de dados a serem mesclados.
        pasta_mesclada (str): Caminho para a pasta onde o arquivo "userData.db" está localizado.

    Retorna:
        Nada. A função apenas mescla os registros da tabela "PlaylistItemMarker" em todos os bancos de dados.

    Exemplo de uso:
        merge_table_playlist_item_marker("caminho_para_pasta_DB", "caminho_para_pasta_file_merged")
    """
    # Conectar ao "userData.db" na pasta mesclada
    caminho_db_mesclado = os.path.join(pasta_mesclada, "userData.db")
    conn_mesclado = sqlite3.connect(caminho_db_mesclado)
    cursor_mesclado = conn_mesclado.cursor()

    # Criar a tabela "PlaylistItemMarker" no banco de dados mesclado, caso ainda não exista
    cursor_mesclado.execute("CREATE TABLE IF NOT EXISTS PlaylistItemMarker (PlaylistItemMarkerId INTEGER PRIMARY KEY, PlaylistItemId INTEGER, Label TEXT, StartTimeTicks INTEGER, DurationTicks INTEGER, EndTransitionDurationTicks INTEGER)")

    for db_file in os.listdir(pasta_db):
        if db_file.endswith(".db"):
            caminho_db = os.path.join(pasta_db, db_file)
            print(f"Conectando ao arquivo: {db_file}")

            # Conectar ao arquivo de banco de dados atual
            conn = sqlite3.connect(caminho_db)
            cursor = conn.cursor()

            # Ler os registros da tabela "PlaylistItemMarker" no banco de dados atual
            cursor.execute("SELECT * FROM PlaylistItemMarker")
            records = cursor.fetchall()

            # Verificar se o registro já existe no banco de dados mesclado com base no valor da coluna "PlaylistItemMarkerId"
            for record in records:
                playlist_item_marker_id = record[0]
                playlist_item_id = record[1]
                label = record[2]
                start_time_ticks = record[3]
                duration_ticks = record[4]
                end_transition_duration_ticks = record[5]

                try:
                    cursor_mesclado.execute("INSERT INTO PlaylistItemMarker (PlaylistItemMarkerId, PlaylistItemId, Label, StartTimeTicks, DurationTicks, EndTransitionDurationTicks) VALUES (?, ?, ?, ?, ?, ?)", (playlist_item_marker_id, playlist_item_id, label, start_time_ticks, duration_ticks, end_transition_duration_ticks))
                    conn_mesclado.commit()
                except sqlite3.IntegrityError:
                    print(f"Registro com PlaylistItemMarkerId {playlist_item_marker_id} já existe no banco de dados mesclado. Gerando novo número aleatório...")
                    new_playlist_item_marker_id = f"{playlist_item_marker_id}{random_id()}"
                    print(f"Novo valor para PlaylistItemMarkerId: {new_playlist_item_marker_id}")
                    update_database(caminho_db, "PlaylistItemMarkerId", playlist_item_marker_id, new_playlist_item_marker_id)
                    cursor_mesclado.execute("INSERT INTO PlaylistItemMarker (PlaylistItemMarkerId, PlaylistItemId, Label, StartTimeTicks, DurationTicks, EndTransitionDurationTicks) VALUES (?, ?, ?, ?, ?, ?)", (new_playlist_item_marker_id, playlist_item_id, label, start_time_ticks, duration_ticks, end_transition_duration_ticks))
                    conn_mesclado.commit()

            cursor.close()
            conn.close()

    cursor_mesclado.close()
    conn_mesclado.close()
    print(">>>>>>Tabela 'PlaylistItemMarker' mesclada com sucesso!")
