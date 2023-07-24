import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_playlist_item_independent_media_map(pasta_db, pasta_mesclada):
    """
    Mescla a tabela "PlaylistItemIndependentMediaMap" de todos os bancos de dados encontrados na pasta DB
    e une ao arquivo "userData.db" na pasta file_merged.

    Para cada arquivo de banco de dados encontrado na pasta DB, esta função realiza o seguinte:
    - Conecta-se ao "userData.db" na pasta mesclada.
    - Cria a tabela "PlaylistItemIndependentMediaMap" no banco de dados mesclado, caso ela ainda não exista.
    - Lê os registros da tabela "PlaylistItemIndependentMediaMap" no banco de dados atual.
    - Verifica se cada registro já existe no banco de dados mesclado com base nos valores das colunas "PlaylistItemId" e "IndependentMediaId".
    - Caso o registro não exista no banco de dados mesclado, insere-o diretamente.
    - Se o registro já existir no banco de dados mesclado, gera novos números aleatórios para "PlaylistItemId" e "IndependentMediaId".
      Os novos valores são concatenados aos valores originais para evitar duplicações.
      O registro é então atualizado com os novos valores no banco de dados atual e inserido no banco de dados mesclado.

    Parâmetros:
        pasta_db (str): Caminho para a pasta que contém os arquivos de banco de dados a serem mesclados.
        pasta_mesclada (str): Caminho para a pasta onde o arquivo "userData.db" está localizado.

    Retorna:
        Nada. A função apenas mescla os registros da tabela "PlaylistItemIndependentMediaMap" em todos os bancos de dados.

    Exemplo de uso:
        merge_table_playlist_item_independent_media_map("caminho_para_pasta_DB", "caminho_para_pasta_file_merged")
    """
    # Conectar ao "userData.db" na pasta mesclada
    caminho_db_mesclado = os.path.join(pasta_mesclada, "userData.db")
    conn_mesclado = sqlite3.connect(caminho_db_mesclado)
    cursor_mesclado = conn_mesclado.cursor()

    # Criar a tabela "PlaylistItemIndependentMediaMap" no banco de dados mesclado, caso ainda não exista
    cursor_mesclado.execute("CREATE TABLE IF NOT EXISTS PlaylistItemIndependentMediaMap (PlaylistItemId INTEGER, IndependentMediaId INTEGER, DurationTicks INTEGER, PRIMARY KEY (PlaylistItemId, IndependentMediaId))")

    for db_file in os.listdir(pasta_db):
        if db_file.endswith(".db"):
            caminho_db = os.path.join(pasta_db, db_file)
            print(f"Conectando ao arquivo: {db_file}")

            # Conectar ao arquivo de banco de dados atual
            conn = sqlite3.connect(caminho_db)
            cursor = conn.cursor()

            # Ler os registros da tabela "PlaylistItemIndependentMediaMap" no banco de dados atual
            cursor.execute("SELECT * FROM PlaylistItemIndependentMediaMap")
            records = cursor.fetchall()

            # Verificar se o registro já existe no banco de dados mesclado com base nos valores das colunas "PlaylistItemId" e "IndependentMediaId"
            for record in records:
                playlist_item_id = record[0]
                independent_media_id = record[1]
                duration_ticks = record[2]

                try:
                    cursor_mesclado.execute("INSERT INTO PlaylistItemIndependentMediaMap (PlaylistItemId, IndependentMediaId, DurationTicks) VALUES (?, ?, ?)", (playlist_item_id, independent_media_id, duration_ticks))
                    conn_mesclado.commit()
                except sqlite3.IntegrityError:
                    print(f"Registro com PlaylistItemId {playlist_item_id} e IndependentMediaId {independent_media_id} já existe no banco de dados mesclado. Gerando novos números aleatórios...")
                    new_playlist_item_id = f"{playlist_item_id}{random_id()}"
                    new_independent_media_id = f"{independent_media_id}{random_id()}"
                    print(f"Novos valores para PlaylistItemId: {new_playlist_item_id} e IndependentMediaId: {new_independent_media_id}")
                    update_database(caminho_db, "PlaylistItemId", playlist_item_id, new_playlist_item_id)
                    update_database(caminho_db, "IndependentMediaId", independent_media_id, new_independent_media_id)
                    cursor_mesclado.execute("INSERT INTO PlaylistItemIndependentMediaMap (PlaylistItemId, IndependentMediaId, DurationTicks) VALUES (?, ?, ?)", (new_playlist_item_id, new_independent_media_id, duration_ticks))
                    conn_mesclado.commit()

            cursor.close()
            conn.close()

    cursor_mesclado.close()
    conn_mesclado.close()
    print(">>>>>>Tabela 'PlaylistItemIndependentMediaMap' mesclada com sucesso!")
