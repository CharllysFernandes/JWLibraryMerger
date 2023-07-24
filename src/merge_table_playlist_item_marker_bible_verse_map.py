import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_playlist_item_marker_bible_verse_map(pasta_db, pasta_mesclada):
    """
    Mescla a tabela "PlaylistItemMarkerBibleVerseMap" de todos os bancos de dados encontrados na pasta DB
    e une ao arquivo "userData.db" na pasta file_merged.

    Para cada arquivo de banco de dados encontrado na pasta DB, esta função realiza o seguinte:
    - Conecta-se ao "userData.db" na pasta mesclada.
    - Cria a tabela "PlaylistItemMarkerBibleVerseMap" no banco de dados mesclado, caso ela ainda não exista.
    - Lê os registros da tabela "PlaylistItemMarkerBibleVerseMap" no banco de dados atual.
    - Verifica se cada registro já existe no banco de dados mesclado com base nos valores das colunas "PlaylistItemMarkerId" e "VerseId".
    - Caso o registro não exista no banco de dados mesclado, insere-o diretamente.
    - Se o registro já existir no banco de dados mesclado, gera novos números aleatórios para "PlaylistItemMarkerId" e "VerseId".
      Os novos valores são concatenados aos valores originais para evitar duplicações.
      O registro é então atualizado com os novos valores no banco de dados atual e inserido no banco de dados mesclado.

    Parâmetros:
        pasta_db (str): Caminho para a pasta que contém os arquivos de banco de dados a serem mesclados.
        pasta_mesclada (str): Caminho para a pasta onde o arquivo "userData.db" está localizado.

    Retorna:
        Nada. A função apenas mescla os registros da tabela "PlaylistItemMarkerBibleVerseMap" em todos os bancos de dados.

    Exemplo de uso:
        merge_table_playlist_item_marker_bible_verse_map("caminho_para_pasta_DB", "caminho_para_pasta_file_merged")
    """
    # Conectar ao "userData.db" na pasta mesclada
    caminho_db_mesclado = os.path.join(pasta_mesclada, "userData.db")
    conn_mesclado = sqlite3.connect(caminho_db_mesclado)
    cursor_mesclado = conn_mesclado.cursor()

    # Criar a tabela "PlaylistItemMarkerBibleVerseMap" no banco de dados mesclado, caso ainda não exista
    cursor_mesclado.execute("CREATE TABLE IF NOT EXISTS PlaylistItemMarkerBibleVerseMap (PlaylistItemMarkerId INTEGER, VerseId INTEGER, PRIMARY KEY (PlaylistItemMarkerId, VerseId))")

    for db_file in os.listdir(pasta_db):
        if db_file.endswith(".db"):
            caminho_db = os.path.join(pasta_db, db_file)
            print(f"Conectando ao arquivo: {db_file}")

            # Conectar ao arquivo de banco de dados atual
            conn = sqlite3.connect(caminho_db)
            cursor = conn.cursor()

            # Ler os registros da tabela "PlaylistItemMarkerBibleVerseMap" no banco de dados atual
            cursor.execute("SELECT * FROM PlaylistItemMarkerBibleVerseMap")
            records = cursor.fetchall()

            # Verificar se o registro já existe no banco de dados mesclado com base nos valores das colunas "PlaylistItemMarkerId" e "VerseId"
            for record in records:
                playlist_item_marker_id = record[0]
                verse_id = record[1]

                try:
                    cursor_mesclado.execute("INSERT INTO PlaylistItemMarkerBibleVerseMap (PlaylistItemMarkerId, VerseId) VALUES (?, ?)", (playlist_item_marker_id, verse_id))
                    conn_mesclado.commit()
                except sqlite3.IntegrityError:
                    print(f"Registro com PlaylistItemMarkerId {playlist_item_marker_id} e VerseId {verse_id} já existe no banco de dados mesclado. Gerando novos números aleatórios...")
                    new_playlist_item_marker_id = f"{playlist_item_marker_id}{random_id()}"
                    new_verse_id = f"{verse_id}{random_id()}"
                    print(f"Novos valores para PlaylistItemMarkerId: {new_playlist_item_marker_id} e VerseId: {new_verse_id}")
                    update_database(caminho_db, "PlaylistItemMarkerId", playlist_item_marker_id, new_playlist_item_marker_id)
                    update_database(caminho_db, "VerseId", verse_id, new_verse_id)
                    cursor_mesclado.execute("INSERT INTO PlaylistItemMarkerBibleVerseMap (PlaylistItemMarkerId, VerseId) VALUES (?, ?)", (new_playlist_item_marker_id, new_verse_id))
                    conn_mesclado.commit()

            cursor.close()
            conn.close()

    cursor_mesclado.close()
    conn_mesclado.close()
    print(">>>>Tabela 'PlaylistItemMarkerBibleVerseMap' mesclada com sucesso!")
