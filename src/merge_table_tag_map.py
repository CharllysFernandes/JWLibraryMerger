import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_tag_map(pasta_db, pasta_mesclada):
    """
    Mescla a tabela "TagMap" de todos os bancos de dados encontrados na pasta DB
    e une ao arquivo "userData.db" na pasta file_merged.

    ... (documentação do código) ...

    Retorna:
        Nada. A função apenas mescla os registros da tabela "TagMap" em todos os bancos de dados.

    Exemplo de uso:
        merge_table_tag_map("caminho_para_pasta_DB", "caminho_para_pasta_file_merged")
    """
    # Conectar ao "userData.db" na pasta mesclada
    caminho_db_mesclado = os.path.join(pasta_mesclada, "userData.db")
    conn_mesclado = sqlite3.connect(caminho_db_mesclado)
    cursor_mesclado = conn_mesclado.cursor()

    # Criar a tabela "TagMap" no banco de dados mesclado, caso ainda não exista
    cursor_mesclado.execute("CREATE TABLE IF NOT EXISTS TagMap (TagMapId INTEGER PRIMARY KEY, PlaylistItemId INTEGER, LocationId INTEGER, NoteId INTEGER, TagId INTEGER, Position INTEGER)")

    for db_file in os.listdir(pasta_db):
        if db_file.endswith(".db"):
            caminho_db = os.path.join(pasta_db, db_file)
            print(f"Conectando ao arquivo: {db_file} para mesclar o TagMapId")

            # Conectar ao arquivo de banco de dados atual
            conn = sqlite3.connect(caminho_db)
            cursor = conn.cursor()

            # Ler os registros da tabela "TagMap" no banco de dados atual
            cursor.execute("SELECT * FROM TagMap")
            records = cursor.fetchall()

            # Verificar se o registro já existe no banco de dados mesclado com base no valor da coluna "TagMapId"
            for record in records:
                tag_map_id = record[0]

                # Verificar se o registro já existe no banco de dados mesclado
                cursor_mesclado.execute("SELECT TagMapId FROM TagMap WHERE TagMapId = ?", (tag_map_id,))
                existing_record = cursor_mesclado.fetchone()

                if existing_record:
                    # Registro já existe no banco de dados mesclado, gerar novo número aleatório para o TagMapId
                    new_tag_map_id = f"{tag_map_id}{random_id()}"
                    print(f"Registro com TagMapId {tag_map_id} já existe no banco de dados mesclado. Gerando novo número aleatório...")
                    print(f"Novo valor para TagMapId: {new_tag_map_id}")

                    # Atualizar o banco de dados atual com o novo valor para TagMapId
                    update_database(caminho_db, "TagMapId", tag_map_id, new_tag_map_id)

                    # Inserir o registro no banco de dados mesclado com o novo valor para TagMapId
                    cursor_mesclado.execute("INSERT INTO TagMap (TagMapId, PlaylistItemId, LocationId, NoteId, TagId, Position) VALUES (?, ?, ?, ?, ?, ?)", (new_tag_map_id, record[1], record[2], record[3], record[4], record[5]))
                else:
                    # Registro não existe no banco de dados mesclado, inserir diretamente
                    cursor_mesclado.execute("INSERT INTO TagMap (TagMapId, PlaylistItemId, LocationId, NoteId, TagId, Position) VALUES (?, ?, ?, ?, ?, ?)", (tag_map_id, record[1], record[2], record[3], record[4], record[5]))

                # Commit a transação após cada inserção
                conn_mesclado.commit()

            cursor.close()
            conn.close()

    cursor_mesclado.close()
    conn_mesclado.close()
    print(">>>>> Tabela 'TagMap' mesclada com sucesso!")

# Exemplo de uso:
# merge_table_tag_map("caminho_para_pasta_DB", "caminho_para_pasta_file_merged")
