import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_tag_map(file_path_databases, file_path_databases_merged):
    """
    Mescla a tabela "TagMap" de todos os bancos de dados encontrados na pasta DB
    e une ao arquivo "userData.db" na pasta file_merged.

    Para cada arquivo de banco de dados encontrado na pasta DB, esta função realiza o seguinte:
    - Conecta-se ao "userData.db" na pasta mesclada.
    - Cria a tabela "TagMap" no banco de dados mesclado, caso ela ainda não exista.
    - Lê os registros da tabela "TagMap" no banco de dados atual.
    - Verifica se cada registro já existe no banco de dados mesclado com base no valor da coluna "TagMapId".
    - Se o registro já existir no banco de dados mesclado, gera um novo número aleatório para o "TagMapId".
      O novo "TagMapId" é concatenado ao valor original para evitar duplicações.
      O registro é então atualizado com o novo "TagMapId" no banco de dados atual e inserido no banco de dados mesclado.
    - Se o registro não existir no banco de dados mesclado, o insere diretamente.

    Parâmetros:
        file_path_databases (str): Caminho para a pasta que contém os arquivos de banco de dados a serem mesclados.
        pasta_mesclada (str): Caminho para a pasta onde o arquivo "userData.db" está localizado.

    Retorna:
        Nada. A função apenas mescla os registros da tabela "TagMap" em todos os bancos de dados.

    Exemplo de uso:
        merge_table_tag_map("caminho_para_pasta_DB", "caminho_para_pasta_file_merged")
    """
    # Conectar ao "userData.db" na pasta mesclada
    file_path_database_merged = os.path.join(file_path_databases_merged, "userData.db")
    conn_merged = sqlite3.connect(file_path_database_merged)
    cursor_merged = conn_merged.cursor()

    # Criar a tabela "TagMap" no banco de dados mesclado, caso ainda não exista
    cursor_merged.execute("CREATE TABLE IF NOT EXISTS TagMap (TagMapId INTEGER PRIMARY KEY, PlaylistItemId INTEGER, LocationId INTEGER, NoteId INTEGER, TagId INTEGER, Position INTEGER)")

    for db_file in os.listdir(file_path_databases):
        if db_file.endswith(".db"):
            file_path_database = os.path.join(file_path_databases, db_file)
            print(f"Conectando ao arquivo: {db_file} para mesclar o TagMapId")

            # Conectar ao arquivo de banco de dados atual
            conn = sqlite3.connect(file_path_database)
            cursor = conn.cursor()

            # Ler os registros da tabela "TagMap" no banco de dados atual
            cursor.execute("SELECT * FROM TagMap")
            records = cursor.fetchall()

            # Verificar se o registro já existe no banco de dados mesclado com base no valor da coluna "TagMapId"
            for record in records:
                tag_map_id = record[0]

                # Verificar se o registro já existe no banco de dados mesclado
                cursor_merged.execute("SELECT TagMapId FROM TagMap WHERE TagMapId = ?", (tag_map_id,))
                existing_record = cursor_merged.fetchone()

                if existing_record:
                    # Registro já existe no banco de dados mesclado, gerar novo número aleatório para o TagMapId
                    new_tag_map_id = f"{tag_map_id}{random_id()}"
                    print(f"Registro com TagMapId {tag_map_id} já existe no banco de dados mesclado. Gerando novo número aleatório...")
                    print(f"Novo valor para TagMapId: {new_tag_map_id}")

                    # Atualizar o banco de dados atual com o novo valor para TagMapId
                    update_database(file_path_database, "TagMapId", tag_map_id, new_tag_map_id)

                    # Inserir o registro no banco de dados mesclado com o novo valor para TagMapId
                    cursor_merged.execute("INSERT INTO TagMap (TagMapId, PlaylistItemId, LocationId, NoteId, TagId, Position) VALUES (?, ?, ?, ?, ?, ?)", (new_tag_map_id, record[1], record[2], record[3], record[4], record[5]))
                else:
                    # Registro não existe no banco de dados mesclado, inserir diretamente
                    cursor_merged.execute("INSERT INTO TagMap (TagMapId, PlaylistItemId, LocationId, NoteId, TagId, Position) VALUES (?, ?, ?, ?, ?, ?)", (tag_map_id, record[1], record[2], record[3], record[4], record[5]))

                # Commit a transação após cada inserção
                conn_merged.commit()

            cursor.close()
            conn.close()
            print(f"Tabela 'TagMap' mesclada com sucesso em {file_path_database}!")

    cursor_merged.close()
    conn_merged.close()

