import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_tag(pasta_db, pasta_mesclada):
    """
    Mescla a tabela "Tag" de todos os bancos de dados encontrados na pasta DB
    e une ao arquivo "userData.db" na pasta file_merged.

    Para cada arquivo de banco de dados encontrado na pasta DB, esta função realiza o seguinte:
    - Conecta-se ao "userData.db" na pasta mesclada.
    - Cria a tabela "Tag" no banco de dados mesclado, caso ela ainda não exista.
    - Lê os registros da tabela "Tag" no banco de dados atual.
    - Verifica se cada registro já existe no banco de dados mesclado com base no valor da coluna "TagId".
    - Caso o registro não exista no banco de dados mesclado, insere-o diretamente.
    - Se o registro já existir no banco de dados mesclado, gera um novo número aleatório para o "TagId".
      O novo "TagId" é concatenado ao valor original para evitar duplicações.
      O registro é então atualizado com o novo "TagId" no banco de dados atual e inserido no banco de dados mesclado.

    Parâmetros:
        pasta_db (str): Caminho para a pasta que contém os arquivos de banco de dados a serem mesclados.
        pasta_mesclada (str): Caminho para a pasta onde o arquivo "userData.db" está localizado.

    Retorna:
        Nada. A função apenas mescla os registros da tabela "Tag" em todos os bancos de dados.

    Exemplo de uso:
        merge_table_tag("caminho_para_pasta_DB", "caminho_para_pasta_file_merged")
    """
    # Conectar ao "userData.db" na pasta mesclada
    caminho_db_mesclado = os.path.join(pasta_mesclada, "userData.db")
    conn_mesclado = sqlite3.connect(caminho_db_mesclado)
    cursor_mesclado = conn_mesclado.cursor()

    # Criar a tabela "Tag" no banco de dados mesclado, caso ainda não exista
    cursor_mesclado.execute("CREATE TABLE IF NOT EXISTS Tag (TagId INTEGER PRIMARY KEY, Type INTEGER, Name TEXT)")

    for db_file in os.listdir(pasta_db):
        if db_file.endswith(".db"):
            caminho_db = os.path.join(pasta_db, db_file)
            print(f"Conectando ao arquivo: {db_file}")

            print(f"Checking database file: {caminho_db}")
            if os.path.exists(caminho_db):
                print("Database file exists.")
            else:
                print("Database file does not exist.")
            # Conectar ao arquivo de banco de dados atual
            conn = sqlite3.connect(caminho_db)
            cursor = conn.cursor()

            # Ler os registros da tabela "Tag" no banco de dados atual
            cursor.execute("SELECT * FROM Tag")
            records = cursor.fetchall()

            # Verificar se o registro já existe no banco de dados mesclado com base no valor da coluna "TagId"
            for record in records:
                tag_id = record[0]
                type_value = record[1]
                name_value = record[2]

                try:
                    cursor_mesclado.execute("INSERT INTO Tag (TagId, Type, Name) VALUES (?, ?, ?)", (tag_id, type_value, name_value))
                    conn_mesclado.commit()
                except sqlite3.IntegrityError:
                    print(f"Registro com TagId {tag_id} já existe no banco de dados mesclado. Gerando novo número aleatório...")
                    new_tag_id = f"{tag_id}{random_id()}"
                    print(f"Novo valor para TagId: {new_tag_id}")
                    update_database(caminho_db, "TagId", tag_id, new_tag_id)
                    cursor_mesclado.execute("INSERT INTO Tag (TagId, Type, Name) VALUES (?, ?, ?)", (new_tag_id, type_value, name_value))
                    conn_mesclado.commit()

            cursor.close()
            conn.close()

    cursor_mesclado.close()
    conn_mesclado.close()
    print(">>>>>>Tabela 'Tag' mesclada com sucesso!")

