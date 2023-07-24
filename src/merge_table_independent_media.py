import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def merge_table_independent_media(pasta_db, pasta_mesclada):
    """
    Mescla a tabela "IndependentMedia" de todos os bancos de dados encontrados na pasta DB
    e une ao arquivo "userData.db" na pasta file_merged.

    Para cada arquivo de banco de dados encontrado na pasta DB, esta função realiza o seguinte:
    - Conecta-se ao "userData.db" na pasta mesclada.
    - Cria a tabela "IndependentMedia" no banco de dados mesclado, caso ela ainda não exista.
    - Lê os registros da tabela "IndependentMedia" no banco de dados atual.
    - Verifica se cada registro já existe no banco de dados mesclado com base no valor da coluna "IndependentMediaId".
    - Caso o registro não exista no banco de dados mesclado, insere-o diretamente.
    - Se o registro já existir no banco de dados mesclado, gera um novo número aleatório para o "IndependentMediaId".
      O novo "IndependentMediaId" é concatenado ao valor original para evitar duplicações.
      O registro é então atualizado com o novo "IndependentMediaId" no banco de dados atual e inserido no banco de dados mesclado.

    Parâmetros:
        pasta_db (str): Caminho para a pasta que contém os arquivos de banco de dados a serem mesclados.
        pasta_mesclada (str): Caminho para a pasta onde o arquivo "userData.db" está localizado.

    Retorna:
        Nada. A função apenas mescla os registros da tabela "IndependentMedia" em todos os bancos de dados.

    Exemplo de uso:
        merge_table_independent_media("caminho_para_pasta_DB", "caminho_para_pasta_file_merged")
    """
    # Conectar ao "userData.db" na pasta mesclada
    caminho_db_mesclado = os.path.join(pasta_mesclada, "userData.db")
    conn_mesclado = sqlite3.connect(caminho_db_mesclado)
    cursor_mesclado = conn_mesclado.cursor()

    # Criar a tabela "IndependentMedia" no banco de dados mesclado, caso ainda não exista
    cursor_mesclado.execute("CREATE TABLE IF NOT EXISTS IndependentMedia (IndependentMediaId INTEGER PRIMARY KEY, OriginalFilename TEXT, FilePath TEXT, MimeType TEXT, Hash TEXT)")

    for db_file in os.listdir(pasta_db):
        if db_file.endswith(".db"):
            caminho_db = os.path.join(pasta_db, db_file)
            print(f"Conectando ao arquivo: {db_file}")

            # Conectar ao arquivo de banco de dados atual
            conn = sqlite3.connect(caminho_db)
            cursor = conn.cursor()

            # Ler os registros da tabela "IndependentMedia" no banco de dados atual
            cursor.execute("SELECT * FROM IndependentMedia")
            records = cursor.fetchall()

            # Verificar se o registro já existe no banco de dados mesclado com base no valor da coluna "IndependentMediaId"
            for record in records:
                independent_media_id = record[0]
                original_filename = record[1]
                file_path = record[2]
                mime_type = record[3]
                hash_value = record[4]

                try:
                    cursor_mesclado.execute("INSERT INTO IndependentMedia (IndependentMediaId, OriginalFilename, FilePath, MimeType, Hash) VALUES (?, ?, ?, ?, ?)", (independent_media_id, original_filename, file_path, mime_type, hash_value))
                    conn_mesclado.commit()
                except sqlite3.IntegrityError:
                    print(f"Registro com IndependentMediaId {independent_media_id} já existe no banco de dados mesclado. Gerando novo número aleatório...")
                    new_independent_media_id = f"{independent_media_id}{random_id()}"
                    print(f"Novo valor para IndependentMediaId: {new_independent_media_id}")
                    update_database(caminho_db, "IndependentMediaId", independent_media_id, new_independent_media_id)
                    cursor_mesclado.execute("INSERT INTO IndependentMedia (IndependentMediaId, OriginalFilename, FilePath, MimeType, Hash) VALUES (?, ?, ?, ?, ?)", (new_independent_media_id, original_filename, file_path, mime_type, hash_value))
                    conn_mesclado.commit()

            cursor.close()
            conn.close()

    cursor_mesclado.close()
    conn_mesclado.close()
    print("_________________________________________________")
    print(">>>>Tabela 'IndependentMedia' mesclada com sucesso!")

