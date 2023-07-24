import sqlite3
import os
from .update_databases import update_database
from .utils import random_id

def get_unique_location_ids(database_path):
    # Conectar ao banco de dados
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Buscar os valores únicos de LocationId na tabela PlaylistItemLocationMap
    cursor.execute("SELECT DISTINCT LocationId FROM PlaylistItemLocationMap")
    location_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return location_ids

def merge_table_location(pasta_db, pasta_mesclada):
    """
    Mescla a tabela "Location" de todos os bancos de dados encontrados na pasta DB
    e une ao arquivo "userData.db" na pasta file_merged.

    Para cada arquivo de banco de dados encontrado na pasta DB, esta função realiza o seguinte:
    - Conecta-se ao "userData.db" na pasta mesclada.
    - Cria a tabela "Location" no banco de dados mesclado, caso ela ainda não exista.
    - Lê os registros da tabela "Location" no banco de dados atual.
    - Verifica se cada registro já existe no banco de dados mesclado com base no valor da coluna "LocationId".
    - Caso o registro não exista no banco de dados mesclado, insere-o diretamente.
    - Se o registro já existir no banco de dados mesclado, gera um novo número aleatório para "LocationId".
      O novo "LocationId" é concatenado ao valor original para evitar duplicações.
      O registro é então atualizado com o novo "LocationId" no banco de dados atual e inserido no banco de dados mesclado.

    Parâmetros:
        pasta_db (str): Caminho para a pasta que contém os arquivos de banco de dados a serem mesclados.
        pasta_mesclada (str): Caminho para a pasta onde o arquivo "userData.db" está localizado.

    Retorna:
        Nada. A função apenas mescla os registros da tabela "Location" em todos os bancos de dados.

    Exemplo de uso:
        merge_table_location("caminho_para_pasta_DB", "caminho_para_pasta_file_merged")
    """
    # Conectar ao "userData.db" na pasta mesclada
    caminho_db_mesclado = os.path.join(pasta_mesclada, "userData.db")
    conn_mesclado = sqlite3.connect(caminho_db_mesclado)
    cursor_mesclado = conn_mesclado.cursor()

    for db_file in os.listdir(pasta_db):
        if db_file.endswith(".db"):
            caminho_db = os.path.join(pasta_db, db_file)
            print(f"Conectando ao arquivo: {db_file}")

            # Conectar ao arquivo de banco de dados atual
            conn = sqlite3.connect(caminho_db)
            cursor = conn.cursor()

            # Obter os valores únicos de LocationId da tabela PlaylistItemLocationMap no banco de dados atual
            location_ids = get_unique_location_ids(caminho_db)

            # Verificar cada LocationId na tabela Location do banco de dados mesclado
            for location_id in location_ids:
                # Ler o registro com o LocationId atual no banco de dados atual
                cursor.execute("SELECT * FROM Location WHERE LocationId=?", (location_id,))
                record = cursor.fetchone()

                if record:
                    # Mesclar o registro para o banco de dados mesclado
                    try:
                        cursor_mesclado.execute("INSERT INTO Location (LocationId, BookNumber, ChapterNumber, DocumentId, Track, IssueTagNumber, KeySymbol, MepsLanguage, Type, Title) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", record)
                        conn_mesclado.commit()
                        print(f"Registro com LocationId {location_id} mesclado com sucesso.")
                    except sqlite3.IntegrityError:
                        print(f"Registro com LocationId {location_id} já existe no banco de dados mesclado. Gerando novo número aleatório...")
                        new_location_id = f"{location_id}{random_id()}"
                        print(f"Novo valor para LocationId: {new_location_id}")
                        update_database(caminho_db, "LocationId", location_id, new_location_id)
                        cursor_mesclado.execute("INSERT INTO Location (LocationId, BookNumber, ChapterNumber, DocumentId, Track, IssueTagNumber, KeySymbol, MepsLanguage, Type, Title) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (new_location_id, record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9]))
                        conn_mesclado.commit()

            cursor.close()
            conn.close()

    cursor_mesclado.close()
    conn_mesclado.close()
    print(">>>> Tabela 'Location' mesclada com sucesso!")
