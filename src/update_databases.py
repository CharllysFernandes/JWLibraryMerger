import sqlite3

def update_database(file_path_database, column_update, value_initial, value_update):
    """
    Atualiza registros em todas as tabelas de um banco de dados SQLite onde a coluna especificada
    possui o valor inicial informado, substituindo-o pelo valor de atualização informado.

    Parâmetros:
        file_path_database (str): Caminho para o arquivo do banco de dados SQLite a ser atualizado.
        column_update (str): Nome da coluna a ser atualizada nas tabelas.
        value_initial (str ou int): Valor inicial da coluna que será substituído.
        value_update (str ou int): Novo valor que substituirá o valor inicial.

    Retorna:
        Nada. A função apenas atualiza os registros no banco de dados.

    Exemplo de uso:
        update_database("caminho_para_seu_banco_de_dados.db", "Nome", "AntigoValor", "NovoValor")
    """
    # Conectar ao banco de dados
    conn = sqlite3.connect(file_path_database)
    cursor = conn.cursor()

    # Obter a lista de tabelas do banco de dados
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Atualizar o registro em cada tabela que contém a coluna informada
    for table in tables:
        table = table[0]

        try:
            cursor.execute(f"UPDATE {table} SET {column_update} = ? WHERE {column_update} = ?", (value_update, value_initial))
            conn.commit()
            print(f"Registro: '{table}' - '{column_update}': {value_initial} -> {value_update}")

        except sqlite3.Error as e:
            print(f"Erro ao atualizar o registro em '{table}': {e}")
            conn.rollback()

    cursor.close()
    conn.close()
    
    print(f"{table} de {file_path_database} atualizada com sucesso!")