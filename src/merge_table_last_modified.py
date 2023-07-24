import sqlite3
import os
from datetime import datetime, timezone

def update_last_modified(database_path):
    """
    Atualiza a coluna "LastModified" na tabela "LastModified" do banco de dados com a data e hora atual em formato ISO (UTC).

    Esta função recebe o caminho para o banco de dados mesclado e realiza o seguinte:
    - Conecta-se ao banco de dados.
    - Obtém a data e hora atual no fuso horário UTC.
    - Formata a data e hora no formato "2023-06-21T21:37:13Z".
    - Faz o update do valor na coluna "LastModified" da tabela "LastModified".

    Parâmetros:
        database_path (str): Caminho para o banco de dados mesclado contendo a tabela "LastModified".

    Retorna:
        Nada. A função apenas atualiza o valor da coluna "LastModified" no banco de dados.
    """
    # Caminho completo para o arquivo de banco de dados
    caminho_db_mesclado = os.path.join(database_path, "userData.db")

    # Conecta-se ao banco de dados
    conn_mesclado = sqlite3.connect(caminho_db_mesclado)
    cursor_mesclado = conn_mesclado.cursor()

    try:
        # Obtém a data e hora atual no formato ISO (UTC)
        current_datetime = datetime.now(timezone.utc)

        # Formata a data e hora no formato "2023-06-21T21:37:13Z"
        new_last_modified = current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Update the LastModified value in the table
        cursor_mesclado.execute("UPDATE LastModified SET LastModified = ?", (new_last_modified,))
        conn_mesclado.commit()

        print("Update da tabela LastModified realizado com sucesso.")

    except sqlite3.Error as e:
        print(f"Erro ao atualizar a tabela LastModified: {e}")

    finally:
        # Close the connection with the database
        cursor_mesclado.close()
        conn_mesclado.close()
