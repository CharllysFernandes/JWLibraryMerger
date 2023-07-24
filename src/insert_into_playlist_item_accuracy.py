import sqlite3
import os

def insert_into_playlist_item_accuracy(database_path):
    """
    Insere os dados na tabela PlaylistItemAccuracy do banco de dados especificado.

    Parâmetros:
        database_path (str): O caminho para o arquivo de banco de dados onde os dados serão inseridos.

    Descrição:
        A função conecta-se ao banco de dados especificado, em seguida, insere os dados na tabela PlaylistItemAccuracy.
        Os dados são inseridos na coluna "PlaylistItemAccuracyId" e "Description" da tabela.
        A função tenta inserir dois registros na tabela, representando duas descrições de acurácia: "Accurate" e "NeedsUserVerification".
        A cláusula "INSERT OR IGNORE" é utilizada para evitar a inserção duplicada de dados já existentes.
        Após a inserção dos dados, a função realiza o commit das alterações no banco de dados.

    Observação:
        A tabela PlaylistItemAccuracy deve existir no banco de dados fornecido, caso contrário, ocorrerá um erro.

    Exemplo de uso:
        insert_into_playlist_item_accuracy("caminho_do_banco_de_dados.db")
    """
    # Combinar o caminho do banco de dados com o nome do arquivo "userData.db"
    caminho_db_mesclado = os.path.join(database_path, "userData.db")

    # Conectar ao banco de dados
    conn_mesclado = sqlite3.connect(caminho_db_mesclado)

    try:
        # Criar um cursor para executar comandos SQL no banco de dados
        cursor_mesclado = conn_mesclado.cursor()

        # Inserir os dados na tabela PlaylistItemAccuracy
        cursor_mesclado.execute("INSERT OR IGNORE INTO PlaylistItemAccuracy (PlaylistItemAccuracyId, Description) VALUES (?, ?)", (1, "Accurate"))
        cursor_mesclado.execute("INSERT OR IGNORE INTO PlaylistItemAccuracy (PlaylistItemAccuracyId, Description) VALUES (?, ?)", (2, "NeedsUserVerification"))

        # Confirmar as alterações realizadas no banco de dados
        conn_mesclado.commit()

        print("Dados inseridos na tabela PlaylistItemAccuracy com sucesso.")

    except sqlite3.Error as e:
        print(f"Erro ao inserir dados na tabela PlaylistItemAccuracy: {e}")

    finally:
        # Fechar a conexão com o banco de dados
        conn_mesclado.close()
