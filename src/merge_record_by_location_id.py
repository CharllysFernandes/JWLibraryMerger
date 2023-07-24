import sqlite3

def merge_record_by_location_id(original_db_path, merged_db_path, location_id):
    """
    Mescla um registro com o LocationId informado do banco de dados original para o banco de dados mesclado.

    Parâmetros:
        original_db_path (str): O caminho para o banco de dados original contendo o registro a ser mesclado.
        merged_db_path (str): O caminho para o banco de dados mesclado onde o registro será mesclado.
        location_id (int): O LocationId do registro a ser mesclado.

    Descrição:
        A função conecta-se aos bancos de dados original e mesclado e busca o registro com o LocationId fornecido no
        banco de dados original. Se o registro for encontrado, ele é mesclado para o banco de dados mesclado.
        O novo registro no banco de dados mesclado manterá o mesmo LocationId e todas as outras informações do registro
        original. Se o registro não for encontrado no banco de dados original, a função imprime uma mensagem de erro.

    Exemplo de uso:
        merge_record_by_location_id("caminho_do_banco_de_dados_original", "caminho_do_banco_de_dados_mesclado", 1234)
    """
    try:
        # Conecta-se ao banco de dados original
        conn_original = sqlite3.connect(original_db_path)
        cursor_original = conn_original.cursor()

        # Conecta-se ao banco de dados mesclado
        conn_merged = sqlite3.connect(merged_db_path)
        cursor_merged = conn_merged.cursor()

        # Busca o registro com o LocationId fornecido no banco de dados original
        cursor_original.execute("SELECT * FROM Location WHERE LocationId=?", (location_id,))
        record = cursor_original.fetchone()

        if record:
            # Mescla o registro para o banco de dados mesclado
            cursor_merged.execute("INSERT INTO Location (LocationId, BookNumber, ChapterNumber, DocumentId, Track, IssueTagNumber, KeySymbol, MepsLanguage, Type, Title) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", record)
            conn_merged.commit()
            print(f"Registro com LocationId {location_id} mesclado com sucesso.")
        else:
            print(f"Registro com LocationId {location_id} não encontrado no banco de dados original.")

    except sqlite3.Error as e:
        print(f"Erro ao mesclar o registro usando o LocationId: {e}")

    # finally:
    #     # Fecha a conexão com os bancos de dados
    #     cursor_original.close()
    #     conn_original.close()
    #     cursor_merged.close()
    #     conn_merged.close()
