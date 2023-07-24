import random
import os

def count_db(pasta):
    """
    Conta a quantidade de arquivos de banco de dados (.db) encontrados na pasta especificada.

    Parâmetros:
        pasta (str): O caminho para a pasta a ser verificada.

    Retorno:
        count (int): O número de arquivos de banco de dados (.db) encontrados na pasta.

    Descrição:
        A função percorre todos os arquivos na pasta especificada e verifica se cada arquivo possui a extensão ".db".
        Se o arquivo tiver a extensão ".db", o contador 'count' é incrementado.
        Ao final da iteração, a função retorna o valor de 'count', que representa a quantidade total de arquivos de banco de dados encontrados na pasta.
    """

    count = 0
    for arquivo in os.listdir(pasta):
        if arquivo.endswith(".db"):
            count += 1
    return count


def random_id():
    """
    Gera um ID aleatório composto por três dígitos.

    Retorno:
        id_aleatorio (int): Um ID aleatório de três dígitos.

    Descrição:
        A função utiliza a biblioteca 'random' para gerar números aleatórios.
        O ID aleatório é composto por três dígitos, onde o primeiro dígito é um número entre 1 e 9 (inclusivo),
        e os outros dois dígitos são números entre 0 e 9 (inclusivo).
        O ID aleatório é retornado como resultado da função.
    """
    return random.randint(100, 999)
