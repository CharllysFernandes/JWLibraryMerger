import os
import zipfile

def zip_merged_folder(merged_dir):
    """
    Compacta a pasta "merged" em um arquivo chamado "merged_playlist.jwlibrary".

    Parâmetros:
        merged_dir (str): O caminho para a pasta "merged" que será compactada.

    Descrição:
        A função cria um arquivo zip chamado "merged_playlist.jwlibrary" na pasta raiz do programa.
        Em seguida, ela percorre todos os arquivos e subpastas dentro da pasta "merged" e os adiciona ao arquivo zip.
        Após adicionar todos os arquivos, o arquivo zip é fechado.

    Exemplo de uso:
        zip_merged_folder("caminho_da_pasta_merged")
    """
    # Nome do arquivo zip a ser criado
    zip_filename = "merged_playlist.jwlibrary"

    # Caminho completo para o arquivo zip
    zip_path = os.path.join(os.getcwd(), zip_filename)

    # Inicializa o arquivo zip em modo de escrita
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # Percorre todos os arquivos e subpastas dentro da pasta "merged"
        for foldername, subfolders, filenames in os.walk(merged_dir):
            for filename in filenames:
                # Caminho completo para o arquivo atual
                file_path = os.path.join(foldername, filename)
                # Caminho relativo para o arquivo dentro do arquivo zip
                relative_path = os.path.relpath(file_path, merged_dir)
                # Adiciona o arquivo ao arquivo zip
                zip_file.write(file_path, relative_path)

    print(f"Pasta 'merged' compactada em '{zip_filename}' com sucesso.")
