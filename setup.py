from setuptools import setup, find_packages

setup(
    name='JW_Merger_Playlist',    # Nome do pacote
    version='0.1.0',       # Número de versão do pacote
    packages=find_packages(),   # Lista de pacotes a serem incluídos
    entry_points={
        'console_scripts': ['jwlibrarymerger=jwlibrarymerger.main:main'],   # Script de entrada (opcional)
    },
)
