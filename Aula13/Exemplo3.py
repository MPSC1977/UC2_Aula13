import pandas as pd
import polars as pl
import os
import gc
from datetime import datetime

try:

    ENDERECO_DADOS = r'./dados/'

    hora_import = datetime.now()

    print('Obtendo dados...')

    inicio = datetime.now()

    lista_arquivos = []

    lista_dir_arquivos = os.listdir(ENDERECO_DADOS)

    for arquivo in lista_dir_arquivos:
       
        if arquivo.endswith('.csv'):
            lista_arquivos.append(arquivo)
        
        print(lista_arquivos)

    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}')

        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia = df

        print(df_bolsa_familia.head())

        df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')

        del df_bolsa_familia

        gc.collect()

    hora_impressao = datetime.now()

    print(f'Tempo de Execução: {hora_impressao - hora_import}')

except ImportError as e:
    print('Erro ao obter dados: ', e)