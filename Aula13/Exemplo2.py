import pandas as pd
import polars as pl
import gc
from datetime import datetime

try:

    ENDERECO_DADOS = r'./dados/'

    hora_import = datetime.now()

    print('Obtendo dados...')

    inicio = datetime.now()

    lista_arquivos = ['202401_NovoBolsaFamilia.csv', '202402_NovoBolsaFamilia.csv']

    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}')

        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia = df
        
        print(df.head())

        del df

        gc.collect()

        #TEMPO DE EXECUÇÃO NO PANDAS 0:02:09
        #TEMPO DE EXECUÇÃO NO POLARS 0:00:44

    hora_impressao = datetime.now()

    print(f'Tempo de Execução: {hora_impressao - hora_import}')

except ImportError as e:
    print('Erro ao obter dados: ', e)