import pandas as pd
from sqlalchemy import create_engine
import os

# Função para processar dados históricos (2022 e 2023)
def processar_dados_historicos(file_path, tipo):

    file_path = os.path.join(os.path.dirname(__file__), '../data/raw/COTAHIST_A2024.TXT')

    df = pd.read_fwf(
        file_path, 
        widths=[2, 8, 2, 12, 3, 12, 10, 3, 4, 13, 13, 13, 13, 13, 13, 13, 5, 18, 16, 11, 1, 8, 7, 7, 12, 3],
        names=["TIPREG", "DATA_PREGAO", "CODBDI", "CODNEG", "TPMERC", "NOMRES", "ESPECI", "PRAZOT", "MODREF", "PREABE", 
               "PREMAX", "PREMIN", "PREMED", "PREULT", "PREOFC", "PREOFV", "TOTNEG", "QUATOT", "VOLTOT", "PREEXE", 
               "INDOPC", "DATVEN", "FATCOT", "PTOEXE", "CODISI", "DISMES"]
    )

    # Filtrar as colunas necessárias
    df_filtrado = df[['DATA_PREGAO', 'CODNEG', 'NOMRES', 'PREABE', 'PREMIN', 'PREMAX', 'PREULT', 'VOLTOT']]

    # Renomear colunas para refletir os nomes do banco de dados
    df_filtrado = df_filtrado.rename(columns={
        'CODNEG': 'papelName',
        'NOMRES': 'company_name',
        'DATA_PREGAO': 'date',
        'PREABE': 'openPrice',
        'PREMIN': 'lowPrice',
        'PREMAX': 'highPrice',
        'PREULT': 'closePrice',
        'VOLTOT': 'volume'
    })

    # Realizar transformações necessárias
    df_filtrado['date'] = pd.to_datetime(df_filtrado['date'], format='%Y%m%d')
    df_filtrado['openPrice'] = df_filtrado['openPrice'].apply(lambda x: float(x) / 100)
    df_filtrado['lowPrice'] = df_filtrado['lowPrice'].apply(lambda x: float(x) / 100)
    df_filtrado['highPrice'] = df_filtrado['highPrice'].apply(lambda x: float(x) / 100)
    df_filtrado['closePrice'] = df_filtrado['closePrice'].apply(lambda x: float(x) / 100)

    # Salvar os dados processados no formato CSV
    caminho_saida = f"data/stage/historico/{tipo}_processado.csv"
    df_filtrado.to_csv(caminho_saida, index=False)

    return caminho_saida


# Função para processar dados diários (2024)
def processar_dados_diarios(caminho_arquivo):
    df = pd.read_fwf(
        caminho_arquivo, 
        widths=[2, 8, 2, 12, 3, 12, 10, 3, 4, 13, 13, 13, 13, 13, 13, 13, 5, 18, 16, 11, 1, 8, 7, 7, 12, 3],
        names=["TIPREG", "DATA_PREGAO", "CODBDI", "CODNEG", "TPMERC", "NOMRES", "ESPECI", "PRAZOT", "MODREF", "PREABE", 
               "PREMAX", "PREMIN", "PREMED", "PREULT", "PREOFC", "PREOFV", "TOTNEG", "QUATOT", "VOLTOT", "PREEXE", 
               "INDOPC", "DATVEN", "FATCOT", "PTOEXE", "CODISI", "DISMES"]
    )

    # Filtrar e transformar dados
    df_filtrado = df[['DATA_PREGAO', 'CODNEG', 'NOMRES', 'PREABE', 'PREMIN', 'PREMAX', 'PREULT', 'VOLTOT']]
    df_filtrado = df_filtrado.rename(columns={
        'CODNEG': 'papelName',
        'NOMRES': 'company_name',
        'DATA_PREGAO': 'date',
        'PREABE': 'openPrice',
        'PREMIN': 'lowPrice',
        'PREMAX': 'highPrice',
        'PREULT': 'closePrice',
        'VOLTOT': 'volume'
    })
    df_filtrado['date'] = pd.to_datetime(df_filtrado['date'], format='%Y%m%d')
    df_filtrado['openPrice'] = df_filtrado['openPrice'].apply(lambda x: float(x) / 100)
    df_filtrado['lowPrice'] = df_filtrado['lowPrice'].apply(lambda x: float(x) / 100)
    df_filtrado['highPrice'] = df_filtrado['highPrice'].apply(lambda x: float(x) / 100)
    df_filtrado['closePrice'] = df_filtrado['closePrice'].apply(lambda x: float(x) / 100)

    # Salvar em stage
    caminho_saida = "data/stage/diario/processado_2024.csv"
    df_filtrado.to_csv(caminho_saida, index=False)

    return caminho_saida
