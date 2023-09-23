import pandas as pd
import logging

logging.basicConfig(filename='application.log', filemode='a')

def transformar_e_salvar_parquet(dataframe, nome_arquivo):
    dataframe['vacina_dataAplicacao'] = pd.to_datetime(dataframe['vacina_dataAplicacao'])
    dataframe['ano'] = dataframe['vacina_dataAplicacao'].dt.year
    dataframe['mes'] = dataframe['vacina_dataAplicacao'].dt.month
    dataframe.to_parquet(nome_arquivo + '_parquet', partition_cols=['ano', 'mes'])

if __name__ == '__main__':
    NOME_ARQUIVO = '/home/victorcoelho/repositorio_wsl/Pipeline_datasus/dados_batch/dados_vacinacao_COVID19'
    NOME_ARQUIVO_CSV = NOME_ARQUIVO + '.csv'

    print('Lendo dados do arquivo CSV...')
    dataframe = pd.read_csv(NOME_ARQUIVO_CSV)

    print('Salvando dados em Parquet...')
    transformar_e_salvar_parquet(dataframe, NOME_ARQUIVO)

    print('Dados salvos em Parquet com sucesso!')
