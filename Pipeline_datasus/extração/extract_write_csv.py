import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import logging

logging.basicConfig(filename='application.log', filemode='a')

def obter_dados_api(url, parametros, usuario, senha):
    try:
        resposta = requests.post(url, json=parametros, auth=HTTPBasicAuth(usuario, senha))
        resposta.raise_for_status()
    except requests.exceptions.RequestException as erro:
        logging.error("Erro na requisição:", erro)
        return None
    return resposta.json()

def normalizar_dados_json(dados, caminho_registro, separador, nivel):
    try:
        dataframe = pd.json_normalize(dados, record_path=caminho_registro, sep=separador, max_level=nivel)
    except Exception as e:
        logging.error("Erro ao normalizar dados JSON:", e)
        return None
    return dataframe

def transformar_dados(dataframe_api):
    try:
        dataframe = dataframe_api.copy()
        colunas_selecionadas = [
            '_source_paciente_id',
            '_source_paciente_idade',
            '_source_paciente_enumSexoBiologico',
            '_source_paciente_racaCor_codigo',
            '_source_paciente_racaCor_valor',
            '_source_paciente_endereco_nmMunicipio',
            '_source_paciente_endereco_uf',
            '_source_vacina_nome',
            '_source_vacina_descricao_dose',
            '_source_vacina_dataAplicacao'
        ]

        print('Filtrando colunas...')
        dataframe_filtrado = dataframe[colunas_selecionadas].drop_duplicates()
        dataframe_filtrado.columns = dataframe_filtrado.columns.str.replace('_source_', '')

        print('Preenchendo valores ausentes...')
        dataframe_filtrado['paciente_endereco_uf'].fillna('BR', inplace=True)
        dataframe_filtrado['vacina_descricao_dose'].fillna('-', inplace=True)

        print('Convertendo e formatando a data de aplicação da vacina...')
        dataframe_filtrado['vacina_dataAplicacao'] = pd.to_datetime(dataframe_filtrado['vacina_dataAplicacao']).dt.strftime('%Y-%m-%d')

    except Exception as e:
        logging.error("Erro ao transformar dados:", e)
        return None
    return dataframe_filtrado

def salvar_csv(dataframe, nome_arquivo):
    try:
        print('Salvando arquivo CSV...')
        if 'vacina_dataAplicacao' not in dataframe.columns:
            print("Aviso: A coluna 'vacina_dataAplicacao' não está presente nos dados.")
        dataframe.to_csv(nome_arquivo + '.csv', index=False)
    except Exception as e:
        logging.error("Erro ao salvar CSV:", e)
        print('Falha ao salvar arquivo CSV!')

if __name__ == '__main__':
    URL_API = 'https://imunizacao-es.saude.gov.br/_search'
    PARAMETROS = {'size': 10000}
    USUARIO = 'imunizacao_public'
    SENHA = 'qlto5t&7r_@+#Tlstigi'
    NOME_ARQUIVO = 'dados_vacinacao_COVID19'

    print('Obtendo dados da API...')
    dados_api = obter_dados_api(url=URL_API, parametros=PARAMETROS, usuario=USUARIO, senha=SENHA)
    if dados_api is None:
        print('Requisição dos dados da API falhou!')
    else:
        print('Normalizando dados JSON...')
        dataframe = normalizar_dados_json(dados=dados_api, caminho_registro=['hits', 'hits'], separador='_', nivel=1)
        if dataframe is None:
            print('Normalização dos dados JSON falhou!')
        else:
            print('Transformando dados...')
            dataframe_transformado = transformar_dados(dataframe_api=dataframe)
            if dataframe_transformado is None:
                print('Transformação dos dados falhou!')
            else:
                print('Salvando dados em CSV...')
                salvar_csv(dataframe_transformado, NOME_ARQUIVO)
                print('Dados salvos em CSV com sucesso!')
