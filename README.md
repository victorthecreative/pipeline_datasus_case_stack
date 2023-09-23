# Projeto de Pipeline de Dados - Vacinação COVID-19

Este projeto consiste em uma pipeline de dados que coleta, transforma, modela e armazena informações sobre a vacinação da COVID-19 no Brasil, oriundas da API do DataSUS.

## Arquitetura

A arquitetura do projeto é composta pelas seguintes etapas e componentes:
![image](https://github.com/victorthecreative/pipeline_datasus_case_stack/assets/50841013/3bd01aca-cacc-492c-a4d4-4a47849a261b)

1. **Coleta de Dados:**

   - **API do DataSUS:** Fonte original dos dados sobre vacinação da COVID-19.
   - **Script import_request.py:** Realiza requisições à API, coleta, normaliza, transforma e salva os dados em CSV.

2. **Transformação e Conversão:**
   - **Script parquet_write.py:** Converte os dados do formato CSV para Parquet, particionado por ano e mês.
   - **Script ingest_firestore.py:** Insere os dados no Firestore para armazenamento NoSQL.
   - **Script cloudsql_db.py:** Insere os dados em uma tabela PostgreSQL para armazenamento SQL.
   - **Scripts createbigquerytable.py e write_bigquery_dataflow.py:** Utilizam Apache Beam e Dataflow para inserir os dados no BigQuery.
   - **Comando gcloud:**
     ```sh
     gcloud sql export csv dbdatasus gs://datasus_case-stack/vacina_datasus.csv --database=susdatabase --query="SELECT * FROM covid19_vacinacao"
     ```
     Este comando é utilizado para exportar os dados do Cloud SQL para o Cloud Storage.

3. **Modelagem de Dados com dbt:**
![Untitled (1)](https://github.com/victorthecreative/pipeline_datasus_case_stack/assets/50841013/47ad1475-939f-4815-9f7d-0fd8d62c83e3)
   - **dbt:** Ferramenta de transformação de dados que é utilizada para criar modelos/tabelas no BigQuery, incluindo:

     - **Tabelas Dimensões:** `dim_pacientes`, `dim_endereco`, `dim_vacina`.
     - **Tabela Fato:** `fact_aplicacao_vacina`.

5. **Armazenamento:**
   - **Firestore:** Banco de dados NoSQL para consultas flexíveis.
   - **PostgreSQL:** Banco de dados relacional SQL para consultas estruturadas.
   - **BigQuery:** Data Warehouse para análise de grandes volumes de dados e armazenamento das tabelas modeladas pelo dbt.

6. **Monitoramento e Logging:**
   - **Logging:** Registro de eventos e erros durante a execução dos scripts.

## Fluxo de Dados

1. **Coleta e Transformação Inicial:** Os dados são coletados da API do DataSUS, transformados e salvos em diferentes formatos e armazenamentos.
2. **Ingestão no BigQuery:** Os dados são processados pelo Apache Beam e Dataflow e inseridos no BigQuery.
3. **Modelagem com dbt:** Dentro do BigQuery, o dbt é utilizado para criar tabelas dimensões e uma tabela fato, estruturando os dados para análises.
4. **Análise e Consulta:** Os dados modelados estão disponíveis para análise e consulta no BigQuery, Firestore e PostgreSQL.
5. **Dataviz**: Como os dados modelos estão disponiveis, podemos usar o Power BI para criar visualizações, como a que esta abaixo, que foi criado com o sample que extraimos a API:
   
   <img width="665" alt="image" src="https://github.com/victorthecreative/pipeline_datasus_case_stack/assets/50841013/1276471e-d468-4527-b634-e3a35ccc6e74">


## Bibliotecas e Ferramentas Utilizadas

- **Requests:** Biblioteca Python para realizar requisições HTTP.
- **Pandas:** Biblioteca Python para manipulação e análise de dados.
- **Logging:** Módulo Python para registro de logs.
- **SQLAlchemy:** Biblioteca Python SQL Toolkit e Object-Relational Mapping.
- **Firebase Admin SDK:** SDK para interagir com o Firebase Firestore.
- **Google Cloud BigQuery Client Library:** Biblioteca cliente para interagir com o Google BigQuery.
- **Apache Beam:** Biblioteca para processamento de dados paralelo e distribuído.
- **dbt:** Ferramenta para transformação de dados em armazenamentos de dados SQL.

## Como Executar

1. **Coleta de Dados:**
   - Execute o script `import_request.py` para coletar os dados da API do DataSUS e salvar em CSV.

2. **Transformação e Conversão:**
   - Execute os scripts `parquet_write.py`, `ingest_firestore.py`, e `cloudsql_db.py` para transformar e salvar os dados em diferentes formatos e armazenamentos.
   - Após a execução do `cloudsql_db.py`, utilize o comando `gcloud` mencionado acima para exportar os dados para o Cloud Storage.

3. **Criação de Tabela no BigQuery:**
   - Execute o script `createbigquerytable.py` para criar o conjunto de dados e a tabela no BigQuery.

4. **Ingestão no BigQuery com Dataflow:**
   - Execute o script `write_bigquery_dataflow.py` para processar e inserir os dados no BigQuery usando Apache Beam e Dataflow.

5. **Modelagem com dbt:**
   - Configure e execute os modelos dbt para criar as tabelas dimensões e a tabela fato no BigQuery.

6. **Análise e Consulta:**
   - Utilize as ferramentas de consulta do BigQuery, Firestore, ou PostgreSQL para analisar os dados modelados.
