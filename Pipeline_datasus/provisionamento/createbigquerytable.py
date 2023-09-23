from google.cloud import bigquery

# Inicialize um cliente do BigQuery
client = bigquery.Client()

# Especifique o ID do seu projeto
project_id = "datasus-pipeline"

# Especifique o ID do conjunto de dados
dataset_id = "datasus_vacinacao_covid19_stg"

# Crie um objeto DatasetReference usando o ID do projeto e do conjunto de dados
dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

# Construa o objeto Dataset
dataset = bigquery.Dataset(dataset_ref)

# Especifique a descrição do conjunto de dados
dataset.description = "Dados de vacinação da covid19"

# Crie o conjunto de dados
dataset = client.create_dataset(dataset)  # API request

print(f"Conjunto de dados {dataset_id} criado.")

# Especifique o ID da tabela
table_id = "covid19_table_stg"

# Crie um objeto TableReference usando o DatasetReference e o ID da tabela
table_ref = dataset_ref.table(table_id)

# Defina o esquema da tabela
schema = [
    bigquery.SchemaField("paciente_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("paciente_idade", "INTEGER"),
    bigquery.SchemaField("paciente_enumSexoBiologico", "STRING"),
    bigquery.SchemaField("paciente_racaCor_codigo", "INTEGER"),
    bigquery.SchemaField("paciente_racaCor_valor", "STRING"),
    bigquery.SchemaField("paciente_endereco_nmMunicipio", "STRING"),
    bigquery.SchemaField("paciente_endereco_uf", "STRING"),
    bigquery.SchemaField("vacina_nome", "STRING"),
    bigquery.SchemaField("vacina_descricao_dose", "STRING"),
    bigquery.SchemaField("vacina_dataAplicacao", "DATE"),
]

# Construa o objeto Table
table = bigquery.Table(table_ref, schema=schema)

# Crie a tabela
table = client.create_table(table)  # API request

print(f"Tabela {table_id} criada no conjunto de dados {dataset_id}.")
