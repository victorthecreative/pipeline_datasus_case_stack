import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Date

# Caminho do seu arquivo CSV
file_path = '/home/victorcoelho/repositorio_wsl/testecase-main/dados_batch/dados_vacinacao.csv'

# Leia o arquivo CSV
df = pd.read_csv(file_path)

# Remova linhas duplicadas com base na coluna 'paciente_id'
df.drop_duplicates(subset='paciente_id', keep='first', inplace=True)

# Substitua os valores de USERNAME, PASSWORD, HOST, PORT e DBNAME
USERNAME = 'myuser'
PASSWORD = '123456'
HOST = '34.95.140.133'
PORT = '5432'
DBNAME = 'susdatabase'

# URL de conexão ao banco de dados PostgreSQL
DATABASE_URL = f'postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}'

# Crie a conexão com o banco de dados
engine = create_engine(DATABASE_URL)

# Substitua o valor de TABLE_NAME
TABLE_NAME = 'covid19_vacinacao'

# Defina a metadata
metadata = MetaData()

# Defina a estrutura da tabela
table = Table(
    TABLE_NAME, metadata,
    Column('paciente_id', String, primary_key=True),
    Column('paciente_idade', Integer),
    Column('paciente_enumSexoBiologico', String),
    Column('paciente_racaCor_codigo', Integer),
    Column('paciente_racaCor_valor', String),
    Column('paciente_endereco_nmMunicipio', String),
    Column('paciente_endereco_uf', String),
    Column('vacina_nome', String),
    Column('vacina_descricao_dose', String),
    Column('vacina_dataAplicacao', Date)
)

# Crie a tabela no banco de dados
metadata.create_all(engine)

# Insira os dados na tabela
df.to_sql(TABLE_NAME, con=engine, index=False, if_exists='append')
