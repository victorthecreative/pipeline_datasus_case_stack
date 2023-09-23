import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore

# Caminho para o seu arquivo de credenciais do Firestore
CRED_PATH = '/home/victorcoelho/repositorio_wsl/testecase-main/cred.json'

# Caminho para o seu arquivo CSV
CSV_PATH = '/home/victorcoelho/repositorio_wsl/testecase-main/dados_batch/dados_vacinacao.csv'

# Inicialize o SDK do Firebase Admin
cred = credentials.Certificate(CRED_PATH)
firebase_admin.initialize_app(cred)

# Abra o arquivo CSV e leia os dados
df = pd.read_csv(CSV_PATH)

# Obtenha uma referência ao banco de dados Firestore
db = firestore.client()

# Itere sobre as linhas do DataFrame e adicione-as ao Firestore
for index, row in df.iterrows():
    # Suponha que você esteja adicionando os dados a uma coleção chamada 'minha_colecao'
    db.collection('coleção_datasus').add(row.to_dict())
