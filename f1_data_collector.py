# f1_data_collector.py

import os
import requests
import pymongo
import sys
from dotenv import load_dotenv

# --- Configuração Inicial ---
# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Lê as configurações do ambiente
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "openf1_data") # Usa "openf1_data" como padrão
API_BASE_URL = "https://api.openf1.org/v1"

# Parâmetros para o caso de uso de demonstração
# Sessão de Corrida (Race) do GP da Itália de 2023 em Monza
SESSION_KEY_DEMO = 9159

def connect_to_mongo(mongo_uri: str, db_name: str):
    """
    Estabelece conexão com o MongoDB e retorna o objeto do banco de dados.

    Args:
        mongo_uri (str): A string de conexão do MongoDB.
        db_name (str): O nome do banco de dados a ser utilizado.

    Returns:
        pymongo.database.Database: Objeto do banco de dados ou None em caso de falha.
    """
    print("Conectando ao MongoDB...")
    try:
        client = pymongo.MongoClient(mongo_uri)
        client.admin.command('ping')  # Verifica se a conexão foi bem-sucedida
        print("Conexão com MongoDB estabelecida com sucesso.")
        return client[db_name]
    except pymongo.errors.ConnectionFailure as e:
        print(f"Erro ao conectar ao MongoDB: {e}", file=sys.stderr)
        sys.exit(1) # Encerra o script em caso de falha na conexão

def fetch_data(endpoint: str, params: dict = None) -> list:
    """
    Busca dados de um endpoint específico da API OpenF1.

    Args:
        endpoint (str): O endpoint da API a ser consultado (ex: 'sessions').
        params (dict, optional): Dicionário de parâmetros para a requisição.

    Returns:
        list: Uma lista de dicionários contendo os dados da API, ou uma lista vazia em caso de erro.
    """
    url = f"{API_BASE_URL}/{endpoint}"
    print(f"Buscando dados de: {url} com parâmetros: {params}")
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Lança uma exceção para códigos de erro HTTP (4xx ou 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer requisição para a API: {e}", file=sys.stderr)
        return []

def save_to_collection(db, data: list, collection_name: str, unique_keys: list):
    """
    Salva uma lista de documentos em uma collection do MongoDB, garantindo idempotência.

    Usa update_one com upsert=True para inserir novos documentos ou atualizar
    os existentes com base em chaves únicas.

    Args:
        db (pymongo.database.Database): O objeto do banco de dados.
        data (list): A lista de documentos (dicionários) a serem salvos.
        collection_name (str): O nome da collection de destino.
        unique_keys (list): Uma lista de strings com os nomes das chaves que formam o identificador único.
    """
    if not data:
        print(f"Nenhum dado para salvar na collection '{collection_name}'.")
        return

    collection = db[collection_name]
    print(f"Salvando {len(data)} documentos na collection '{collection_name}'...")

    operations = []
    for item in data:
        # Cria o filtro de busca com base nas chaves únicas
        filter_query = {key: item.get(key) for key in unique_keys}
        
        # Cria a operação de update com $set para atualizar todos os campos e upsert=True
        # Isso garante que se o documento não existir, ele será criado.
        operations.append(
            pymongo.UpdateOne(filter_query, {"$set": item}, upsert=True)
        )
    
    try:
        # Executa as operações em lote (bulk) para melhor performance
        result = collection.bulk_write(operations)
        print(f"Operação concluída. Inseridos: {result.upserted_count}, Modificados: {result.modified_count}.")
    except pymongo.errors.PyMongoError as e:
        print(f"Erro ao salvar dados no MongoDB na collection '{collection_name}': {e}", file=sys.stderr)


def main():
    """
    Função principal que orquestra a coleta e armazenamento dos dados.
    """
    print("--- Iniciando Coletor de Dados da OpenF1 ---")
    
    # 1. Conectar ao MongoDB
    db = connect_to_mongo(MONGO_URI, DB_NAME)
    
    # 2. Buscar e salvar dados da sessão específica
    print("\n[Passo 1/3] Coletando dados da Sessão...")
    sessions_data = fetch_data("sessions", params={"session_key": SESSION_KEY_DEMO})
    save_to_collection(db, sessions_data, "sessions", ["session_key"])
    
    # 3. Buscar e salvar dados dos pilotos para a sessão
    print("\n[Passo 2/3] Coletando dados dos Pilotos...")
    drivers_data = fetch_data("drivers", params={"session_key": SESSION_KEY_DEMO})
    save_to_collection(db, drivers_data, "drivers", ["session_key", "driver_number"])

    # 4. Buscar e salvar dados das voltas para a sessão
    print("\n[Passo 3/3] Coletando dados das Voltas...")
    laps_data = fetch_data("laps", params={"session_key": SESSION_KEY_DEMO})
    save_to_collection(db, laps_data, "laps", ["session_key", "driver_number", "lap_number"])

    print("\n--- Coleta de Dados Concluída com Sucesso! ---")


if __name__ == "__main__":
    main()