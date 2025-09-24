# f1_data_collector.py

import os
import logging
import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

# --- Configuração Inicial ---
# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração básica de logging para exibir informações no terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Constantes e Variáveis Configuráveis ---
BASE_URL = "https://api.openf1.org/v1"
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "openf1_data"

# Parâmetros para a demonstração, carregados do .env
SESSION_KEY_DEMO = os.getenv("SESSION_KEY")


def connect_to_mongo(uri: str, db_name: str):
    """
    Cria uma conexão com o banco de dados MongoDB.

    Args:
        uri (str): A string de conexão do MongoDB.
        db_name (str): O nome do banco de dados a ser acessado.

    Returns:
        Database: O objeto do banco de dados do PyMongo em caso de sucesso, ou None.
    """
    logging.info("Tentando conectar ao MongoDB...")
    if not uri:
        logging.error("A variável de ambiente MONGO_URI não foi definida.")
        return None
    try:
        client = MongoClient(uri)
        # O comando ismaster() é uma forma rápida de verificar a conexão.
        client.admin.command('ismaster')
        logging.info("Conexão com o MongoDB estabelecida com sucesso.")
        return client[db_name]
    except PyMongoError as e:
        logging.error(f"Erro ao conectar ao MongoDB: {e}")
        return None


def fetch_data(endpoint: str, params: dict) -> list:
    """
    Busca dados de um endpoint específico da API OpenF1.

    Args:
        endpoint (str): O endpoint da API a ser consultado (ex: 'sessions').
        params (dict): Um dicionário de parâmetros para a requisição.

    Returns:
        list: Uma lista de dicionários contendo os dados, ou uma lista vazia em caso de erro.
    """
    url = f"{BASE_URL}/{endpoint}"
    logging.info(f"Buscando dados de {url} com parâmetros {params}...")
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Lança uma exceção para respostas com erro (4xx ou 5xx)
        data = response.json()
        logging.info(f"Sucesso! {len(data)} registros recebidos de {endpoint}.")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar dados da API em {url}: {e}")
        return []


def save_to_collection(db, data: list, collection_name: str, unique_keys: list):
    """
    Salva uma lista de documentos em uma collection do MongoDB de forma idempotente.

    Usa update_one com upsert=True para inserir novos documentos ou atualizar
    os existentes com base em um conjunto de chaves únicas.

    Args:
        db: O objeto do banco de dados do PyMongo.
        data (list): A lista de documentos a serem salvos.
        collection_name (str): O nome da collection de destino.
        unique_keys (list): Uma lista de strings com os nomes das chaves que
                            formam o identificador único de um documento.
    """
    if not data:
        logging.warning(f"Nenhum dado para salvar na collection '{collection_name}'. Pulando.")
        return

    collection = db[collection_name]
    logging.info(f"Salvando {len(data)} documentos na collection '{collection_name}'...")

    upserted_count = 0
    modified_count = 0

    for item in data:
        # Cria o filtro de busca com base nas chaves únicas
        filter_query = {key: item.get(key) for key in unique_keys}

        try:
            result = collection.update_one(
                filter_query,
                {"$set": item},
                upsert=True
            )
            if result.upserted_id:
                upserted_count += 1
            elif result.modified_count > 0:
                modified_count += 1
        except PyMongoError as e:
            logging.error(f"Erro ao salvar documento na collection '{collection_name}': {e}")
            continue # Continua para o próximo item

    logging.info(
        f"Operação em '{collection_name}' concluída. "
        f"Documentos inseridos: {upserted_count}, atualizados: {modified_count}."
    )


def main():
    """
    Função principal que orquestra a coleta e armazenamento dos dados.
    """
    logging.info("--- Iniciando Coletor de Dados da OpenF1 ---")

    db = connect_to_mongo(MONGO_URI, DB_NAME)
    if not db:
        logging.error("Finalizando script devido a falha na conexão com o banco de dados.")
        return

    # --- 1. Buscar e Salvar Dados da Sessão ---
    sessions_data = fetch_data("sessions", {"session_key": SESSION_KEY_DEMO})
    save_to_collection(db, sessions_data, "sessions", ["session_key"])

    # --- 2. Buscar e Salvar Dados dos Pilotos da Sessão ---
    drivers_data = fetch_data("drivers", {"session_key": SESSION_KEY_DEMO})
    save_to_collection(db, drivers_data, "drivers", ["session_key", "driver_number"])

    # --- 3. Buscar e Salvar Dados das Voltas da Sessão ---
    laps_data = fetch_data("laps", {"session_key": SESSION_KEY_DEMO})
    save_to_collection(db, laps_data, "laps", ["session_key", "driver_number", "lap_number"])

    logging.info("--- Processo de Coleta de Dados Finalizado com Sucesso ---")


if __name__ == "__main__":
    main()