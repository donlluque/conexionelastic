import os
import json
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import logging
import time
import sys

# Configurar logging con formato UTF-8
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("etl_process.log", encoding='utf-8')]
)

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Configuración para ElasticSearch
es_host = os.getenv('ES_HOST')
es_user = os.getenv('ES_USER')
es_password = os.getenv('ES_PASSWORD')
batch_size = int(os.getenv('BATCH_SIZE', 1000))  # Valor por defecto si no está definido en .env

# Crear directorios si no existen
os.makedirs('json', exist_ok=True)

# Obtener lista de índices de ElasticSearch
def get_indices_from_elasticsearch():
    try:
        response = requests.get(
            f"{es_host}/_cat/indices?format=json",
            auth=HTTPBasicAuth(es_user, es_password)
        )
        response.raise_for_status()
        return [index['index'] for index in response.json()]
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener los índices de ElasticSearch: {e}")
        return []

# Conectar a ElasticSearch y extraer datos
def fetch_data_from_elasticsearch(index):
    try:
        response = requests.get(
            f"{es_host}/{index}/_search?size={batch_size}",
            auth=HTTPBasicAuth(es_user, es_password)
        )
        response.raise_for_status()
        data = response.json()['hits']['hits']

        if not data:
            reason = f"Índice '{index}' vacío."
            return None, reason

        # Guardar datos en formato JSON
        save_json(index, data)
        return True, None

    except requests.exceptions.RequestException as e:
        reason = f"Error al obtener datos del índice '{index}': {e}"
        return None, reason

# Guardar los datos en formato JSON
def save_json(index, data):
    try:
        json_file = f"json/{index}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump([doc['_source'] for doc in data], f, ensure_ascii=False, indent=4)
        logging.info(f"Datos del índice '{index}' guardados en '{json_file}'.")
    except IOError as e:
        logging.error(f"Error al guardar los datos del índice '{index}' en JSON: {e}")

# Animación de puntos en la consola
def print_loading_animation():
    sys.stdout.write('.')
    sys.stdout.flush()

# Ejecutar ETL
if __name__ == "__main__":
    try:
        es_indices = get_indices_from_elasticsearch()

        if not es_indices:
            logging.error("No se encontraron índices en ElasticSearch.")
            exit(1)

        success_count = 0
        fail_count = 0
        failed_indices = []

        for index in es_indices:
            result, reason = fetch_data_from_elasticsearch(index)
            if result:
                success_count += 1
            else:
                fail_count += 1
                failed_indices.append((index, reason))

            # Mostrar animación con puntos consecutivos
            print_loading_animation()

        # Mostrar resultados finales
        sys.stdout.write(f"\nProceso completado: {success_count} éxitos, {fail_count} fallos.\n")
        if failed_indices:
            for index, reason in failed_indices:
                sys.stdout.write(f"Fallo en índice '{index}': {reason}\n")

        # Logging final
        logging.info(f"Proceso completado: {success_count} éxitos, {fail_count} fallos.")
        for index, reason in failed_indices:
            logging.error(f"Fallo en índice '{index}': {reason}")

    except Exception as e:
        logging.critical(f"Error crítico durante la ejecución del ETL: {e}")
        exit(1)
