"""
Este script extrae los datos de TODOS los índices de ElasticSearch 
los convierte en JSON y guarda en una carpeta "json" 

"""


import os
import json
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Configuración para ElasticSearch
es_host = "http://TU_SERVIDOR:9200"
es_user = "TU_USUARIO"
es_password = "TU_PASS"

# Tamaño del lote para la carga de datos
batch_size = int(os.getenv('BATCH_SIZE', 1000))  # Valor por defecto si no está en .env

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
        indices = [index['index'] for index in response.json()]
        return indices
    except requests.RequestException as e:
        print(f"Error al obtener la lista de índices de Elasticsearch: {e}")
        return []

# Conectar a ElasticSearch y extraer datos
def fetch_data_from_elasticsearch(index):
    data = []
    scroll_id = None

    while True:
        try:
            url = f"{es_host}/{index}/_search?scroll=1m&size={batch_size}"
            if scroll_id:
                url = f"{es_host}/_search/scroll?scroll=1m&scroll_id={scroll_id}"

            response = requests.get(
                url,
                auth=HTTPBasicAuth(es_user, es_password)
            )
            response.raise_for_status()

            result = response.json()
            scroll_id = result.get('_scroll_id')
            hits = result['hits']['hits']
            if not hits:
                break

            data.extend(hits)
        except requests.RequestException as e:
            print(f"Error al recuperar datos del índice '{index}': {e}")
            return None

    # Guardar datos en formato JSON
    json_file = f"json/{index}.json"
    try:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump([doc['_source'] for doc in data], f, ensure_ascii=False, indent=4)
        print(f"Datos del índice '{index}' guardados en '{json_file}'.")
    except IOError as e:
        print(f"Error al guardar el archivo JSON para el índice '{index}': {e}")

# Ejecutar script
if __name__ == "__main__":
    try:
        es_indices = get_indices_from_elasticsearch()

        for index in es_indices:
            try:
                fetch_data_from_elasticsearch(index)
            except Exception as e:
                print(f"Error crítico durante el procesamiento del índice '{index}': {e}")

        print("Proceso completado.")
    except Exception as e:
        print(f"Error crítico durante la ejecución del script: {e}")
