"""
Este script extrae uno o varios índices de ElasticSearch 
los convierte en JSON, los guarda en la raiz 
y luego los carga en una base de datos MySQL.

En la línea 40 se cargan los nombres de los índices

"""

import os
import json
import requests
from requests.auth import HTTPBasicAuth
import pymysql
import pandas as pd
from dotenv import load_dotenv
import sys
import numpy as np

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Configuración para ElasticSearch
es_host = os.getenv('ES_HOST')
es_user = os.getenv('ES_USER')
es_password = os.getenv('ES_PASSWORD')

# Configuración para MySQL
mysql_host = os.getenv('MYSQL_HOST')
mysql_user = os.getenv('MYSQL_USER')
mysql_password = os.getenv('MYSQL_PASSWORD')
mysql_database = os.getenv('MYSQL_DATABASE')

# Tamaño del lote para la carga de datos
batch_size = int(os.getenv('BATCH_SIZE', 1000))  # Valor por defecto si no está en .env

# Crear directorios si no existen
os.makedirs('json', exist_ok=True)

# Índices a procesar (definidos directamente en el código)
indices_to_process = [
    'lml_documentstemplate_mesa4core',  # Reemplaza 'index1' con el nombre real de tu índice
    
    # Agrega más índices según sea necesario
]

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
    json_file = f"{index}.json"
    try:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump([doc['_source'] for doc in data], f, ensure_ascii=False, indent=4)
        print(f"Datos del índice '{index}' guardados en '{json_file}'.")
    except IOError as e:
        print(f"Error al guardar el archivo JSON para el índice '{index}': {e}")

# Ejecutar script
if __name__ == "__main__":
    try:
        conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_database}")
        conn.close()
        print(f"Base de datos '{mysql_database}' verificada/creada en MySQL")

        es_indices = get_indices_from_elasticsearch()

        for index in indices_to_process:
            if index in es_indices:
                print(f"Índice '{index}' encontrado en Elasticsearch.")
                fetch_data_from_elasticsearch(index)
            else:
                print(f"Índice '{index}' no encontrado en Elasticsearch.")
        
        print("Proceso completado.")
    except Exception as e:
        print(f"Error crítico durante la ejecución del script: {e}")
        sys.exit(1)
