"""
Este script extrae uno o varios índices de ElasticSearch los convierte en JSON, 
los cuales son guardados en la raiz de la carpeta.
Se solicitan credenciales por consola y el/los nombre/s de los índices 
que se quieren obtener. En el caso de necesitarvarios índices, se separan con coma.
Se guarda un archivo log con los resultados.

Completar en línea 32"es_host" la dirección de la bbdd elastic junto con el puerto.

"""


import json
import requests
from requests.auth import HTTPBasicAuth
import logging
import sys
import getpass

# Configurar logging con formato UTF-8
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("etl1indice_process.log", encoding='utf-8')]
)

# Solicitar al usuario ingresar usuario y contraseña
input_user = input("Ingrese el usuario: ")
input_password = getpass.getpass("Ingrese la contraseña: ")  # Ocultar la entrada de la contraseña

# Configuración para ElasticSearch
es_host = "http://TU_SERVIDOR:9200"
batch_size = 1000

# Verificar si el usuario y la contraseña ingresados son válidos contra el host de ElasticSearch
def validate_user_credentials(es_host, input_user, input_password):
    try:
        # Realiza una solicitud GET al endpoint raíz de Elasticsearch
        response = requests.get(
            es_host,
            auth=HTTPBasicAuth(input_user, input_password)
        )
        # Si la respuesta es 200 OK, las credenciales son válidas
        if response.status_code == 200:
            return True
        else:
            return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al conectar con ElasticSearch: {e}")
        return False

# Validar las credenciales del usuario
if not validate_user_credentials(es_host, input_user, input_password):
    print("Usuario o contraseña incorrectos.")
    sys.exit(1)

# Obtener lista de índices de ElasticSearch
def get_indices_from_elasticsearch():
    try:
        response = requests.get(
            f"{es_host}/_cat/indices?format=json",
            auth=HTTPBasicAuth(input_user, input_password)
        )
        response.raise_for_status()
        indices = [index['index'] for index in response.json()]
        return indices
    except requests.RequestException as e:
        logging.error(f"Error al obtener la lista de índices de Elasticsearch: {e}")
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
                auth=HTTPBasicAuth(input_user, input_password)
            )
            response.raise_for_status()

            result = response.json()
            scroll_id = result.get('_scroll_id')
            hits = result['hits']['hits']
            if not hits:
                break

            data.extend(hits)
        except requests.RequestException as e:
            logging.error(f"Error al recuperar datos del índice '{index}': {e}")
            return

    # Guardar datos en formato JSON
    json_file = f"{index}.json"
    try:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump([doc['_source'] for doc in data], f, ensure_ascii=False, indent=4)
        print(f"Datos del índice '{index}' guardados en '{json_file}'.")
        logging.info(f"Datos del índice '{index}' guardados en '{json_file}'.")
    except IOError as e:
        logging.error(f"Error al guardar el archivo JSON para el índice '{index}': {e}")

# Ejecutar script
if __name__ == "__main__":
    try:
        es_indices = get_indices_from_elasticsearch()
        if not es_indices:
            print("No se encontraron índices en Elasticsearch.")
            logging.error("No se encontraron índices en Elasticsearch.")
            sys.exit(1)

        # Solicitar al usuario los índices a procesar
        indices_to_process = input("Ingrese el nombre de los índices a procesar, separados por comas: ").split(',')

        for index in indices_to_process:
            index = index.strip()  # Eliminar espacios en blanco
            if index in es_indices:
                print(f"Índice '{index}' encontrado en Elasticsearch.")
                logging.info(f"Índice '{index}' encontrado en Elasticsearch.")
                fetch_data_from_elasticsearch(index)
            else:
                print(f"Índice '{index}' no encontrado en Elasticsearch.")
                logging.warning(f"Índice '{index}' no encontrado en Elasticsearch.")

        print("Proceso completado.")
        logging.info("Proceso completado.")
    except Exception as e:
        logging.critical(f"Error crítico durante la ejecución del script: {e}")
        sys.exit(1)
