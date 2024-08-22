"""
Este script muestra una lista con los nombres de los indices disponibles en el servidor.
Solicita credenciales por consola.
Se guarda un archivo log con los resultados.

Completar en línea 28 "es_host" la dirección de la bbdd elastic junto con el puerto.

"""

import requests
from requests.auth import HTTPBasicAuth
import logging
import getpass
import sys

# Configurar logging con formato UTF-8
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("list_indices.log", encoding='utf-8')]
)

# Solicitar al usuario ingresar usuario y contraseña
input_user = input("Ingrese el usuario: ")
input_password = getpass.getpass("Ingrese la contraseña: ")  # Ocultar la entrada de la contraseña

# Configuración para ElasticSearch
es_host = "http://TU_SERVIDOR:9200"

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
def get_indices_from_elasticsearch(es_host, input_user, input_password):
    try:
        response = requests.get(
            f"{es_host}/_cat/indices?format=json",
            auth=HTTPBasicAuth(input_user, input_password)
        )
        response.raise_for_status()
        indices = response.json()
        return [index['index'] for index in indices]
    except requests.RequestException as e:
        logging.error(f"Error al obtener la lista de índices de Elasticsearch: {e}")
        return []

# Ejecutar script
if __name__ == "__main__":
    try:
        # Obtener y ordenar los índices alfabéticamente
        index_names = sorted(get_indices_from_elasticsearch(es_host, input_user, input_password))

        if index_names:
            print("\nListado de nombres de los índices (ordenado alfabéticamente):")
            for name in index_names:
                print(name)
                logging.info(f"Índice encontrado: {name}")

            # Mostrar la cantidad total de índices
            print(f"\nCantidad total de índices: {len(index_names)}")
            logging.info(f"Cantidad total de índices: {len(index_names)}")
        else:
            print("No se encontraron índices en Elasticsearch.")
            logging.warning("No se encontraron índices en Elasticsearch.")

    except Exception as e:
        logging.critical(f"Error crítico durante la ejecución del script: {e}")
        sys.exit(1)
