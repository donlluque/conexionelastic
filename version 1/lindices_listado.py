"""
Este script muestra una lista con los nombres de los
indices disponibles en el servidor.

"""

import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Configuración para ElasticSearch
es_host = os.getenv('ES_HOST')
es_user = os.getenv('ES_USER')
es_password = os.getenv('ES_PASSWORD')

# Solicitud directa a Elasticsearch
response = requests.get(
    f"{es_host}/_cat/indices?format=json",
    auth=HTTPBasicAuth(es_user, es_password)
)

# Verifica si la solicitud fue exitosa y muestra la respuesta
if response.status_code == 200:
    print("Conexión exitosa a Elasticsearch")
    
    indices = response.json()
    
    # Obtener nombres de los índices y ordenarlos alfabéticamente
    index_names = sorted([index['index'] for index in indices])
    
    # Mostrar nombres de los índices
    print("\nListado de nombres de los índices (ordenado alfabéticamente):")
    for name in index_names:
        print(name)
    
    # Mostrar la cantidad de índices
    print(f"\nCantidad total de índices: {len(index_names)}")
else:
    print(f"Error al conectarse a Elasticsearch: {response.status_code} - {response.text}")
