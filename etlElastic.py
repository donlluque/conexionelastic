"""
Este script extrae el contenido de los índices de ElasticSearch y los convierte en JSON, 
los cuales son guardados en la carpeta "json".
Se solicitan credenciales por consola.

Completar en línea 32 "es_host" la dirección de la bbdd elastic junto con el puerto.

"""


import os  # Módulo estándar de Python para interactuar con el sistema operativo.
import json  # Módulo estándar para manejar archivos y datos en formato JSON.
import requests  # Librería externa para realizar solicitudes HTTP.
from requests.auth import HTTPBasicAuth  # Clase de la librería requests para manejar autenticación básica HTTP.
import logging  # Módulo estándar para registrar mensajes de log.
import sys  # Módulo estándar para interactuar con el sistema operativo, utilizado aquí para finalizar el script.
import getpass  # Módulo estándar para solicitar contraseñas de manera segura (sin que se vean en pantalla).

# Configurar logging para que los mensajes se guarden en un archivo y se muestren en formato específico.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("etlElastic_process.log", encoding='utf-8')]  # Guardar logs en 'etl_process.log'
)

# Solicitar al usuario ingresar el nombre de usuario y la contraseña.
# La contraseña se oculta mientras se escribe.
input_user = input("Ingrese el usuario: ")
input_password = getpass.getpass("Ingrese la contraseña: ")

# Configuración del host de ElasticSearch y tamaño de lote para las solicitudes.
es_host = "http://TU_SERVIDOR:9200"
batch_size = 1000  # Tamaño de lote por defecto.

# Función para validar las credenciales del usuario contra el servidor ElasticSearch.
def validate_user_credentials(es_host, input_user, input_password):
    try:
        # Realiza una solicitud GET al host de ElasticSearch con las credenciales proporcionadas.
        response = requests.get(
            es_host,
            auth=HTTPBasicAuth(input_user, input_password)
        )
        # Si la respuesta es 200 (OK), las credenciales son válidas.
        if response.status_code == 200:
            return True
        else:
            return False
    except requests.exceptions.RequestException as e:
        # Captura y registra cualquier error durante la conexión a ElasticSearch.
        logging.error(f"Error al conectar con ElasticSearch: {e}")
        return False

# Validar las credenciales del usuario. Si son incorrectas, se termina la ejecución del script.
if not validate_user_credentials(es_host, input_user, input_password):
    print("Usuario o contraseña incorrectos.")
    sys.exit(1)

# Crear directorios para guardar los archivos JSON, si no existen.
os.makedirs('json', exist_ok=True)

# Función para obtener la lista de índices de ElasticSearch.
def get_indices_from_elasticsearch():
    try:
        # Realiza una solicitud GET para obtener todos los índices en formato JSON.
        response = requests.get(
            f"{es_host}/_cat/indices?format=json",
            auth=HTTPBasicAuth(input_user, input_password)
        )
        response.raise_for_status()  # Verifica si la solicitud fue exitosa.
        # Retorna una lista de nombres de índices.
        return [index['index'] for index in response.json()]
    except requests.exceptions.RequestException as e:
        # Captura y registra cualquier error durante la obtención de los índices.
        logging.error(f"Error al obtener los índices de ElasticSearch: {e}")
        return []

# Función para extraer datos de un índice específico en ElasticSearch.
def fetch_data_from_elasticsearch(index):
    try:
        # Realiza una solicitud GET para obtener los datos del índice especificado.
        response = requests.get(
            f"{es_host}/{index}/_search?size={batch_size}",
            auth=HTTPBasicAuth(input_user, input_password)
        )
        response.raise_for_status()  # Verifica si la solicitud fue exitosa.
        data = response.json()['hits']['hits']  # Extrae los datos reales del JSON.

        if not data:
            reason = f"Índice '{index}' vacío."
            return None, reason  # Retorna si el índice está vacío.

        # Guardar los datos en un archivo JSON.
        save_json(index, data)
        return True, None

    except requests.exceptions.RequestException as e:
        # Captura y registra cualquier error durante la extracción de datos.
        reason = f"Error al obtener datos del índice '{index}': {e}"
        return None, reason

# Función para guardar los datos extraídos en un archivo JSON.
def save_json(index, data):
    try:
        json_file = f"json/{index}.json"  # Define el nombre del archivo JSON.
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump([doc['_source'] for doc in data], f, ensure_ascii=False, indent=4)
        # Registro de que el archivo se guardó correctamente.
        logging.info(f"Datos del índice '{index}' guardados en '{json_file}'.")
    except IOError as e:
        # Captura y registra cualquier error durante la escritura del archivo.
        logging.error(f"Error al guardar los datos del índice '{index}' en JSON: {e}")

# Función para mostrar una animación simple de carga en la consola (puntos consecutivos).
def print_loading_animation():
    sys.stdout.write('.')
    sys.stdout.flush()

# Punto de entrada del script. Ejecuta el proceso ETL.
if __name__ == "__main__":
    try:
        # Obtener la lista de índices desde ElasticSearch.
        es_indices = get_indices_from_elasticsearch()

        # Si no se encontraron índices, termina la ejecución del script.
        if not es_indices:
            logging.error("No se encontraron índices en ElasticSearch.")
            exit(1)

        success_count = 0  # Contador de operaciones exitosas.
        fail_count = 0  # Contador de operaciones fallidas.
        failed_indices = []  # Lista para almacenar índices que fallaron.

        # Iterar sobre cada índice para extraer y guardar sus datos.
        for index in es_indices:
            result, reason = fetch_data_from_elasticsearch(index)
            if result:
                success_count += 1
            else:
                fail_count += 1
                failed_indices.append((index, reason))

            # Mostrar animación con puntos consecutivos.
            print_loading_animation()

        # Mostrar resultados finales en la consola.
        sys.stdout.write(f"\nProceso completado: {success_count} éxitos, {fail_count} fallos.\n")
        if failed_indices:
            for index, reason in failed_indices:
                sys.stdout.write(f"Fallo en índice '{index}': {reason}\n")

        # Registrar los resultados finales en el archivo de log.
        logging.info(f"Proceso completado: {success_count} éxitos, {fail_count} fallos.")
        for index, reason in failed_indices:
            logging.error(f"Fallo en índice '{index}': {reason}")

    except Exception as e:
        # Captura y registra cualquier error crítico que ocurra durante la ejecución.
        logging.critical(f"Error crítico durante la ejecución del ETL: {e}")
        exit(1)
