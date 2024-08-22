# ETL Script for Elasticsearch

## Descripción

Este script realiza un proceso de ETL (Extracción, Transformación y Carga) desde un servidor de Elasticsearch, extrayendo datos de todos los índices disponibles y guardándolos en archivos JSON. El script también solicita credenciales de usuario y contraseña para autenticarse contra Elasticsearch.

## Requisitos

- **Python 3.x**: El script está escrito en Python 3
- **Dependencias**: El script utiliza las siguientes bibliotecas de Python:

  - `requests`: Librería externa para realizar solicitudes HTTP.
  - `getpass`: Librería estándar de python para solicitar contraseñas de manera segura.
  - `logging`: Librería estándar de python para registrar la actividad del script.
  - `os`: Módulo estándar de Python para interactuar con el sistema operativo.
  - `json`: Módulo estándar para manejar archivos y datos en formato JSON.
  - `sys`: Módulo estándar para interactuar con el sistema operativo, utilizado aquí para finalizar el script.

Se pueden instalar las dependencias utilizando el siguiente comando:

    ```
    pip install -r requirements.txt
    ```

## Uso

### Ejecución del Script

1. **Clonar o descargar el script**: Asegúrate de tener el script en tu máquina local.

2. **Instalar las dependencias**: Ejecuta el siguiente comando en la terminal para instalar las dependencias necesarias:

   ```
   pip install -r requirements.txt
   ```

3. **Cargar la dirección y puerto de la bbdd**: Es necesario completar en la variable "es_host" la dirección de la bbdd elastic junto con el puerto.

4. **Ejecutar el script**: Para ejecutar el script, simplemente haz doble clic en el archivo .exe generado (en el caso de que se haya instalado pyinstaller), o ejecuta el archivo .py desde la terminal:

   ```
   python etlElastic.py
   ```

Opcionalmente se puede instalar pyinstaller para generar un archivo .exe y luego ejecutar directamente el .exe generado que se guarda en la carpeta **dist**:

    ```
    pip install pyinstaller
    ```

    ```
    pyinstaller --onefile etlElastic.py
    ```

5. **Ingresar credenciales**: Al ejecutar el script, se solicitará ingresar el nombre de usuario y la contraseña. La contraseña no se mostrará mientras se escribe.

6. **Proceso de ETL**: El script se conectará a Elasticsearch utilizando las credenciales proporcionadas, obtendrá una lista de todos los índices disponibles y extraerá los datos de cada índice en archivos JSON individuales.

### Archivos Generados

`json/<index_name>.json`: Para cada índice en Elasticsearch, se generará un archivo JSON con los datos extraídos y se guardan en una carpeta llamada `json`.

`etl_process.log`: Un archivo de log donde se registra toda la actividad del script, incluidos errores y el resultado del proceso ETL.

### Manejo de Errores

- Si el usuario ingresa credenciales incorrectas, el script finalizará con un mensaje de error.
- Si se produce un error durante la extracción de datos de un índice específico, se registrará en el archivo de log y el script continuará con los demás índices.

### Personalización

- **Tamaño de lote**: Se puede ajustar el tamaño de lote (batch_size) para controlar cuántos documentos se extraen por solicitud.
- **Logging**: La configuración de logging se puede modificar para cambiar el formato de los mensajes o la ubicación del archivo de log.

## Explicación de las Funciones del Script

### `validate_user_credentials(es_host, input_user, input_password)`

- **Descripción**: Esta función valida las credenciales del usuario (nombre de usuario y contraseña) contra el servidor de Elasticsearch especificado. Se realiza una solicitud GET al endpoint raíz de Elasticsearch utilizando las credenciales proporcionadas.

- **Parámetros**:

  - `es_host`: La URL del host de Elasticsearch.
  - `input_user`: El nombre de usuario ingresado por el usuario.
  - `input_password`: La contraseña ingresada por el usuario.

- **Retorno**:
  - `True` si las credenciales son válidas (es decir, si la respuesta del servidor es 200 OK).
  - `False` si las credenciales son incorrectas o si hay un problema al conectar con Elasticsearch.

### `get_indices_from_elasticsearch()`

- **Descripción**: Esta función obtiene una lista de todos los índices disponibles en el servidor de Elasticsearch. Utiliza las credenciales del usuario para autenticarse y realiza una solicitud GET al endpoint `/_cat/indices`.

- **Parámetros**: Ninguno. La función utiliza variables globales para la URL del host, el nombre de usuario y la contraseña.

- **Retorno**:
  - Una lista de nombres de índices disponibles en Elasticsearch.
  - Una lista vacía si ocurre un error durante la solicitud o si no se encuentran índices.

### `fetch_data_from_elasticsearch(index)`

- **Descripción**: Esta función extrae los datos de un índice específico en Elasticsearch. Realiza una solicitud GET al endpoint de búsqueda (`/_search`) del índice, limitando la cantidad de documentos extraídos por solicitud según el tamaño de lote (`batch_size`).

- **Parámetros**:

  - `index`: El nombre del índice del cual se extraerán los datos.

- **Retorno**:
  - `True` y `None` si la extracción de datos es exitosa.
  - `None` y una cadena de texto con la razón del fallo si la extracción falla o si el índice está vacío.

### `save_json(index, data)`

- **Descripción**: Esta función guarda los datos extraídos de un índice de Elasticsearch en un archivo JSON. El archivo se nombra según el índice y se guarda en la carpeta `json`.

- **Parámetros**:

  - `index`: El nombre del índice de Elasticsearch, utilizado para nombrar el archivo JSON.
  - `data`: Los datos extraídos del índice, que serán guardados en el archivo JSON.

- **Retorno**: Ninguno. La función registra un mensaje en el archivo de log indicando si la operación fue exitosa o si ocurrió un error al intentar guardar el archivo.

### `print_loading_animation()`

- **Descripción**: Esta función muestra una animación de carga simple en la consola, representada por puntos consecutivos. Es utilizada para dar feedback visual al usuario mientras el script realiza operaciones que pueden llevar tiempo.

- **Parámetros**: Ninguno.

- **Retorno**: Ninguno. La función simplemente imprime un punto en la consola sin saltar a la siguiente línea.

### `if __name__ == "__main__":`

- **Descripción**: Este bloque es el punto de entrada del script. Primero, solicita al usuario ingresar sus credenciales. Luego, valida las credenciales contra el servidor de Elasticsearch. Si las credenciales son correctas, el script procede a ejecutar el proceso ETL, extrayendo datos de todos los índices en Elasticsearch y guardándolos en archivos JSON. También maneja la animación de carga y el registro de resultados en el archivo de log.

- **Proceso**:
  1. Solicita y valida las credenciales.
  2. Obtiene la lista de índices de Elasticsearch.
  3. Para cada índice:
     - Extrae los datos.
     - Guarda los datos en un archivo JSON.
     - Muestra la animación de carga.
  4. Registra los resultados del proceso en la consola y en el archivo de log.

## Scripts Complementarios

### `etl1indice.py`

Este script extrae uno o varios índices de ElasticSearch los convierte en JSON, los cuales son guardados en la raiz de la carpeta. Se solicitan credenciales por consola y el/los nombre/s de los índices que se quieren obtener. En el caso de necesitar varios índices, se separan con coma. Se guarda un archivo log con los resultados. Es necesario completar en la variable "es_host" la dirección de la bbdd elastic junto con el puerto.

### `etlListado.py`

Este script muestra una lista con los nombres de los índices disponibles en el servidor. Solicita credenciales por consola. Se guarda un archivo log con los resultados. Es necesario completar en la variable "es_host" la dirección de la bbdd elastic junto con el puerto.

### Nota importante

En las primeras versiones del script, las credenciales, el host y el puerto se obtenian desde un archivo .env En la actualidad, se solicitan por consola.

Así mismo, originalmente el script no solo iba a generar los json, sino que iba a cargarlos en una base mySQL generando una transformación del origen (elastic) hacia el destino (mySQL). Finalmente esta idea se abandonó ya que la generación de los json era suficiente.
