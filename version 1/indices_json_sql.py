"""
Este script extrae los datos de TODOS los índices de ElasticSearch 
los convierte en JSON y guarda en una carpeta "json" 
y luego los carga en una base de datos MySQL.

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
batch_size = int(os.getenv('BATCH_SIZE'))

# Crear directorios si no existen
os.makedirs('json', exist_ok=True)

# Variables para contar el éxito y fallo de los procesos
success_count = 0
fail_count = 0

# Obtener lista de índices de ElasticSearch
def get_indices_from_elasticsearch():
    response = requests.get(
        f"{es_host}/_cat/indices?format=json",
        auth=HTTPBasicAuth(es_user, es_password)
    )
    response.raise_for_status()
    indices = [index['index'] for index in response.json()]
    return indices

# Conectar a ElasticSearch y extraer datos
def fetch_data_from_elasticsearch(index):
    response = requests.get(
        f"{es_host}/{index}/_search?size={batch_size}",
        auth=HTTPBasicAuth(es_user, es_password)
    )
    response.raise_for_status()
    data = response.json()['hits']['hits']

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame([doc['_source'] for doc in data])

    # Agregar el campo 'id' desde Elasticsearch si no está presente
    if 'id' not in df.columns:
        df['id'] = [doc['_id'] for doc in data]

    # Aplanar columnas con diccionarios anidados
    df = flatten_columns(df)

    # Guardar datos en formato JSON
    json_file = f"json/{index}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump([doc['_source'] for doc in data], f, ensure_ascii=False, indent=4)
    
    return df

# Función para aplanar las columnas que son diccionarios
def flatten_columns(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            df[col] = df[col].apply(json.dumps)
    return df

# Crear tablas en MySQL y cargar datos
def load_data_to_mysql(index, df):
    try:
        conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
        cursor = conn.cursor()

        columns = []
        for column_name, dtype in df.dtypes.items():
            if column_name == 'id':
                col_type = "VARCHAR(255) PRIMARY KEY"
            elif "int" in str(dtype):
                col_type = "INT"
            elif "float" in str(dtype):
                col_type = "FLOAT"
            else:
                col_type = "LONGTEXT"
            columns.append(f"`{column_name}` {col_type}")
        
        columns_sql = ", ".join(columns)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{index}` (
            {columns_sql}
        );
        """
        
        cursor.execute(create_table_sql)
        conn.commit()

        df = df.replace({np.nan: None})

        for start in range(0, len(df), batch_size):
            batch_df = df.iloc[start:start+batch_size]
            for _, row in batch_df.iterrows():
                row = row.apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

                columns = ", ".join([f"`{col}`" for col in row.index])
                values = ", ".join(['%s'] * len(row))
                update_sql = ", ".join([f"`{col}`=VALUES(`{col}`)" for col in row.index])
                insert_sql = f"""
                INSERT INTO `{index}` ({columns}) VALUES ({values})
                ON DUPLICATE KEY UPDATE {update_sql}
                """
                
                cursor.execute(insert_sql, tuple(row.values))

            conn.commit()
        
        conn.close()
        return True
    except pymysql.MySQLError as e:
        print(f"Error al cargar los datos en MySQL para el índice '{index}': {e}")
        return False

# Ejecutar ETL
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

        for index in es_indices:
            try:
                df = fetch_data_from_elasticsearch(index)
                if not df.empty:
                    loaded_to_mysql = load_data_to_mysql(index, df)
                    if loaded_to_mysql:
                        print(f"Proceso para el índice '{index}': OK")
                        success_count += 1
                    else:
                        print(f"Proceso para el índice '{index}': FAIL")
                        fail_count += 1
            except Exception as e:
                print(f"Error crítico durante el procesamiento del índice '{index}': {e}")
                fail_count += 1
        
        print(f"Proceso ETL completado: {success_count} OK, {fail_count} FAIL.")
    except Exception as e:
        print(f"Error crítico durante la ejecución del ETL: {e}")
        sys.exit(1)
