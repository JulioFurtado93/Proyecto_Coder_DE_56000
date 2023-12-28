from datetime import timedelta,datetime
from pathlib import Path
import json
import requests
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from airflow import DAG
import pandas as pd
import os

# Operadores
from airflow.operators.python_operator import PythonOperator

dag_path = os.getcwd()     #path original.. home en Docker

url='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
with open(dag_path+'/keys/'+"db.txt",'r') as f:
    data_base= f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    user= f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
    pwd= f.read()

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}

# argumentos por defecto para el DAG
default_args = {
    'owner': 'Furtado_Julio',
    'start_date': datetime(2023,12,11),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

OW_dag = DAG(
    dag_id='OpenWeather_ETL',
    default_args=default_args,
    description='Agrega data de Open Weather de forma diaria',
    schedule_interval="@daily",
    catchup=False
)

dag_path = os.getcwd()     #path original.. home en Docker

# funcion de extraccion de datos
def extraer_data(exec_date):
    try:
        print(f"Adquiriendo data para la fecha: {exec_date}")
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')
        city_name = 'Buenos Aires'
        country_code = 'AR'
        lang = 'es'
        units = 'metric'
        with open(dag_path+'/keys/'+'api_key.txt','r') as p:
            api_key = p.read()
        response = requests.get(
            f'https://api.openweathermap.org/data/2.5/weather?q={city_name},{country_code}&appid={api_key}&lang={lang}&units={units}')
        if response:
            print('Datos adquiridos!')
            data = response.json()
            with open(dag_path+'/raw_data/'+"dataOW_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "w") as json_file:
                json.dump(data, json_file)
        else:
            print('Occurió un error.') 
    except ValueError as e:
        print("Formato datetime deberia ser %Y-%m-%d %H", e)
        raise e       

# Funcion de transformacion en tabla
def transformar_data(exec_date):       
    print(f"Transformando la data para la fecha: {exec_date}") 
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/raw_data/'+"dataOW_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
        loaded_data=json.load(json_file)
    # Extraer la data en tabla
    df = pd.json_normalize(loaded_data)
    norm_weather_df = pd.json_normalize(df['weather'][0])
    all_weather_df = pd.concat([df, norm_weather_df], axis=1)
    
    #eliminacion de columnas sin uso
    reduced_weather_df = all_weather_df.drop(['base', 'timezone', 'id', 'cod', 'sys.type', 'sys.id', 'id', 'icon'], axis=1)
    
    #orden de columnas
    sorted_weather_df = reduced_weather_df[['dt','name','sys.country','coord.lon','coord.lat','main','description','main.temp','main.feels_like','main.temp_min','main.temp_max','main.pressure','main.humidity','wind.speed','wind.deg','clouds.all','sys.sunrise','sys.sunset']]

    #formato datetime de UNIX a GMT 0
    sorted_weather_df['dt'] = pd.to_datetime(sorted_weather_df['dt'], unit='s')
    sorted_weather_df['sys.sunrise'] = pd.to_datetime(sorted_weather_df['sys.sunrise'], unit='s')
    sorted_weather_df['sys.sunset'] = pd.to_datetime(sorted_weather_df['sys.sunset'], unit='s')

    #GMT -3 usando timedelta
    sorted_weather_df['dt'] = sorted_weather_df['dt'] - timedelta(hours=3)
    sorted_weather_df['sys.sunrise'] = sorted_weather_df['sys.sunrise'] - timedelta(hours=3)
    sorted_weather_df['sys.sunset'] = sorted_weather_df['sys.sunset'] - timedelta(hours=3)

    #renombrar columnas para exportar luego a Redshift
    sorted_weather_df.rename(columns = {'sys.country':'country','coord.lon':'longitude','coord.lat':'latitude','main.temp':'temp','main.feels_like':'feels_like','main.temp_min':'temp_min','main.temp_max':'temp_max','main.pressure':'pressure','main.humidity':'humidity','wind.speed':'wind_speed','wind.deg':'wind_deg','clouds.all':'clouds','sys.sunrise':'sunrise_time','sys.sunset':'sunset_time'}, inplace = True)
    sorted_weather_df.to_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv", index=False, mode='a')

# Funcion conexion a redshift
def conexion_redshift(exec_date):
    print(f"Conectandose a la DB en la fecha: {exec_date}") 
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439')
        print(conn)
        print("Conexion a RedShift exitosa!")
    except Exception as e:
        print("No se pudo conectar a RedShift.")
        print(e)

# Funcion de envio de data
def cargar_data(exec_date):
    print(f"Cargando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    dataframe=pd.read_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv")
    print(dataframe.shape)
    print(dataframe.head())
    # conexion a database
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    conn = psycopg2.connect(
        host=url,
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')
    table_name = 'open_weather_v3'
    
    #almacena los tipos de datos del df
    dtypes= dataframe.dtypes

    #las columnas del df
    columnas= list(dtypes.index )

    #los valores dentro de los tipos de dato
    tipos_pre= list(dtypes.values)

    #mapa de tipos para redshift
    mapa = {
        'int64': 'INT',
        'float64': 'FLOAT',
        'object': 'VARCHAR(50)',
        'datetime64[ns]': 'TIMESTAMP'
        }
    
    #itera para asignarle un valor del mapa a cada dtype de esa variable
    sql_tipos = [mapa[str(dtype)] for dtype in tipos_pre]

    #arma los nombres de las columnas en sql para poder armar la sentencia de creacion
    columnas_redshift = [f"{name} {data_type}" for name, data_type in zip(columnas, sql_tipos)]

    #schema por si no existiese la tabla
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columnas_redshift)}
        );
        """
    
    #crea la tabla si no existe
    cur = conn.cursor()
    cur.execute(table_schema)

    #guarda los valores en tuplas para insertar
    values = [tuple(x) for x in dataframe.to_numpy()]

    #define el insert usando sql de psycopg2 (itera con las columnas tambien)
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columnas))
    )

    #execute values ejecuta la inserción en la tabla
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Carga completada en Redshift!')

# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=OW_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=OW_dag,
)

# 3. Conexion a base de datos
task_3= PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=OW_dag,
)

# 4. Envio final
task_4 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=OW_dag,
)

# Definicion orden de tareas
task_1 >> task_2 >> task_3 >> task_4
