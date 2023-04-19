from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python    import PythonOperator
from airflow.models              import Variable
from airflow                     import DAG

from datetime                    import timedelta, datetime

import requests
import logging
import psycopg2

def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def dt2date(dt):
    return datetime.fromtimestamp(dt).strftime("%Y-%m-%d")

def extract(**context):
    lat            = context["params"]["lat"]
    lon            = context["params"]["lon"]
    key            = context["params"]["key"]
    link           = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={key}&units=metric"
    task_instance  = context["task_instance"]
    execution_date = context["execution_date"]

    logging.info(execution_date)
    f = requests.get(link)
    print(f.json())
    return f.json()

def transform(**context):
    json  = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = json["daily"]
    return lines

def load(**context):
    schema = context["params"]["schema"]

    cur   = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    sql   = "BEGIN;"
    sql  += f"DROP TABLE IF EXISTS {schema}.weather_forecast;" 
    sql  += f"CREATE TABLE {schema}.weather_forecast (date date primary key, temp float, min_temp float, max_temp float, created_date timestamp default GETDATE() );"
    for line in lines:
        dt       = dt2date(line["dt"])
        temp     = line["temp"]["eve"]
        min_temp = line["temp"]["min"]
        max_temp = line["temp"]["max"]
        sql     += f"INSERT INTO {schema}.weather_forecast VALUES ('{dt}', '{temp}', '{min_temp}', '{max_temp}');"
    sql  += "END;"
    cur.execute(sql)

dag_second_assignment = DAG(
    dag_id = "weather_forecast_full_refresh",
    start_date = datetime(2023,4,10),
    schedule = "0 1 * * *",
    max_active_runs = 1,
    catchup = True, 
    default_args = {
        'retries':1,
        'retry_delay': timedelta(minutes=1),
    }
)

extract = PythonOperator(
    task_id         = 'extract',
    python_callable = extract,
    params          = {
        'lat': Variable.get("target_latitude"),
        'lon': Variable.get("target_longitude"),
        'key': Variable.get("open_weather_api_key"),
    },
    dag             = dag_second_assignment 
)

transform = PythonOperator(
    task_id         = 'transform',
    python_callable = transform,
    params          = {},
    dag             = dag_second_assignment

)

load = PythonOperator(
    task_id         = 'load',
    python_callable = load,
    params          = {'schema': 'gotjd709'},
    dag             = dag_second_assignment
)

extract >> transform >> load