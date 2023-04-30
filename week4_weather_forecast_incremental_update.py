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
    table  = context["params"]["table"]

    cur   = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    
    # origin table update
    sql  = f"CREATE TABLE IF NOT EXISTS {schema}.{table} (date date primary key, temp float, min_temp float, max_temp float, created_date timestamp default GETDATE() );"
    for line in lines:
        dt       = dt2date(line["dt"])
        temp     = line["temp"]["eve"]
        min_temp = line["temp"]["min"]
        max_temp = line["temp"]["max"]
        sql     += f"INSERT INTO {schema}.{table} VALUES ('{dt}', '{temp}', '{min_temp}', '{max_temp}');"
    try:
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # create temp table 
    sql  = f"""DROP TABLE IF EXISTS {schema}.temp_{table};
        CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS);
    """
    try:
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # alter from new to old
    sql  = f"""DELETE FROM {schema}.{table};
        INSERT INTO {schema}.{table} 
        SELECT date, temp, min_temp, max_temp
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date) AS seq 
            FROM {schema}.temp_{table}               
        )
        WHERE seq = 1;
    """
    try:
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

dag_second_assignment = DAG(
    dag_id = "weather_forecast_incremental_update",
    start_date = datetime(2023,4,10),
    schedule = "0 1 * * *",
    max_active_runs = 1,
    catchup = False, 
    default_args = {
        'retries':1,
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
    params          = {
        'schema': 'gotjd709',
        'table' : 'weather_forecast_inc'},
    dag             = dag_second_assignment
)

extract >> transform >> load