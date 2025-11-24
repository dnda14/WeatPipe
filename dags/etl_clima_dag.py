import sys
sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.extract import extraer_datos
from src.transform import transformar_datos
from src.load import cargar2

CIUDADES = ["Arequipa,PE", "Lima,PE"]

def run_etl_ciudad(ciudad): 
    data = extraer_datos(ciudad)
    print(data)
    if data:
        clean_data = transformar_datos(data)
        cargar2(clean_data)
        
with DAG(
    dag_id="etl_clima_dag",
    start_date=datetime(2025,11,16),
    schedule_interval = "*/10 * * * *",
    catchup = False,
    
) as dag:
    tareas = []
    for ciudad in CIUDADES:
        tarea = PythonOperator(
            task_id = f"etl_{ciudad.replace(',','_')}",
            python_callable=run_etl_ciudad,
            op_kwargs={"ciudad": ciudad}
        )
    
        tareas.append(tarea)
        