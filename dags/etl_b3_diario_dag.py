from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.etl_b3 import processar_dados_diarios
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG('etl_b3_diario', default_args=default_args, schedule_interval='@daily', start_date=datetime(2024, 1, 1)) as dag:
    
    task_processar_dados_diarios = PythonOperator(
        task_id='processar_dados_2024',
        python_callable=processar_dados_diarios,
        op_args=["data/raw/COTAHIST_A2024.TXT"]
    )
    
    task_processar_dados_diarios
