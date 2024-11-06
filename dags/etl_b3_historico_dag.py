from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.etl_b3 import processar_dados_historicos
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG('etl_b3_historico', default_args=default_args, schedule_interval='@daily', start_date=datetime(2023, 1, 1)) as dag:
    
    task_processar_dados_2022 = PythonOperator(
        task_id='processar_dados_2022',
        python_callable=processar_dados_historicos,
        op_args=["data/raw/COTAHIST_A2022.TXT", "historico_2022"]
    )
    
    task_processar_dados_2023 = PythonOperator(
        task_id='processar_dados_2023',
        python_callable=processar_dados_historicos,
        op_args=["data/raw/COTAHIST_A2023.TXT", "historico_2023"]
    )
    
    task_processar_dados_2022 >> task_processar_dados_2023
