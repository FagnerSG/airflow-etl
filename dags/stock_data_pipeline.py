from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import psycopg2

# Função para buscar os dados de ações e inserir no PostgreSQL
def fetch_and_store_stock_data():
    ticker = 'PETR4.SA'  # Empresa da B3
    data = yf.download(ticker, period='1d')

    # Conexão com o banco de dados PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="stock_data",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Criar tabela caso não exista
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices (
        date DATE PRIMARY KEY,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT
    );
    """)

    # Inserir dados no banco
    for index, row in data.iterrows():
        cur.execute("""
        INSERT INTO stock_prices (date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING;
        """, (index.date(), row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))

    conn.commit()
    cur.close()
    conn.close()

# Configurações do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Criação do DAG
with DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Pipeline para buscar dados diários de ações da B3 e armazenar no PostgreSQL',
    schedule_interval='@daily',
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_and_store_stock_data',
        python_callable=fetch_and_store_stock_data
    )

fetch_data_task
