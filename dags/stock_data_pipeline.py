from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import psycopg2
from datetime import datetime
from table_manager import TableManager
import time

# Função para extrair dados históricos e diários usando a API do Yahoo Finance
def extract_data(company_code):
    def fetch_data(ticker, start_date=None, end_date=None, period="1d"):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={period}"
        if start_date and end_date:
            url += f"&period1={int(pd.Timestamp(start_date).timestamp())}&period2={int(pd.Timestamp(end_date).timestamp())}"
        
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'chart' in data and 'result' in data['chart']:
                return data['chart']['result'][0]
        elif response.status_code == 429:
            print(f"Rate limit exceeded for {ticker}. Waiting 3 seconds before retrying...")
            time.sleep(3)  # Espera de 3 segundos antes de tentar novamente
            return fetch_data(ticker, start_date, end_date, period)
        else:
            raise Exception(f"Failed to fetch data for {ticker}, status code {response.status_code}")

    # Extrair dados históricos para 2022, 2023 e 2024
    historical_2022 = fetch_data(company_code, start_date="2022-01-01", end_date="2022-12-31")
    historical_2023 = fetch_data(company_code, start_date="2023-01-01", end_date="2023-12-31")
    historical_2024 = fetch_data(company_code, start_date="2024-01-01", end_date="2024-12-31")

    # Organizar os dados históricos em um DataFrame
    def build_dataframe(data):
        timestamps = data['timestamp']
        indicators = data['indicators']['quote'][0]
        df = pd.DataFrame({
            'Date': pd.to_datetime(timestamps, unit='s'),
            'Open': indicators['open'],
            'High': indicators['high'],
            'Low': indicators['low'],
            'Close': indicators['close'],
            'Volume': indicators['volume']
        })
        return df

    df_2022 = build_dataframe(historical_2022)
    df_2023 = build_dataframe(historical_2023)
    df_2024 = build_dataframe(historical_2024)

    # Concatenar os dados históricos
    df_historical = pd.concat([df_2022, df_2023, df_2024])
    df_historical.to_csv('/opt/airflow/staging/historical_data.csv', index=False)

    # Extraindo a cotação diária
    daily_data = fetch_data(company_code, period="1d")
    df_daily = build_dataframe(daily_data)
    df_daily.to_csv('/opt/airflow/staging/daily_data.csv', index=False)

# Função para transformar os dados
def transform_data():
    df_historical = pd.read_csv('/opt/airflow/staging/historical_data.csv')
    df_daily = pd.read_csv('/opt/airflow/staging/daily_data.csv')

    # Concatenar dados históricos e diários
    df = pd.concat([df_historical, df_daily])

    # Transformar e criar dimensões
    df['Date'] = pd.to_datetime(df['Date'])
    df['year'] = df['Date'].dt.year
    df['month'] = df['Date'].dt.month
    df['day'] = df['Date'].dt.day
    df['quarter'] = df['Date'].dt.quarter
    df['week'] = df['Date'].dt.isocalendar().week

    # Salvar dados prontos para carga
    df.to_csv('/opt/airflow/staging/transformed_data.csv', index=False)

def create_tables():
    table_manager = TableManager(
        host="postgres", 
        database="airflow", 
        user="airflow", 
        password="airflow", 
        port="5432"
    )
    table_manager.create_tables()
    table_manager.close()
    
# Função para carregar os dados no PostgreSQL
def load_data(company_code, company_name):
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    # Inserir dados no StockPriceFact e nas dimensões
    df = pd.read_csv('/opt/airflow/staging/transformed_data.csv')
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO CompanyDTO (company_code, company_name)
            VALUES (%s, %s)
            ON CONFLICT (company_code) DO NOTHING
        """, (company_code, company_name))
        
        cursor.execute("""
            INSERT INTO CalendarDTO (date, year, month, day, quarter, week)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING
        """, (row['Date'], row['year'], row['month'], row['day'], row['quarter'], row['week']))

        cursor.execute("""
            INSERT INTO StockPriceFact (company_code, date, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (company_code, date) DO NOTHING
        """, (company_code, row['Date'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))

    conn.commit()
    cursor.close()
    conn.close()

# Definir a DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

with DAG('b3_stock_etl', default_args=default_args, schedule_interval='@daily') as dag:

    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables
    )
  
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={'company_code': 'PETR4.SA'}  # Passando o company_code diretamente
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={'company_code': 'PETR4.SA'}  # Passando o company_code diretamente para o load_data
    )
    
    create_tables_task >> extract_task >> transform_task >> load_task
