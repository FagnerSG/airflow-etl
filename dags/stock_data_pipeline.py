#from airflow import DAG
#from airflow.operators.python import PythonOperator
#from airflow.utils.dates import days_ago
#import requests
#import pandas as pd
#import psycopg2
#from datetime import datetime
#from table_manager import TableManager
#import time
#
## Função para extrair a cotação diária usando a API do Yahoo Finance
#def extract_data(company_code):
#    def fetch_data(ticker, period="1d", retries=3):
#        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={period}"
#        attempt = 0
#        backoff_time = 60  # Começa com 60 segundos
#
#        while attempt < retries:
#            response = requests.get(url)
#            if response.status_code == 200:
#                data = response.json()
#                if 'chart' in data and 'result' in data['chart']:
#                    return data['chart']['result'][0]
#            elif response.status_code == 429:
#                print(f"Rate limit exceeded for {ticker}. Waiting {backoff_time} seconds before retrying... (Attempt {attempt + 1}/{retries})")
#                time.sleep(backoff_time)
#                backoff_time *= 2  # Dobra o tempo de espera a cada tentativa
#                attempt += 1
#            else:
#                raise Exception(f"Failed to fetch data for {ticker}, status code {response.status_code}")
#        
#        raise Exception(f"Rate limit exceeded for {ticker} after {retries} retries.")
#
#    # Extraindo a cotação diária
#    daily_data = fetch_data(company_code, period="1d")
#    
#    # Organizar os dados diários em um DataFrame
#    timestamps = daily_data['timestamp']
#    indicators = daily_data['indicators']['quote'][0]
#    df_daily = pd.DataFrame({
#        'Date': pd.to_datetime(timestamps, unit='s'),
#        'Open': indicators['open'],
#        'High': indicators['high'],
#        'Low': indicators['low'],
#        'Close': indicators['close'],
#        'Volume': indicators['volume']
#    })
#
#    # Salvando o arquivo em "stage"
#    df_daily.to_csv('/opt/airflow/staging/daily_data.csv', index=False)
#
## Função para transformar os dados
#def transform_data():
#    df_daily = pd.read_csv('/opt/airflow/staging/daily_data.csv')
#
#    # Transformar e criar dimensões
#    df_daily['Date'] = pd.to_datetime(df_daily['Date'])
#    df_daily['year'] = df_daily['Date'].dt.year
#    df_daily['month'] = df_daily['Date'].dt.month
#    df_daily['day'] = df_daily['Date'].dt.day
#    df_daily['quarter'] = df_daily['Date'].dt.quarter
#    df_daily['week'] = df_daily['Date'].dt.isocalendar().week
#
#    # Salvando os dados transformados
#    df_daily.to_csv('/opt/airflow/staging/transformed_data.csv', index=False)
#
## Função para criar tabelas no PostgreSQL
#def create_tables():
#    table_manager = TableManager(
#        host="postgres", 
#        database="airflow", 
#        user="airflow", 
#        password="airflow", 
#        port="5432"
#    )
#    table_manager.create_tables()
#    table_manager.close()
#
## Função para carregar os dados no PostgreSQL
#def load_data(company_code, company_name):
#    conn = psycopg2.connect(
#        host="postgres",
#        database="airflow",
#        user="airflow",
#        password="airflow"
#    )
#    cursor = conn.cursor()
#
#    # Inserir dados no StockPriceFact e nas dimensões
#    df = pd.read_csv('/opt/airflow/staging/transformed_data.csv')
#    for _, row in df.iterrows():
#        # Inserir a empresa na dimensão CompanyDTO
#        cursor.execute("""
#            INSERT INTO CompanyDTO (company_code, company_name)
#            VALUES (%s, %s)
#            ON CONFLICT (company_code) DO NOTHING
#        """, (company_code, company_name))
#        
#        # Inserir a data na dimensão CalendarDTO
#        cursor.execute("""
#            INSERT INTO CalendarDTO (date, year, month, day, quarter, week)
#            VALUES (%s, %s, %s, %s, %s, %s)
#            ON CONFLICT (date) DO NOTHING
#        """, (row['Date'], row['year'], row['month'], row['day'], row['quarter'], row['week']))
#
#        # Inserir os dados no fato StockPriceFact
#        cursor.execute("""
#            INSERT INTO StockPriceFact (company_code, date, open_price, high_price, low_price, close_price, volume)
#            VALUES (%s, %s, %s, %s, %s, %s, %s)
#            ON CONFLICT (company_code, date) DO NOTHING
#        """, (company_code, row['Date'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))
#
#    conn.commit()
#    cursor.close()
#    conn.close()
#
## Definir a DAG
#default_args = {
#    'owner': 'airflow',
#    'start_date': days_ago(1),
#    'retries': 1
#}
#
#with DAG('b3_stock_etl', default_args=default_args, schedule_interval='@daily') as dag:
#
#    create_tables_task = PythonOperator(
#        task_id='create_tables',
#        python_callable=create_tables
#    )
#  
#    extract_task = PythonOperator(
#        task_id='extract_data',
#        python_callable=extract_data,
#        op_kwargs={'company_code': 'PETR4.SA'}  # Passando o company_code diretamente
#    )
#    
#    transform_task = PythonOperator(
#        task_id='transform_data',
#        python_callable=transform_data
#    )
#    
#    load_task = PythonOperator(
#        task_id='load_data',
#        python_callable=load_data,
#        op_kwargs={'company_code': 'PETR4.SA', 'company_name': 'Petrobras'}  # Adicionando também o nome da empresa
#    )
#    
#    create_tables_task >> extract_task >> transform_task >> load_task
#