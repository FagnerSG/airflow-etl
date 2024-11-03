from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import psycopg2

# Função para criar dimensões e staging
def create_dimensions_and_staging():
    conn = psycopg2.connect(
        host="postgres",
        database="stock_data",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Criar tabela CompanyDTO para armazenar informações da empresa
    cur.execute("""
    CREATE TABLE IF NOT EXISTS CompanyDTO (
        company_id SERIAL PRIMARY KEY,
        company_name VARCHAR(50) UNIQUE
    );
    """)

    # Criar tabela CalendarDTO para armazenar dados de calendário
    cur.execute("""
    CREATE TABLE IF NOT EXISTS CalendarDTO (
        date DATE PRIMARY KEY,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        quarter INTEGER,
        week INTEGER
    );
    """)

    # Criar tabela staging para armazenar dados temporários
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stock_data_staging (
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT,
        company_name VARCHAR(50)
    );
    """)

    conn.commit()
    cur.close()
    conn.close()

# Função para popular a dimensão CalendarDTO
def populate_calendar_dimension():
    conn = psycopg2.connect(
        host="postgres",
        database="stock_data",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Populando o CalendarDTO de 2000 até 2030
    for year in range(2000, 2031):
        for month in range(1, 13):
            for day in range(1, 32):
                try:
                    date = datetime(year, month, day)
                    week = date.isocalendar()[1]
                    quarter = (month - 1) // 3 + 1
                    cur.execute("""
                    INSERT INTO CalendarDTO (date, year, month, day, quarter, week)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date) DO NOTHING;
                    """, (date, year, month, day, quarter, week))
                except:
                    continue  # Ignorar datas inválidas

    conn.commit()
    cur.close()
    conn.close()

# Função para fazer a extração e carregar no staging
def extract_and_stage_data():
    ticker = 'PETR4.SA'  # Empresa da B3
    data = yf.download(ticker, period='1d')

    conn = psycopg2.connect(
        host="postgres",
        database="stock_data",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Inserir nome da empresa na dimensão CompanyDTO
    cur.execute("""
    INSERT INTO CompanyDTO (company_name)
    VALUES (%s)
    ON CONFLICT (company_name) DO NOTHING;
    """, (ticker,))

    # Inserir dados no staging
    for index, row in data.iterrows():
        cur.execute("""
        INSERT INTO stock_data_staging (date, open, high, low, close, volume, company_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (index.date(), row['Open'], row['High'], row['Low'], row['Close'], row['Volume'], ticker))

    conn.commit()
    cur.close()
    conn.close()

# Função para transformar e carregar os dados do staging para as tabelas finais
def transform_and_load_data():
    conn = psycopg2.connect(
        host="postgres",
        database="stock_data",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Transformação: Relacionar dados do staging com CalendarDTO e CompanyDTO e carregar no fato final
    cur.execute("""
    INSERT INTO stock_prices (date, open, high, low, close, volume, company_id)
    SELECT s.date, s.open, s.high, s.low, s.close, s.volume, c.company_id
    FROM stock_data_staging s
    JOIN CompanyDTO c ON s.company_name = c.company_name
    WHERE s.date = (SELECT date FROM CalendarDTO WHERE date = s.date);
    """)

    # Limpar staging após a carga
    cur.execute("DELETE FROM stock_data_staging;")

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

# Definindo o DAG
with DAG(
    'stock_data_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline para dados de ações da B3 com staging, transformação e carga',
    schedule='@daily',
) as dag:

    create_dimensions_task = PythonOperator(
        task_id='create_dimensions_and_staging',
        python_callable=create_dimensions_and_staging
    )

    populate_calendar_task = PythonOperator(
        task_id='populate_calendar_dimension',
        python_callable=populate_calendar_dimension
    )

    extract_and_stage_task = PythonOperator(
        task_id='extract_and_stage_data',
        python_callable=extract_and_stage_data
    )

    transform_and_load_task = PythonOperator(
        task_id='transform_and_load_data',
        python_callable=transform_and_load_data
    )

# Definindo a ordem das tarefas
create_dimensions_task >> populate_calendar_task >> extract_and_stage_task >> transform_and_load_task
