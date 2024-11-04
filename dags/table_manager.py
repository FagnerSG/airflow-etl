import psycopg2

class TableManager:
    def __init__(self, host, database, user, password, port="5433"):
        self.conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        self.cursor = self.conn.cursor()

    def create_tables(self):
        # Criar tabela de datas
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS CalendarDTO (
            date DATE PRIMARY KEY,
            year INT,
            month INT,
            day INT,
            quarter INT,
            week INT
        );
        """)

        # Criar tabela de empresas
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS CompanyDTO (
            company_code VARCHAR(10) PRIMARY KEY,
            company_name VARCHAR(255)
        );
        """)

        # Criar tabela de preços de ações
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS StockPriceFact (
            company_code VARCHAR(10),
            date DATE,
            open_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            close_price FLOAT,
            volume BIGINT,
            PRIMARY KEY (company_code, date),
            FOREIGN KEY (company_code) REFERENCES CompanyDTO(company_code),
            FOREIGN KEY (date) REFERENCES CalendarDTO(date)
        );
        """)

        # Confirmar as mudanças
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()
