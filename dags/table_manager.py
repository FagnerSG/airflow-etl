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
        CREATE TABLE IF NOT EXISTS daily_data (
            id SERIAL PRIMARY KEY,
            data_pregao DATE NOT NULL,
            codneg VARCHAR(10) NOT NULL,
            nomres VARCHAR(100),
            preabe FLOAT,
            premin FLOAT,
            premax FLOAT,
            preult FLOAT,
            voltot INT,
            company_id INT,
            date_id INT,
            FOREIGN KEY (company_id) REFERENCES company(id),
            FOREIGN KEY (date_id) REFERENCES calendar(id)
        );
        """)

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS historical_data (
            id SERIAL PRIMARY KEY,
            data_pregao DATE NOT NULL,
            codneg VARCHAR(10) NOT NULL,
            nomres VARCHAR(100),
            preabe FLOAT,
            premin FLOAT,
            premax FLOAT,
            preult FLOAT,
            voltot INT,
            company_id INT,
            date_id INT,
            FOREIGN KEY (company_id) REFERENCES company(id),
            FOREIGN KEY (date_id) REFERENCES calendar(id)
        );
        """)

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

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS PapelDTO (
            papelId SERIAL PRIMARY KEY,
            papelName VARCHAR(255) UNIQUE       
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
            stockId SERIAL PRIMARY KEY,
            companyId INT REFERENCES CompanyDTO(companyId),
            papelId INT REFERENCES PapelDTO(papelId),
            dateId INT REFERENCES CalendarDTO(dateId),
            openPrice FLOAT,
            highPrice FLOAT,
            lowPrice FLOAT,
            closePrice FLOAT,
            volume BIGINT
        );
        """)

        # Confirmar as mudanças
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()
