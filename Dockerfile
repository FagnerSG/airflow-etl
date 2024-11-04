# Use a imagem oficial do Apache Airflow
FROM apache/airflow:2.10.2

# Instala as dependências necessárias no sistema operacional
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Instala os pacotes do Python necessários, incluindo o yfinance
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Copia os DAGs para o diretório correto dentro do contêiner
COPY ./dags /opt/airflow/dags

# Defina a porta padrão para a interface web do Airflow
EXPOSE 8080

# Inicializa o Airflow com o comando para inicializar o banco de dados
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["airflow db init && airflow webserver"]
