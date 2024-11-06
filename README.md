/airflow-etl
│
└───dags/
│   ├── etl_b3_historico_dag.py    # DAG para processar dados históricos (COTAHIST_A2022.TXT, COTAHIST_A2023.TXT)
│   ├── etl_b3_diario_dag.py       # DAG para processar dados diários (COTAHIST_A2024.TXT)
│   └── __init__.py                # Arquivo de inicialização para a pasta dags
│
├── scripts/
│   ├── etl_b3.py                  # Scripts Python para as funções de ETL (processamento dos dados)
│   └── __init__.py                # Arquivo de inicialização para a pasta scripts
│
├── data/
│   ├── raw/                         # Arquivos de dados brutos (ex: COTAHIST_A2022.TXT, etc.)
│   ├── processed/                   # Arquivos processados após ETL (ex: .csv de dados transformados)
│   └── stage/                       # Arquivos intermediários e tabelas de estágio
│
└── Dockerfile                       # Dockerfile para configurar o ambiente do Airflow
└───sql/
│   └───create_tables.sql         # Script SQL para criação das tabelas no PostgreSQL
│
├── docker-compose.yml             # Configuração do Docker Compose para o ambiente do Airflow
├── airflow.cfg                    # Arquivo de configuração do Airflow


#DEVIDO AO SEU TAMANHO OS AQRUIVOS COTAHIST_A2022.TXT, COTAHIST_A2023.TXT E COTAHIST_A2024.TXT DEVEM SER CONVERTIDOS PARA UM FOMARTO CSV POR EXEMPLO.

#Necessario instalar as dependencias pandas, sqlalchemy, datetime, psycopg2

#Criar o requeriments.txt(ira conter todas as dependencias instaladas)

#1-Iniciar o Apache Airflow
#docker compose up airflow-init

#2-Iniciar o conteiner Docker
#docker-compose up -d

#Acessar o Airflow
#http://localhost:8080/
#Login: airflow
#Senha: airflow