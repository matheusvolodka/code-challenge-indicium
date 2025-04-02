from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import os

#Função que renomeia o diretório
def rename_dir():
    date = datetime.now()
    old_dir = '/home/volodka/documentos/code-challenge-indicium/ext_data/date'
    new_dir = f'/home/volodka/documentos/code-challenge-indicium/ext_data/northwind_data_{date.strftime("%Y-%m-%d/")}'
    os.rename(old_dir, new_dir)

#Variavel para o caminho do diretório do Meltano
meltano_project = '~/documentos/code-challenge-indicium/northwind_meltano'

# Construção da DAG do Airflow. Para extrair dados de dias anteriores, basta mudar a integral do days_ago()
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "northwind_pipeline",
    default_args=default_args,
    description="Executa a pipeline da database northwind",
    schedule_interval="@daily",
    catchup=False,
)
#Extrai o primeiro csv order_details e carrega no diretório "csv"
extract_order_details = BashOperator(
    task_id="extract_order_details",
    bash_command=f"cd {meltano_project} && meltano run tap-csv target-csv",
    dag=dag,
)
#Extrai as tabelas do postgres e carrega no diretório das tabelas "postgres"
extract_postgres_data = BashOperator(
    task_id="extract_postgres_data",
    bash_command=f"cd {meltano_project} && meltano run tap-postgres target-csv--postgres",
    dag=dag,
)
#Carrega os arquivos para a database do PostgreSQL
load_postgres = BashOperator(
    task_id="load_postgres",
    bash_command=f"cd {meltano_project} && meltano run tap-csv--2 target-postgres",
    dag=dag,
)
#Renomeia o diretório com a data em que os dados foram extraídos, sempre que a DAG rodar, um novo diretório com os dados será
#gerado, mantendo os diretórios passados
rename_directory = PythonOperator(
    task_id='rename_directory',
    python_callable=rename_dir,
    dag=dag,
)

[extract_order_details, extract_postgres_data] >> load_postgres >> rename_directory
