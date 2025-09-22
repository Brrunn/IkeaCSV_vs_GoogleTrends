from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
from pathlib import Path
import sys

# Ajouter tes scripts dans le path
sys.path.append("/usr/local/airflow/include/scripts/ingestion")
sys.path.append("/usr/local/airflow/include/scripts/combination")
sys.path.append("/usr/local/airflow/include/scripts/indexing")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ikea_trends_full_pipeline_grafana',
    default_args=default_args,
    description='Pipeline complet Ikea → Parquet → Elasticsearch (Grafana)',
    schedule_interval='@daily',
    catchup=False,
    tags=['ikea', 'trends', 'grafana'],
) as dag:

    @task()
    def enrich_ikea_keywords_if_needed():
        output_path = Path("/usr/local/airflow/include/data/processed/ikea_with_keywords.csv")
        if output_path.exists():
            print("Le fichier ikea_with_keywords.csv existe déjà, tâche ignorée.")
        else:
            print("Le fichier est manquant, lancement du script d\'enrichissement.")
            from make_SEO_friendly import enrich_csv_keywords_parallel
            enrich_csv_keywords_parallel(
                Path("/usr/local/airflow/include/data/raw/ikea/ikea.csv"),
                Path("/usr/local/airflow/include/data/processed/ikea_with_keywords.csv"),
                "gemini-1.5-flash",
                250
            )

    enrich_keywords = enrich_ikea_keywords_if_needed()

    fetch_google_trends = BashOperator(
        task_id='fetch_google_trends',
        bash_command='python3 /usr/local/airflow/include/scripts/ingestion/fetch_google_trends_api.py {{ ds }} /usr/local/airflow/include/data/formatted/ikea/{{ ds }}_ikea_with_keywords.parquet',
    )

    test_google_trends_api = BashOperator(
        task_id='test_google_trends_api',
        bash_command='python3 /usr/local/airflow/include/scripts/ingestion/test_google_trends_api.py {{ ds }}'
    )

    fetch_ikea = BashOperator(
        task_id='fetch_ikea',
        bash_command='python3 /usr/local/airflow/include/scripts/ingestion/fetch_ikea.py {{ ds }} /usr/local/airflow/include/data/processed/ikea_with_keywords.csv'
    )

    format_google_trends = BashOperator(
        task_id='format_google_trends',
        bash_command='python3 /usr/local/airflow/include/scripts/formatting/format_google_trends.py {{ ds }} /usr/local/airflow/include/data/raw/trends/google_trends/{{ ds }}/data.json'
    )

    format_ikea_csv = BashOperator(
        task_id='format_ikea_csv',
        bash_command='python3 /usr/local/airflow/include/scripts/formatting/format_ikea_csv.py {{ ds }} /usr/local/airflow/include/data/processed/ikea_with_keywords.csv'
    )

    analyse_correlation = BashOperator(
        task_id='analyse_trends_vs_ikea',
        bash_command='python3 /usr/local/airflow/include/scripts/combination/analyse_trends_vs_ikea.py {{ ds }}',
    )

    index_data_to_elastic = BashOperator(
        task_id='index_data_to_elastic',
        bash_command='python3 /usr/local/airflow/include/scripts/indexing/index_to_elasticsearch.py {{ ds }} ikea_trends',
    )

    # Dépendances
    enrich_keywords >> fetch_ikea
    fetch_ikea >> format_ikea_csv
    [test_google_trends_api, format_ikea_csv] >> fetch_google_trends
    fetch_google_trends >> format_google_trends
    format_google_trends >> analyse_correlation
    analyse_correlation >> index_data_to_elastic
