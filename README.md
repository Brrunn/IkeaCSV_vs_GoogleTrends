# Description

Ce projet met en place un pipeline de traitement et d’analyse de données automatisé avec Apache Airflow, PySpark, Elasticsearch et Grafana.
L’objectif est de combiner les données Ikea (produits et descriptions) avec les données Google Trends, afin d’obtenir des insights exploitables pour le SEO et le suivi des tendances.

Le projet inclut :
1. L’ingestion et le nettoyage des données IKEA et Google Trends
2. Le formatage en Parquet pour un traitement performant avec Spark
3. L’analyse et la combinaison des données pour générer un dataset final enrichi
4. L’indexation des données dans Elasticsearch
5. La visualisation des résultats avec Grafana

Le tout est entièrement dockerisé.

# Architecture du projet

learning-airflow/

|-- astro/                 # Images et configuration Astro

|-- airflow/               # Configuration Airflow

|-- apps/                  # Applications supplémentaires

|-- dags/

|   `-- dags.py            # DAG Airflow principal

|-- include/

|   `-- data/

|       |-- combined/

|       |-- formatted/

|       |-- processed/

|       `-- raw/

|-- scripts/

|   |-- analyse_trends_vs_ikea.py

|   |-- formatting/

|   |   |-- format_google_trends.py

|   |   `-- format_ikea_csv.py

|   |-- indexing/

|   |   `-- index_to_elasticsearch.py

|   `-- ingestion/

|       |-- fetch_google_trends_api.py

|       |-- fetch_ikea.py

|       |-- make_SEO_friendly.py

|       `-- test_google_trends_api.py

|-- Dockerfile

`-- docker-compose.override.yml





# Deploy Your Project Locally

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either stop your existing Docker containers or change the port.

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

## Fonctionnement du pipeline

1. Enrichissement des mots-clés Ikea 
   Le script `make_SEO_friendly.py` enrichit les produits Ikea avec des mots-clés SEO pertinents via l’API Google Gemini.

2. Ingestion des données
   - `fetch_ikea.py` : collecte et filtre les données Ikea.  
   - `fetch_google_trends_api.py` : récupère les tendances Google pour les mots-clés enrichis.  
   - `test_google_trends_api.py` : vérifie la connexion à Google Trends.

3. Formatage des données
   - `format_ikea_csv.py` : conversion du CSV Ikea en Parquet.  
   - `format_google_trends.py` : transformation du JSON Google Trends en Parquet.

4. Analyse et combinaison
   - `analyse_trends_vs_ikea.py` : combine les datasets Ikea et Google Trends avec PySpark pour obtenir un dataset final filtré.

5. Indexation et visualisation
   - `index_to_elasticsearch.py` : envoie les données combinées dans Elasticsearch.  
   - Les dashboards Grafana permettent de visualiser l’évolution des tendances et les corrélations avec les produits Ikea.

---

## Démarrage du projet avec Astro

Le projet utilise Astro (Astronomer) pour gérer Airflow et ses dépendances.  

### Prérequis
- [Astro CLI](https://www.astronomer.io/docs/astro/cli-installation) installé
- Docker et Docker Compose installés

### Démarrage en local

astro dev init
astro dev start

Airflow : http://localhost:8080  # Utile pour éxecuter les dags depuis l'interface d'airflow

Elasticsearch : http://localhost:9200

Grafana : http://localhost:3000 (admin/admin)

Kibana : http://localhost:5601
