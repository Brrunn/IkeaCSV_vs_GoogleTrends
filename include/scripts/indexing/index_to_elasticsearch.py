import sys
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

if len(sys.argv) < 3:
    raise ValueError("Usage: python index_to_elasticsearch.py <YYYY-MM-DD> <index_name>")

date_str = sys.argv[1]
index_name = sys.argv[2]

INPUT_FILE = f"/usr/local/airflow/include/data/combined/ikea_trends/{date_str}_combined_data.parquet"

# Connexion à Elasticsearch (via host.docker.internal pour accéder au localhost de macOS)
ES_URL = "http://host.docker.internal:9200"
es = Elasticsearch(ES_URL)

if not es.ping():
    raise ConnectionError(f"Impossible de se connecter à Elasticsearch ({ES_URL})")

# Lire le fichier parquet
df = pd.read_parquet(INPUT_FILE)
print(f"📂 {len(df)} lignes lues depuis {INPUT_FILE}")

# Transformer en documents Elasticsearch
actions = [
    {
        "_index": index_name,
        "_id": f"{date_str}_{i}",  # id unique (date + row)
        "_source": row.to_dict()
    }
    for i, row in df.iterrows()
]

# Ingestion
success, _ = bulk(es, actions)
print(f" {success} documents indexés dans {index_name}")
