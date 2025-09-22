import sys
import pandas as pd
from datetime import datetime

if len(sys.argv) < 3:
    raise ValueError("Format : python fetch_ikea.py <YYYY-MM-DD> /opt/data/processed/ikea_with_keywords.csv")
    

csv_path = sys.argv[2]
date_str = sys.argv[1]


try:
    date = datetime.strptime(date_str, "%Y-%m-%d")
except ValueError:
    raise ValueError("Date invalide. Utilise le format YYYY-MM-DD")

df = pd.read_csv(csv_path)

df_filtered = df[["name", "category", "seo_keyword"]]

df_filtered.to_csv(csv_path, index=False)
print(f"Fichier filtré et sauvegardé : {csv_path}")