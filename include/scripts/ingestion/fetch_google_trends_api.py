import sys, os, json, pandas as pd, time, random
from datetime import datetime, timedelta
from pytrends.request import TrendReq
from collections import Counter

# Vérif arguments
if len(sys.argv) < 3:
    raise ValueError("Format: python fetch_google_trends_api.py <YYYY-MM-DD> <parquet_file_path>")

date_str, parquet_file = sys.argv[1], sys.argv[2]

if not os.path.exists(parquet_file):
    raise FileNotFoundError(f"Fichier parquet introuvable : {parquet_file}")

df = pd.read_parquet(parquet_file)

# Extraire les keywords
def extract_keywords(column):
    keywords = []
    for cell in df[column].dropna():
        if isinstance(cell, str):
            parts = [p.strip() for p in cell.replace("[","").replace("]","").replace("'","").split(",")]
            keywords.extend([p.lower() for p in parts if p])
    return keywords

keywords_seo = extract_keywords("seo_keyword")
categories = df["category"].dropna().str.lower().unique().tolist() if "category" in df.columns else []

clean_keywords = [kw for kw in keywords_seo if len(kw) > 2]
top_keywords = [kw for kw, _ in Counter(clean_keywords).most_common(20)]
top_categories = [cat for cat in categories if len(cat) > 2][:5]
all_terms = list(set(top_keywords + top_categories))

print(f"Total terms: {len(all_terms)} → Exemple: {all_terms[:10]}")

pytrends = TrendReq(hl='fr-FR', tz=360)
start_date = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
timeframe = f"{start_date} {date_str}"

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

results = pd.DataFrame()

def fetch_trends(keywords_chunk, max_retries=3):
    for attempt in range(max_retries):
        try:
            pytrends.build_payload(keywords_chunk, timeframe=timeframe)
            interest = pytrends.interest_over_time()
            if interest.empty:
                print(f"Aucune donnée pour {keywords_chunk}")
                return None
            return interest.drop(columns=["isPartial"], errors="ignore").reset_index()
        except Exception as e:
            if "429" in str(e):
                wait = (attempt + 1) * 10 + random.randint(0,5)
                print(f"Rate limit pour {keywords_chunk}, tentative {attempt+1}/{max_retries}, attente {wait}s")
                time.sleep(wait)
            else:
                print(f"Erreur PyTrends: {e}")
                return None
    print(f"Abandon pour {keywords_chunk}")
    return None

# Fetch avec chunks
for i, chunk_terms in enumerate(chunks(all_terms, 5), start=1):
    print(f"Requête {i}: {chunk_terms}")
    df_interest = fetch_trends(chunk_terms)
    if df_interest is not None and not df_interest.empty:
        if results.empty:
            results = df_interest
        else:
            results = pd.merge(results, df_interest, on="date", how="outer")
    time.sleep(1)  # sleep court pour Airflow

if results.empty:
    raise ValueError("Aucune donnée Google Trends récupérée")

# Sauvegarde complète
results["date"] = results["date"].astype(str)
output_dir = os.path.join("/usr/local/airflow/include/data/raw/trends/google_trends", date_str)
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "data.json")

results.to_json(output_path, orient="records", force_ascii=False, indent=2)
print(f"Données Google Trends récupérées → {output_path}")
