import os
import pandas as pd
import time
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.generativeai import GenerativeModel
import google.generativeai as genai

GOOGLE_API_KEY = "AIzaSyAKSHcBsZtbTVWuDbRoZmxyjZUgOU7OjQg"
genai.configure(api_key=GOOGLE_API_KEY)

# === CONFIGURATION ===
INPUT_CSV = Path(__file__).parent / "data" / "raw" / "ikea.csv"
OUTPUT_CSV = Path(__file__).parent / "data" / "processed" / "ikea_with_keywords.csv"
MODEL_NAME = "gemini-1.5-flash"
BATCH_SIZE = 250
RETRIES = 3
RETRY_DELAY = 5
MAX_WORKERS = 5  # Ajuste selon tes ressources


def load_csv(csv_path: Path) -> pd.DataFrame:
    print(f"Lecture de   : {csv_path}")
    return pd.read_csv(csv_path)


def save_csv(df: pd.DataFrame, csv_path: Path):
    output_dir = csv_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Écriture vers: {csv_path}")
    df.to_csv(csv_path, index=False)


def clean_and_parse_json(raw_json):
    raw_json = raw_json.strip()
    if raw_json.startswith("```json"):
        raw_json = raw_json[len("```json"):].strip()
    if raw_json.endswith("```"):
        raw_json = raw_json[:-3].strip()
    return json.loads(raw_json)


def generate_keywords(model, input_text: str) -> str:
    prompt = f"""Tu es un expert SEO. Génère un mot-clé principal pertinent de 3-5 mots grand maximum, pour chaque description de produit ci-dessous. Ne mentionne pas les dimensions.
Réponds uniquement avec un JSON comme ceci :
{{"1": "mot clé", "2": "autre mot clé", ...}}

Descriptions :
{input_text}
"""
    for attempt in range(1, RETRIES + 1):
        try:
            response = model.generate_content(prompt)
            return response.text
        except Exception as e:
            print(f"[WARN] essai {attempt} échoué: {e} — retry in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)
    return None


def process_batch(df, model_name, start, end):
    model = GenerativeModel(model_name)  # Création ici pour chaque thread
    sub_df = df.iloc[start:end]
    input_text = "\n".join(
        f"{i + 1}: {row}" for i, row in enumerate(sub_df["short_description"].tolist())
    )

    text = generate_keywords(model, input_text)
    mapping = {}

    if not text:
        print(f"[ERROR] Échec Gemini pour le lot {start + 1}-{end}")
        return mapping

    try:
        data = clean_and_parse_json(text)

        if isinstance(data, dict):
            for k, v in data.items():
                if k.isdigit():
                    idx = start + int(k) - 1
                    mapping[idx] = v
        elif isinstance(data, list):
            for i, v in enumerate(data):
                idx = start + i
                mapping[idx] = v
        else:
            print(f"[ERROR] JSON inattendu : {type(data)} — {text!r}")

    except json.JSONDecodeError as e:
        print(f"[ERROR] impossible de parser JSON pour le lot {start+1}-{end} : {e}\nContenu : {text!r}")

    return mapping


def enrich_csv_keywords_parallel(csv_path, output_csv, model_name, batch_size):
    df = load_csv(csv_path)
    all_mappings = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for start in range(0, len(df), batch_size):
            end = min(start + batch_size, len(df))
            futures.append(executor.submit(process_batch, df, model_name, start, end))

        for future in as_completed(futures):
            mapping = future.result()
            all_mappings.update(mapping)
            print(f"✅ Lot traité ({len(mapping)} mots-clés récupérés)")

    df["seo_keyword"] = df.index.map(all_mappings.get)
    save_csv(df, output_csv)


# === Lancement du script ===
if __name__ == "__main__":
    enrich_csv_keywords_parallel(
        csv_path=INPUT_CSV,
        output_csv=OUTPUT_CSV,
        model_name=MODEL_NAME,
        batch_size=BATCH_SIZE,
    )
