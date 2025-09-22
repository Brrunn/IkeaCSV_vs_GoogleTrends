import sys
import os
from pyspark.sql import SparkSession
from datetime import datetime

if len(sys.argv) < 3:
    raise ValueError("Format : python format_ikea_csv.py <YYYY-MM-DD> /usr/local/airflow/include/data/processed/ikea_with_keywords.csv") # Updated usage message too
    
date_str = sys.argv[1]
csv_path = sys.argv[2]


try:
    date = datetime.strptime(date_str, "%Y-%m-%d")
except ValueError:
    raise ValueError("Date invalide. Utilise le format YYYY-MM-DD")

output_dir = "/usr/local/airflow/include/data/formatted/ikea/"

os.makedirs(output_dir, exist_ok=True)

parquet_filename = f"{date_str}_{os.path.splitext(os.path.basename(csv_path))[0]}.parquet"
parquet_path = os.path.join(output_dir, parquet_filename)

spark = SparkSession.builder.appName("FormatIkeaCSV").getOrCreate()

try:
    df = spark.read.option("header", "true").csv(csv_path)
    df.write.mode("overwrite").parquet(parquet_path)
    print(f"Fichier parquet sauvegardé : {parquet_path}")

except Exception as e:
    print(f"Erreur lors du traitement du CSV : {e}")
    sys.exit(1)

finally:
    spark.stop()