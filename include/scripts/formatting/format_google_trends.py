import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, map_from_entries, struct, array, lit

if len(sys.argv) < 2:
    raise ValueError("Format : python format_google_trends.py <YYYY-MM-DD>")

date_str = sys.argv[1]

INPUT = f"/usr/local/airflow/include/data/raw/trends/google_trends/{date_str}/data.json"
OUTPUT = f"/usr/local/airflow/include/data/formatted/trends/google_trends/{date_str}/data.parquet"

spark = SparkSession.builder.appName("FormatGoogleTrends").getOrCreate()

try:
    df_raw = spark.read.option("multiline", "true").json(INPUT)
except Exception as e:
    raise FileNotFoundError(f"Impossible de lire le fichier JSON d'entrée à {INPUT} : {e}")

# Récupère toutes les colonnes sauf "date"
field_names = [c for c in df_raw.columns if c != "date"]

# Crée une map (clé = nom de la colonne, valeur = score)
df_temp = df_raw.withColumn(
    "keywords_map",
    map_from_entries(
        array([struct(lit(c).alias("key"), col(c).alias("value")) for c in field_names])
    )
)

# Explose la map en lignes (keyword, trend_score)
df_formatted = df_temp.select(
    col("date").cast("date"),
    explode("keywords_map").alias("keyword", "trend_score")
)

df_formatted.write.mode("overwrite").parquet(OUTPUT)

print(f"Données Google Trends formatées → {OUTPUT}")

spark.stop()
