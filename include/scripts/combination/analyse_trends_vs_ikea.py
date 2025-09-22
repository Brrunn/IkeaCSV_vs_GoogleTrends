import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, lit, coalesce, split, explode

if len(sys.argv) < 2:
    raise ValueError("Format : python analyse_trends_vs_ikea.py <YYYY-MM-DD>")

date_str = sys.argv[1]

INPUT_IKEA = f"/usr/local/airflow/include/data/formatted/ikea/{date_str}_ikea_with_keywords.parquet"
INPUT_TRENDS = f"/usr/local/airflow/include/data/formatted/trends/google_trends/{date_str}/data.parquet"
OUTPUT_COMBINED = f"/usr/local/airflow/include/data/combined/ikea_trends/{date_str}_combined_data.parquet"

spark = SparkSession.builder.appName("CombineIkeaAndTrends").getOrCreate()

try:
    # --- IKEA DATA ---
    df_ikea = spark.read.parquet(INPUT_IKEA)
    print(f"Données IKEA lues depuis : {INPUT_IKEA}")
    df_ikea.printSchema()

    # explode seo_keyword (si plusieurs mots clés dans une ligne)
    df_ikea_clean = df_ikea.withColumn("seo_keyword", lower(col("seo_keyword")))
    df_ikea_clean = df_ikea_clean.withColumn("keyword_exploded", explode(split(col("seo_keyword"), ",")))
    df_ikea_clean = df_ikea_clean.withColumn("join_keyword", lower(col("keyword_exploded")))

    # --- GOOGLE TRENDS DATA ---
    df_trends = spark.read.parquet(INPUT_TRENDS)
    print(f"Données Google Trends lues depuis : {INPUT_TRENDS}")
    df_trends.printSchema()

    df_trends_clean = df_trends.withColumn("join_keyword", lower(col("keyword")))

    # --- JOIN ---
    df_combined = df_ikea_clean.join(
        df_trends_clean,
        on="join_keyword",
        how="left"
    )

    # Filtrage si pas de trend_score
    df_filtered = df_combined.filter(col("trend_score").isNotNull())
    print(f"Filtrage effectué : conservé {df_filtered.count()} lignes avec un trend_score.")

    # Colonnes finales
    df_final = df_filtered.select(
        col("name"),
        col("category"),
        col("seo_keyword"),
        col("join_keyword").alias("matched_keyword"),
        coalesce(col("trend_score"), lit(0)).alias("trend_score"),
        lit(date_str).alias("date")
    )

    # Sauvegarde
    df_final.write.mode("overwrite").parquet(OUTPUT_COMBINED)
    print(f"Données combinées et filtrées sauvegardées → {OUTPUT_COMBINED}")

except Exception as e:
    print(f"Erreur lors de la combinaison des données : {e}")
    sys.exit(1)

finally:
    spark.stop()
