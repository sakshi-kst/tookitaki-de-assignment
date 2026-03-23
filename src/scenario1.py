from pyspark.sql.window import Window
from pyspark.sql import functions as F


# Scenario 1: De-duplication & Idempotent Ingestion (Core)
# Write PySpark logic to produce the latest record per party_key, using:
# - source_updated_at as the primary ordering field
# - ingested_at as a tie-breaker

window_spec = Window.partitionBy("party_key") \
                    .orderBy(
                        F.col("source_updated_at").desc(),
                        F.col("ingested_at").desc()
                    )

df_latest = df.withColumn("rn", F.row_number().over(window_spec)) \
              .filter(F.col("rn") == 1) \
              .drop("rn")