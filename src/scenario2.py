from pyspark.sql import functions as F
from pyspark.sql.types import *


# Scenario 2: Handling Large Datasets

# Assumption: Schema is same as mentioned in the previous Scenario
# - party_key, source_updated_at, name, dob, country, is_deleted, ingested_at
schema = StructType(
        [
            StructField("party_key", StringType(), False),
            StructField("source_updated_at", TimestampType(), False),
            StructField("name", StringType(), True),
            StructField("dob", DateType(), True),
            StructField("country", StringType(), False),
            StructField("is_deleted", BooleanType(), False),
            StructField("ingested_at", TimestampType(), False)
        ]
    )

# Apply an explicit schema
df = spark.read \
          .option("header", True) \
          .schema(schema) \
          .csv("/path/to/file.csv")


# # Can convert ingested_at (Timestamp value) to ingestion_date (Date value), and use that for partitioning
# # if using ingested_at will lead to very few records per partition.
# df_with_date = df.withColumn("ingestion_date", F.to_date(F.col("ingested_at")))

# Avoids the small files problem + Partitioning strategy
df.repartition(480) \
  .write \
  .mode("overwrite") \
  .partitionBy("ingested_at") \
  .parquet("s3://bucket-name/path/to/folder")
