from pyspark.sql import functions as F

# Scenario 3: Data Validation Before Ingestion

# 3 Hard Validations: Should block ingestion
# 1. party_key should not be null
# 2. source_updated_at should not be null
invalid_df = df.filter(
        F.col("party_key").isNull() |
        F.col("source_updated_at").isNull()
    )
    
if invalid_df.count() > 0:
    raise Exception("Hard validation failed -> `party_key` or `source_updated_at` is NULL")


# 3. party_key should be unique
df.createOrReplaceTempView("temp_table")

df_with_duplicates = spark.sql("""
                        SELECT
                            party_key,
                            COUNT(*)
                        FROM
                            temp_table
                        GROUP BY
                            party_key
                        HAVING
                            COUNT(*) > 1
                    """)

if df_with_duplicates.count() > 0:
    raise Exception("Hard validation failed -> `party_key` is not unique")




# 2 Soft Validations: Should only raise alerts

# 1. More than 10% of `name` column should not be null
total = df.count()
null_pct = df.filter(F.col("name").isNull()).count() / total

if null_pct > 0.1:
    null_percentage = null_pct * 100.0
    print(f"ALERT -> High null percentage in name: {null_percentage}%")


# 2. dob should not be null and be in valid Date format: yyyy-MM-dd
invalid_dob_df = df.filter(F.to_date(F.col("dob"), "yyyy-MM-dd").isNull())

if invalid_dob_df.count() > 0:
    print("ALERT -> `dob` column does not have data in yyyy-MM-dd format")