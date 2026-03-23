-- Scenario 6: Spark Optimization & SQL Reasoning

-- 2. Write SQL queries to:
-- - Identify duplicate party_key records
SELECT
  party_key,
  COUNT(*)
FROM
  main_table
GROUP BY
  party_key
HAVING
  COUNT(*) > 1;


-- - Compare record counts between raw and curated tables
SELECT
    (SELECT COUNT(*) FROM raw_table) AS raw_count,
    (SELECT COUNT(*) FROM curated_table) AS curated_count;


-- - Find records rejected during ingestion
df = spark.read \
          .option("header", True) \
          .option("mode", "PERMISSIVE") \
          .option("columnNameOfCorruptRecord", "_corrupt_record") \
          .schema(schema) \
          .csv("/path/to/file.csv")

rejected_df = df.filter(col("_corrupt_record").isNotNull())
rejected_df.show()
