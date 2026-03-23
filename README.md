# Tookitaki Data Engineer Assignment

## Overview

This project demonstrates scalable data engineering practices using PySpark, SQL, and Airflow concepts.

Key areas covered:

* Idempotent ingestion
* De-duplication logic
* Large-scale ingestion design
* Data validation (Custom + Great Expectations)
* Workflow orchestration (Airflow DAG)
* Spark optimization & SQL Queries

---

## Tech Stack

* PySpark
* SQL
* Great Expectations
* Apache Airflow

---

## Assumptions

* Input data refreshes daily
* Data volume: ~60GB/day
* Ingestion File Format: CSV
* Storage: S3
* Table format: Parquet / Iceberg
* Primary Key: `party_key`

---

## Scenario 1: De-duplication & Idempotent Ingestion (Core)

### Approach

* Idempotency i.e. running the same data gives the same output
    * Window function with deterministic ordering:
        * `source_updated_at` (primary)
        * `ingested_at` (tie-breaker)
    * We can also make use of UPSERT logic, using MERGE INTO command in Iceberg. This will UPDATE the record if it already exists. Else, it will INSERT a new record.


* Duplicate Records
    * We make use of `row_number()` to rank the duplicate records and keep only the latest record, based on the `source_updated_at` and `ingested_at` values.


* Soft Deletes
    * We can filter out the `is_deleted = true` records and keep only the non-deleted records using:
    `df_latest = df.filter(F.col("is_deleted") == False)`
    * Else, we can keep the deleted records, handle them via flag in the output dataframe, and let the downstream decide on how to proceed.

---

## Scenario 2: Handling Large Datasets

### Approach

* Handling Large File Size
    * We have defined an explicit schema, not relying on inference.
    * Parallel read via Spark
    * Partition tuning during write (60 GB data / 128MB chunks = 480 partitions)

* Small Files Problem
    * We use `repartition()` before write to target ~128MB file size.
    * Else, we can also make use of `coalesce()` to dynamically reduce the number of partitions:
    `df.coalesce(50)`

* Partitioning
    * We partition the data on ingestion_date.
    * We can further partition the data on `country` column, for better optimization, depending on the use case.

* Restart Strategy
    * We can maintain checkpoints in the form of a Metadata table, that tracks which files have been processed. In case of failure, it only processes those files which have not been processed yet.
    * Ensures no duplicate ingestion

---

## Scenario 3: Data Validation Before Ingestion

### Hard Validations: Block ingestion & fail pipeline

* `party_key` should not be NULL
* `source_updated_at` should not be NULL
* `party_key` should be unique

### Soft Validations: Only raise alerts

* High NULL % in `name`
* `dob` should be of valid Date format: `yyyy-MM-dd`

---

## Scenario 4: Great Expectations

### Expectations

* `party_key` should be non-NULL and unique
* `country` should be within an allowed set
* `source_updated_at` should be a valid timestamp
* `dob`, if present, must be before today
* Null percentage of `name` should be below a defined threshold

* ETL Pipeline Integration
    * First, read the data
    * Then, we execute the Great Expectations validation, before ingestion
    * If validations pass, we continue the ETL pipeline
    * If Hard validations fail, we stop the pipeline
    * If Soft validations fail, we trigger an alert via Slack / Email, and continue the pipeline

* Validation Failure
    * Hard validations: Stop the pipeline
    * Soft validations: Trigger alert via Slack / Email, and continue the pipeline

---

## Scenario 5: Airflow DAG Design

### Steps:
1. Detect new files / data
2. Run data validation
3. Execute Spark ingestion job
4. Generate reconciliation metrics
5. Send notification (Success / Failure)

* Retry and Backoff Strategy
    * We have specified that the DAG should do `3 retries`, in case of failures, with the `retry_delay of 5 minutes`.
    * We have also set `retry_exponential_backoff = True`, so that the wait time between 2 retries is doubled in case of repeated failures. This prevents frequent retries and reduces load on the Airflow server. It also gives more time to fix any internal issues (like, missing files) and hence, improves job success rate.

* Prevent Reprocessing
    * We can maintain a Metadata table, that tracks which files have been processed. Before processing, we can check if a file is already present in the metadata table, we skip it. Else, we can process it as it would not have been processed yet.

---

## Scenario 6: Spark Optimization & SQL Reasoning

### Spark Optimization Techniques

* Heavy shuffles
    * Repartition strategy: To avoid shuffles, we need to repartition wisely. We can repartition on the attribute which are being used for joins, pre-filter dataset before wide transformations, and can also explicitly set the number of partitions we want based on the data size:
    `spark.conf.set("spark.sql.shuffle.partitions", 480)`

    * Broadcast: Joins are usually shuffle-heavy operations because they require comparing join columns between 2 datasets. If one of the dataset to be joined is small (preferably, less than 10 MB), we should use Broadcast join. This ensures that the smaller dataset is not shuffled; instead, it copies it to every worker node.
    `joined_df = df.join(F.broadcast(small_df), "join_key", "left")`

* Data skew
    * Salting: We can use Salting to distribute the skewed keys into smaller chunks (and hence, across multiple partitions) using a salt key. We can then do partial aggregation using `(party_key, salt)` to spread the load, and then re-aggregate it to get the final output.
    `num_salts = 10`
    `df_salted = df.withColumn("salt", (F.rand() * num_salts).cast("int"))`

    * Adaptive Query Execution (AQE): We can make use of Spark 3's AQE feature, which handles Data Skew internally.
    `spark.conf.set("spark.sql.adaptive.enabled", True)`
    `spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)`

    * Repartitioning: We can also repartition on the skewed column, if the skew is moderate.

* Too many small output files
    * We can use `repartition()` before write to target ~128MB file size.
    
    * We can also make use of `coalesce()` to dynamically reduce the number of partitions:

---