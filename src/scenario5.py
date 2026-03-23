# Scenario 5: Airflow DAG Design

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta


# Dummy functions to be called by DAG Task
def detect_new_files():
    pass

def validate():
    pass

def reconcile():
    pass


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True
}

# DAG will run daily at 10 AM UTC
with DAG(
    dag_id = "data_ingestion_pipeline",
    default_args = default_args,
    start_date = datetime(2026, 3, 1),
    catchup = False,
    schedule_interval = "0 10 * * *"
) as dag:

    detect = PythonOperator(
        task_id = "detect_file",
        python_callable = detect_new_files
    )

    validate = PythonOperator(
        task_id = "validate",
        python_callable = validate
    )

    ingest = SparkSubmitOperator(
        task_id = "ingest",
        application = "/path/to/spark_job.py",
        executor_cores = 4,
        executor_memory = "5G",
        num_executors = 20
    )

    reconcile = PythonOperator(
        task_id = "reconcile",
        python_callable = reconcile
    )

    notify = SlackWebhookOperator(
        task_id = "notify_on_slack",
        slack_webhook_conn_id = "slack_default",
        message = "Data Ingestion workflow completed successfully!",
        channel = "#airflow-notifications",
        username = "Airflow User"
    )

detect >> validate >> ingest >> reconcile >> notify