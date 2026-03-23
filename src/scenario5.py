# Scenario 5: Airflow DAG Design

from airflow import DAG
from airflow.utils.context import Context
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta


# Method to notify failures via Slack message
def notify_failure(context: Context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    log_url = context.get('task_instance').log_url
    
    slack_msg = f"""
        :red_circle: Task: {task_id} in {dag_id} DAG failed.
        View error log: {log_url}
    """
    
    failed_alert = SlackWebhookOperator(
        task_id = "notify_failure_on_slack",
        slack_webhook_conn_id = "slack_default",
        message = slack_msg,
        channel = "#airflow-notifications",
        username = "Airflow User"
    )

    return failed_alert.execute(context=context)


# Dummy functions to be called by DAG Task
def detect_new_files():
    # This python function looks for a new file / input data, that needs to be processed.
    # We can also make use of FileSensor instead of writing a PythonOperator.
    pass

def validate():
    # This python function runs validation checks on the new data, before ingestion.
    # It can be used to trigger the Hard / Soft validation checks or the Great Expectations Suite.
    pass

def reconcile():
    # This python function generates the reconciliation metrics, after the data ingestion has completed.
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
        python_callable = detect_new_files,
        on_failure_callback = notify_failure
    )

    validate = PythonOperator(
        task_id = "validate",
        python_callable = validate,
        on_failure_callback = notify_failure
    )

    ingest = SparkSubmitOperator(
        task_id = "ingest",
        application = "/path/to/spark_job.py",
        executor_cores = 4,
        executor_memory = "5G",
        num_executors = 20,
        on_failure_callback = notify_failure
    )

    reconcile = PythonOperator(
        task_id = "reconcile",
        python_callable = reconcile,
        on_failure_callback = notify_failure
    )

    notify_success = SlackWebhookOperator(
        task_id = "notify_success_on_slack",
        slack_webhook_conn_id = "slack_default",
        message = "Data Ingestion workflow completed successfully!",
        channel = "#airflow-notifications",
        username = "Airflow User"
    )

detect >> validate >> ingest >> reconcile >> notify_success