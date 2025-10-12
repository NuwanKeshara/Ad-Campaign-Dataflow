import os
import subprocess
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from dotenv import load_dotenv


load_dotenv("/opt/airflow/AD-CAMPAIGN-DATAFLOW/.env")

PYTHON_SCRIPTS_PATH = os.getenv("PYTHON_SCRIPTS_PATH")
DBT_MODELS_PATH = os.getenv("DBT_MODELS_PATH")
DBT_PROFILES_PATH = os.getenv("DBT_PROFILES_PATH")


default_args = {
    "owner": "Nuwan",
    "depends_on_past": False,
    "retries": 1,
}


dag = DAG(
    dag_id = "ad_campaign_dataflow",
    default_args = default_args,
    description = "Run Python scripts and dbt models daily at midnight",
    schedule = "0 0 * * *",
    start_date = datetime(2025, 10, 10),
    catchup = False,
    tags = ["ad_campaign", "dbt", "python", "airflow", "bigquery"],
)


# run python scripts
def run_python_script(script_name):
    script_path = os.path.join(PYTHON_SCRIPTS_PATH, script_name)
    subprocess.run(["python", script_path], check=True)



load_to_bigquery = PythonOperator(
    task_id = "load_to_bigquery",
    python_callable = run_python_script,
    op_args = ["load_to_bigquery.py"], 
    dag=dag,
)

run_quality_checks = PythonOperator(
    task_id = "run_quality_checks_on_staging_table",
    python_callable = run_python_script,
    op_args = ["run_quality_checks_staging_table.py"],
    dag = dag,
)

staging_to_datamodel = PythonOperator(
    task_id = "load_staging_to_datamodel_table",
    python_callable = run_python_script,
    op_args = ["staging_to_datamodel.py"],
    dag = dag,
)



# dbt models
run_staging_models = BashOperator(
    task_id = "dbt_run_staging_models",
    bash_command = f"cd {DBT_MODELS_PATH} && dotenv run -- dbt run --select daily_ad_logs_view.sql campaign_daily_metrics_view.sql --profiles-dir {DBT_PROFILES_PATH}",
    dag = dag,
)

test_staging_models = BashOperator(
    task_id = "dbt_test_staging_models",
    bash_command = f"cd {DBT_MODELS_PATH} && dotenv run -- dbt test --select daily_ad_logs_view.sql campaign_daily_metrics_view.sql --profiles-dir {DBT_PROFILES_PATH}",
    dag = dag,
)

run_data_model = BashOperator(
    task_id = "dbt_run_data_model",
    bash_command = f"cd {DBT_MODELS_PATH} && dotenv run -- dbt run --select campaign_metrics_view.sql --profiles-dir {DBT_PROFILES_PATH}",
    dag = dag,
)

test_data_model = BashOperator(
    task_id = "dbt_test_data_model",
    bash_command = f"cd {DBT_MODELS_PATH} && dotenv run -- dbt test --select campaign_metrics_view.sql --profiles-dir {DBT_PROFILES_PATH}",
    dag = dag,
)



# pipeline
(
    load_to_bigquery >> run_quality_checks >> run_staging_models >> test_staging_models >> staging_to_datamodel >> run_data_model >> test_data_model
)
