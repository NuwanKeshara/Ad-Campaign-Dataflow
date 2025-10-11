import os
import subprocess
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from dotenv import load_dotenv


load_dotenv("/opt/airflow/AD-CAMPAIGN-DATAFLOW/.env")

PY_SCRIPTS_PATH = os.getenv("PY_SCRIPTS_PATH")
DBT_MODELS_PATH = os.getenv("DBT_MODELS_PATH")


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
    tags = ["ad_campaign", "dbt", "python", "airflow"],
)


# python scripts
def run_python_script(script_name):
    script_path = os.path.join(PY_SCRIPTS_PATH, script_name)
    subprocess.run(["python", script_path], check=True)

script1 = PythonOperator(
    task_id = "run_script1",
    python_callable = run_python_script,
    op_args = ["load_to_postgres.py"],
    dag=dag,
)

script2 = PythonOperator(
    task_id = "run_script2",
    python_callable = run_python_script,
    op_args = ["run_quality_checks.py"],
    dag = dag,
)

script3 = PythonOperator(
    task_id = "run_script3",
    python_callable = run_python_script,
    op_args = ["stage_to_dm.py"],
    dag = dag,
)


# dbt models
dbt_run_staging = BashOperator(
    task_id = "dbt_run_staging_models",
    bash_command = f"cd {DBT_MODELS_PATH} && dbt run --select staging.daily_ad_logs_view.sql staging.campaign_daily_metrics_view.sql",
    dag = dag,
)


dbt_test_staging = BashOperator(
    task_id = "dbt_test_staging_models",
    bash_command = f"cd {DBT_MODELS_PATH} && dbt test --select staging.daily_ad_logs_view.sql staging.campaign_daily_metrics_view.sql",
    dag = dag,
)


dbt_run_final = BashOperator(
    task_id = "dbt_run_final_model",
    bash_command = f"cd {DBT_MODELS_PATH} && dbt run --select data_model.campaign_metrics_view.sql",
    dag = dag,
)


dbt_test_final = BashOperator(
    task_id = "dbt_test_final_model",
    bash_command = f"cd {DBT_MODELS_PATH} && dbt test --select data_model.campaign_metrics_view.sql",
    dag = dag,
)


# pipeline
(
    script1
    >> script2
    >> dbt_run_staging
    >> dbt_test_staging
    >> script3
    >> dbt_run_final
    >> dbt_test_final
)
