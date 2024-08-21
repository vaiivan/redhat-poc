from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

def il_failure_callback():
    try:
        print('-------------- tasked failed --------------')
    except Exception as e:
        raise Exception


with DAG(
    "IL_test_fail",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "owner": "il"
    },
    description="A simple tutorial DAG",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    on_failure_callback=il_failure_callback(),
) as dag:
    IL_test_fail = PythonOperator(
        task_id="IL_test_fail",
        python_callable=lambda: 1 / 0
    )

    IL_test_fail