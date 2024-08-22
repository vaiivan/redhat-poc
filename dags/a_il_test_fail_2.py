from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

def task_failure_alert(context):
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


with DAG(
    "a_il_test_fail_2",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "owner": "il",
        # "email_on_failure": True,
        # "email_on_retry": False,
        # "email": ["ivan.lo@amidas.com.hk"]
    },
    description="DAG testing for SES",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    on_failure_callback=task_failure_alert,
) as dag:
    a_il_test_fail_2 = PythonOperator(
        task_id="a_il_test_fail_2",
        python_callable=lambda: 1 / 0
    )

    a_il_test_fail_2