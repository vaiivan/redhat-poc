from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from datetime import datetime, timedelta
from textwrap import dedent

def dag_success_alert(context):
    print(f"DAG has succeeded, run_id: {context['run_id']}-------------")

with DAG(
    "TestDAG",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tutorial DAG",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    on_success_callback=dag_success_alert,
) as dag:
    test = KubernetesPodOperator(
        namespace="airflow",
        image="quay.io/eformat/airflow-runner:2.5.1",
        cmds=["bash", "-cx"],
        arguments=["echo", "10", "echo pwd"],
        name="test",
        task_id="test",
        get_logs=True,
    )

    test