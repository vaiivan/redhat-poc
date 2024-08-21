from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
    "IL_test_dag",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "owner": "il"
    },
    description="A simple tutorial DAG",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test = KubernetesPodOperator(
        namespace="airflow",
        image="quay.io/eformat/airflow-runner:2.5.1",
        cmds=["bash", "-cx"],
        arguments=["echo", "10", "echo pwd"],
        name="test",
        task_id="IL_test",
        get_logs=True,
    )

    test