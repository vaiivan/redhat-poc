from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.email_operator import EmailOperator

def task_failure_alert(context):
    print(f"Task has failed, task_instance_key_str:---------------------")


with DAG(
    dag_id="a_il_test_fail_2",
    # default_args={
    #     "depends_on_past": False,
    #     "retries": 0,
    #     "retry_delay": timedelta(minutes=5),
    #     "owner": "il",
    #     # "email_on_failure": True,
    #     # "email_on_retry": False,
    #     # "email": ["ivan.lo@amidas.com.hk"]
    # },
    description="DAG testing for SES",
    # dagrun_timeout=datetime.timedelta(minutes=60),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    on_failure_callback=task_failure_alert,
) as dag:
    email_status = EmailOperator(
        mime_charset='utf-8',
        task_id="sending_status_email",
        to="loyanngai@hotmail.com",
        subject="Test from SES",
        html_content="Trying to send an email from airflow through SES."
    )
    a_il_test_fail_2 = PythonOperator(
        task_id="a_il_test_fail_2",
        python_callable=lambda: 1 / 0,
        on_failure_callback=task_failure_alert
    )



    email_status >> a_il_test_fail_2