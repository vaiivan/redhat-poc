from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.sns_publish_operator import SnsPublishOperator

sns_topic_arn = "arn:aws:sns:ap-east-1:695314914535:airflow-sns-tpoic"

def task_failure_alert(context):
    print(f"Task has failed, task_instance_key_str:---------------------")


def dag_check_SNS(sns_topic_arn,context: Context):

    sns = SnsPublishOperator(
        task_id="check_list",
        target_arn=sns_topic_arn,
        subject="testing-subject",
        message="testing-content",
    )
    sns.execute(context)


with DAG(
    dag_id="a_il_test_fail_2",
    description="DAG testing for SES",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    # on_failure_callback=task_failure_alert,
) as dag:
    email_status = EmailOperator(
        mime_charset='utf-8',
        task_id="sending_status_email",
        to="loyanngai@hotmail.com",
        subject="Test from SES",
        html_content="Trying to send an email from airflow through SES.",
        on_failure_callback=dag_check_SNS
    )
    a_il_test_fail_2 = PythonOperator(
        task_id="a_il_test_fail_2",
        python_callable=lambda: 1 / 0,
        on_failure_callback=task_failure_alert
    )



    email_status >> a_il_test_fail_2