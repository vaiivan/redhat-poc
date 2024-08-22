import os 
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.sns_publish_operator import SnsPublishOperator
from airflow.providers.amazon.aws.notifications.sns import send_sns_notification
from airflow.models.connection import Connection

conn = Connection(
    conn_id="sample_aws_connection",
    conn_type="aws",
    login="AKIA2DY77TTT7QKMED7Y",  # Reference to AWS Access Key ID
    password="kmSs1TQZyk19lNq2c7/A3w9HCAXf4LcoIezEdfny",  # Reference to AWS Secret Access Key
    extra={
        # Specify extra parameters here
        "region_name": "ap-east-1",
    },
)
# Generate Environment Variable Name and Connection URI
env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
conn_uri = conn.get_uri()
print(f"{env_key}={conn_uri}")
# AIRFLOW_CONN_SAMPLE_AWS_CONNECTION=aws://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY@/?region_name=eu-central-1

os.environ[env_key] = conn_uri
print(conn.test_connection())  # Validate connection credentials.
# Generate Environment Variable Name and Connection URI
env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
conn_uri = conn.get_uri()
print(f"{env_key}={conn_uri}")

sns_topic_arn = "arn:aws:sns:ap-east-1:695314914535:airflow-sns-tpoic"

from airflow.providers.amazon.aws.notifications.sns import send_sns_notification

dag_failure_sns_notification = send_sns_notification(
    aws_conn_id=conn_uri,
    region_name="ap-east-1",
    message="The DAG {{ dag.dag_id }} failed",
    target_arn=sns_topic_arn,
)


def task_failure_alert(context):
    print(f"Task has failed, task_instance_key_str:---------------------")


# def dag_check_SNS(context):

#     sns = SnsPublishOperator(
#         task_id="check_list",
#         target_arn=sns_topic_arn,
#         subject=f"testing-subject{context['task_instance_key_str']}",
#         message="testing-content",
#     )
#     sns.execute(context)


with DAG(
    dag_id="a_il_test_fail_3",
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
        on_failure_callback=dag_failure_sns_notification
    )
    a_il_test_fail_3 = PythonOperator(
        task_id="a_il_test_fail_3",
        python_callable=lambda: 1 / 0,
        on_failure_callback=task_failure_alert
    )



    email_status >> a_il_test_fail_3