from airflow import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime

def send_email_ses():
    import boto3
    # Retrieve the AWS connection credentials
    aws_hook = AwsBaseHook(aws_conn_id='aws_connection', client_type='ses')
    credentials = aws_hook.get_credentials()
    ses = boto3.client(
        'ses',
        region_name='ap-east-1',
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
    )
    response = ses.send_email(
        Source="your-verified-email@example.com",
        Destination={
            'ToAddresses': [
                "recipient@example.com",
            ],
        },
        Message={
            'Subject': {
                'Data': 'Test from SES',
                'Charset': 'UTF-8'
            },
            'Body': {
                'Html': {
                    'Data': '<p>Test email sent from Airflow using SES</p>',
                    'Charset': 'UTF-8'
                },
            }
        }
    )
    print(response)

with DAG(
    dag_id="a_il_test_fail_ses_virtualenv",
    description="DAG testing SES with virtualenv",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    send_email_task = PythonVirtualenvOperator(
        task_id="send_email_ses",
        python_callable=send_email_ses,
        requirements=["boto3"],
        system_site_packages=False,
    )

send_email_task
