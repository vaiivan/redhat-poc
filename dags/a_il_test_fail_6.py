from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime
import boto3

def send_email_ses():
    # Initialize the AWS Hook to retrieve credentials
    aws_hook = AwsBaseHook(aws_conn_id='aws_connection', client_type='ses')
    credentials = aws_hook.get_credentials()
    
    # Initialize the Boto3 SES client with the retrieved credentials
    ses_client = boto3.client(
        'ses',
        region_name='ap-east-1',
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
    )
    
    # Send email using the Boto3 SES client
    response = ses_client.send_email(
        Source="ivan.lo@amidas.com.hk",
        Destination={
            'ToAddresses': ["loyanngai@gmail.com"],
        },
        Message={
            'Subject': {
                'Data': 'Test from SES',
                'Charset': 'UTF-8',
            },
            'Body': {
                'Html': {
                    'Data': '<p>Test email sent from Airflow using SES</p>',
                    'Charset': 'UTF-8',
                },
            },
        },
    )
    print(response)

with DAG(
    dag_id="a_il_test_fail_ses_boto3",
    description="DAG testing SES with Boto3",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    send_email_task = PythonOperator(
        task_id="send_email_ses",
        python_callable=send_email_ses,
    )

send_email_task
