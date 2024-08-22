from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.ses import SESHook  # Import SESHook
from datetime import datetime

def send_email_ses():
    # Initialize the SESHook
    ses_hook = SESHook(aws_conn_id='aws_connection', region_name='ap-east-1')
    
    # Send email using the hook's send_email method
    response = ses_hook.send_email(
        to_emails=["loyanngai@gmail.com"],
        subject="Test from SES",
        html_content="<p>Test email sent from Airflow using SES</p>",
        from_email="ivan.lo@amidas.com.hk"
    )
    print(response)

with DAG(
    dag_id="a_il_test_fail_ses_seshook",
    description="DAG testing SES with SESHook",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    send_email_task = PythonOperator(
        task_id="send_email_ses",
        python_callable=send_email_ses,
    )

send_email_task
