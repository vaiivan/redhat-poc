from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.amazon.aws.hooks.ses import SESHook
from datetime import datetime, timedelta
from airflow.utils.email import send_email_smtp


ses_region_name = "ap-east-1"
sender_email = "ivan.lo@amidas.com.hk"
recipient_email = "loyanngai@gmail.com"


def send_failure_email(context):
    subject = f"Airflow DAG {context['dag'].dag_id} - Task {context['task_instance'].task_id} Failed"
    html_content = f"""
    <p>Dear User,</p>
    <p>The following task has failed:</p>
    <ul>
        <li><strong>DAG ID:</strong> {context['dag'].dag_id}</li>
        <li><strong>Task ID:</strong> {context['task_instance'].task_id}</li>
        <li><strong>Execution Date:</strong> {context['execution_date']}</li>
    </ul>
    <p>Regards,<br>Airflow</p>
    """
    
    ses_hook = SESHook(aws_conn_id="aws_connection", region_name=ses_region_name)
    ses_hook.send_email(
        mail_from=sender_email,
        to=recipient_email,
        subject=subject,
        html_content=html_content,
    )


with DAG(
    dag_id="a_il_test_fail_6",
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
        on_failure_callback=send_failure_email
    )
    a_il_test_fail_6 = PythonOperator(
        task_id="a_il_test_fail_6",
        python_callable=lambda: 1 / 0,
        on_failure_callback=send_failure_email
    )

    email_status >> a_il_test_fail_6