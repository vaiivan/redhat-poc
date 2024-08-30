import os 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def spark_task():
    spark = SparkSession.builder.appName("demo").getOrCreate()
    df = spark.createDataFrame(
        [
            ("sue", 32),
            ("li", 3),
            ("bob", 75),
            ("heo", 13),
        ],
        ["first_name", "age"],
    )
    df.show()

with DAG(
    dag_id="a_il_test_fail_5",
    description="DAG testing for Spark",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    a_il_test_spark = PythonOperator(
        task_id="a_il_test_spark",
        python_callable=spark_task,
        dag=dag,
    )

    a_il_test_spark