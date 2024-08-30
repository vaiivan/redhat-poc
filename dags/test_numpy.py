import os 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import numpy as np
import pandas as pd


def numpy_test():
    a = np.array([1, 2, 3, 4, 5, 6])
    print("------shape:------", a.shape)
    print("------type:------", type(a))
    print("------ndim:------", a.ndim)

def pandas_test():
    data = {
      "calories": [420, 380, 390],
      "duration": [50, 40, 45]
    }

    #load data into a DataFrame object:
    df = pd.DataFrame(data)

    print(df)


with DAG(
    dag_id="a_il_test_numpy",
    description="DAG testing for Spark",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    a_il_test_numpy = PythonOperator(
        task_id="a_il_test_numpy",
        python_callable=numpy_test,
        dag=dag,
    )

    a_il_test_pandas = PythonOperator(
        task_id="a_il_test_pandas",
        python_callable=pandas_test,
        dag=dag,
    )

    a_il_test_numpy >> a_il_test_pandas