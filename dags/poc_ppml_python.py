from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import papermill as pm

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1)
}


def execute_notebook():
    pm.execute_notebook('/usr/local/airflow/notebooks/poc_ppml.ipynb',
                        '/usr/local/airflow/notebooks/test_ppml.ipynb',
                        parameters=dict(a='a b c d')
                        )


with DAG(dag_id="poc_papermill_python", schedule_interval="@daily",
         default_args=default_args) as dag:

    exec_notebook_python = PythonOperator(task_id="exec_notebook",
                                   python_callable=execute_notebook,
                                   dag=dag)

    exec_notebook_python

    #todo try out Papermill operator once dict issue has been solved

    # exec_notebook = PapermillOperator(task_id="exec_notebook",
    #                                   input_nb="/notebooks/poc_ppml.ipynb",
    #                                   output_nb="/notebooks/test_ppml.ipynb",
    #                                   parameters={"a": "a b c d"}
    #                                   )


