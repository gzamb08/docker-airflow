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


with DAG(dag_id="poc_papermill_bash", schedule_interval="@daily",
         default_args=default_args) as dag:

    exec_notebook_bash = BashOperator(task_id="exec_notebook",
                                      bash_command="papermill /usr/local/airflow/notebooks/poc_ppml.ipynb /usr/local/airflow/notebooks/test_ppml.ipynb -p a 'a b c d' ",
                                      dag=dag)

    exec_notebook_bash



