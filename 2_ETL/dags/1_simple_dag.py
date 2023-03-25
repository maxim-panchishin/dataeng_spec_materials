"""
Simple DAG with three operators: a BashOperator, and two PythonOperators.
BashOperator writes parameters of DAG run
PythonOperators are similar. They were created just to see logs in different tasks.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-panchishin',
    'poke_interval': 600
}

dag_params = {
    'dag_id': "1_simple_dag",
    'default_args': DEFAULT_ARGS,    
    'schedule_interval': '@daily',
    'max_active_runs': 1,
    'tags': ['m-panchishin'] 
}

with DAG(**dag_params) as dag:


    start_dag = DummyOperator (
        task_id = 'start_dag'
        )

    """
    BashOperator writes data about DAG Run to the log using the echo command
    """
    bash_jinja_params_output = BashOperator(task_id='bash_jinja_params_output',
                           bash_command='''
                                echo \">>> Start Writing <<<\";
                                echo \"DAG: {{ dag }}\"; 
                                echo \"Task: {{ task }}\"; 
                                echo \"Task Instance: {{ ti }}\"; 
                                echo \"Run ID: {{ run_id }}\"; 
                                echo \"Logical Date: {{ ds }}\"; 
                                echo \">>> Stop Writing <<<\";
                            ''',
                           dag=dag)


    def first_func(**kwargs):
        task = kwargs['templates_dict']['task']
        ds = kwargs['templates_dict']['ds']
        logging.info("First PythonOperator {0} DS: {1}".format(task, ds))

    """
    PythonOperator writes current task ID and timestamp
    """
    python_task_1 = PythonOperator(task_id='python_task_1',
                                python_callable=first_func,
                                templates_dict={'task': '{{ task }}',
                                                'ds': '{{ ds }}'},
                                dag=dag)


    def second_func(**kwargs):
        task = kwargs['templates_dict']['task']
        ds = kwargs['templates_dict']['ds']
        logging.info("Second PythonOperator {0} DS: {1}".format(task, ds))


    """
    PythonOperator writes current task ID and timestamp
    """
    python_task_2 = PythonOperator(task_id='python_task_2',
                                 python_callable=second_func,
                                 templates_dict={'task': '{{ task }}',
                                                 'ds': '{{ ds }}'},
                                 dag=dag)

    start_dag >> bash_jinja_params_output >> [python_task_1, python_task_2]

    dag.doc_md = __doc__
    bash_jinja_params_output.doc_md = "Writes several parameters from templates to the log"
    python_task_1.doc_md = "Writes 'First PythonOperator' to the log"
    python_task_2.doc_md = "Writes 'Second PythonOperator' to the log. Copy of previous one"
