"""
Collecting data from Greenplum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-panchishin',
    'poke_interval': 600
}


dag_params = {
    'dag_id': "2_hook_dag",
    'default_args': DEFAULT_ARGS,    
    'schedule_interval': '0 15 * * 1-6',
    'max_active_runs': 1,
    'tags': ['m-panchishin'] 
}


with DAG(**dag_params) as dag:

    def xcom_ds(**kwargs):
        from datetime import datetime
        ds = datetime.strptime(kwargs['templates_dict']['ds'], '%Y-%m-%d').weekday()+1
        return ds


    def gp_select(**kwargs):
        ti = kwargs['ti']
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("select_gp")
        weekday = ti.xcom_pull(task_ids='get_execution_date')
        sql=f'SELECT heading FROM articles WHERE id = {weekday}'
        cursor.execute(sql)
        query_res = cursor.fetchone()[0]
        ti.xcom_push(value=query_res, key='article')


    start = DummyOperator (
        task_id = 'start'
    )


    end = DummyOperator(
        task_id='end'
    )
    

    get_execution_date = PythonOperator(
        task_id='get_execution_date',
        python_callable=xcom_ds,
        templates_dict={'ds': '{{ ds }}'},
    )

    
    push_greenplum = PythonOperator(
        task_id='push_greenplum',
        python_callable=gp_select,
        provide_context=True,
        )


    start >> get_execution_date >> push_greenplum >> end


    dag.doc_md = __doc__
    get_execution_date.doc_md = """Obtaining the day of the week number and passing it to XCOM"""
    push_greenplum.doc_md = """Retrieving the result of the previous task's execution from XCOM and sending it further to XCOM"""