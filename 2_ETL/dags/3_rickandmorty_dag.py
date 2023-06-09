"""
Working with API using a custom operator, storing data in Greenplum
"""


from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from plugins.ramapi_location_operator import RAMAPILocationOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-panchishin',
    'retries': 3,
    'max_active_runs': 1,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=1),
}


dag_params = {
    'dag_id': '3_rickandmorty_dag',
    'catchup': False,
    'default_args': DEFAULT_ARGS,
    'tags': ['m-panchishin'],
    'schedule_interval': '@daily'
}


with DAG(**dag_params) as dag:

    start = DummyOperator(
        task_id='start',
    )


    end = DummyOperator(
        task_id='end',
    )

    """
    Custom operator to retrieve data from API
    """
    ram_operator = RAMAPILocationOperator(
        task_id='ram_operator',
    )

    """
    PostgresOperator with DDL to create table in Greenplum
    """
    create_gp_table = PostgresOperator(
        task_id='create_gp_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            CREATE TABLE IF NOT EXISTS m_panchishin_ram_location
            (
                id integer PRIMARY KEY,
                name varchar(512) not null,
                type varchar(256) not null,
                dimension varchar(512) not null,
                resident_cnt integer
            )
            DISTRIBUTED BY (id);
        """,
        autocommit=True,
    )

    """
    PostgresOperator to insert data into Greenplum from API via XCOM
    """
    load_gp = PostgresOperator(
        task_id='load_gp',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            "TRUNCATE TABLE m_panchishin_ram_location",
            "INSERT INTO m_panchishin_ram_location VALUES {{ ti.xcom_pull(task_ids='ram_operator') }}",
        ], 
        autocommit=True,
    )


    start >> ram_operator >> create_gp_table >> load_gp >> end


ram_operator.doc_md = """API result retrieval operator"""
create_gp_table.doc_md = """Creating a table in Greenplum"""
load_gp.doc_md = """Loading data into Greenplum"""