from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html
# https://airflow.apache.org/docs/apache-airflow/1.10.1/tutorial.html#default-arguments
# https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    #'start_date': datetime.now(),
    'depends_on_past' : False,
    'retries' : 3,
    'retries_delay' : timedelta(minutes = 5),
    'catchup' : False,
    'email_on_retry' : False,
    'schedule_interval' : '@hourly'
}

# Instantiate the DAG

dag = DAG('p5_airflow_27',
          default_args=default_args,
          description='udacity Data Pipelines Project in Redshift with Airflow'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create tables using the create_tables.sql file

create_tables_sql = PostgresOperator(
    task_id="create_tables_sql",
    postgres_conn_id="redshift",
    sql="create_tables.sql",
    dag=dag
)

# Use StageToRedshiftOperator to load data to the 
# staging_events and staging_songs staging tables 

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_path='s3://udacity-dend/log_json_path.json',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    #s3_key='song_data/A/A/A',
    dag=dag
)

# Load data to the fact table with the LoadFactOperator

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    table='songplays',
    load_sql=SqlQueries.songplay_table_insert,
    dag=dag
)

# Load data to the dimension tables with the LoadDimensionOperator

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='songs',
    load_sql=SqlQueries.song_table_insert,
    delete_load=True,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    table='users',
    load_sql=SqlQueries.user_table_insert,
    delete_load=True,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='artists',
    load_sql=SqlQueries.artist_table_insert,
    delete_load=True,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='time',
    load_sql=SqlQueries.time_table_insert,
    delete_load=True,
    dag=dag
)

# Define list of tables to be quality checked and check them with DataQualityOperator

all_tables = ['staging_events', 'staging_songs', 'songplays', 'songs', 'users', 'artists', 'time']

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    tables=all_tables,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task Dependencies
# https://www.astronomer.io/guides/managing-dependencies/

start_operator >> create_tables_sql
create_tables_sql >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table,
                         load_user_dimension_table,
                         load_artist_dimension_table, 
                         load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
