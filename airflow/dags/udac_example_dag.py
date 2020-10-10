from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import ( CreateTablesOperator, StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 1, 12),
    'depends_on_past': False,
    'retries' : 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup' : False    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id='redshift')

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    log_json_path="log_json_path.json",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    log_json_path="",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.songplay_table_insert,
    table="songplays",
    provide_context=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.user_table_insert,
    table="users",
    provide_context=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.song_table_insert,
    table="songs",
    provide_context=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.artist_table_insert,
    table="artists",
    provide_context=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.time_table_insert,
    table="time",
    provide_context=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables = ['songplays','users','songs','artists','time'],
    provide_context=True
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator


