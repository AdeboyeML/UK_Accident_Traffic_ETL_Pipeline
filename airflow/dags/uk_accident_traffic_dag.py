import logging
from boto3.session import Session
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import (DownloadfromS3Operator, LoadToRedshiftOperator, UploadToS3Operator, DataQualityOperator)


default_args = {
    'owner': 'AdeboyeML',
    'start_date' : datetime(2020, 6, 5),
    'end_date' : datetime(2021, 6, 6),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}
#start_date = datetime.utcnow()
dag = DAG('UK_Accident_Traffic_DAG',
          default_args=default_args,
          description='Extract data from S3, perform cleaning and transformation using Pyspark, then load back to S3 and finally load transformed data to Redshift with Airflow ---> ETL Data Pipeline',
          schedule_interval='@monthly',
          max_active_runs = 3)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



download_from_s3 = DownloadfromS3Operator(
    task_id='download_from_s3',
    dag=dag,
    aws_credentials_id="aws_credentials",
    s3_bucket = Variable.get('s3_bucket')
)

upload_to_s3 = UploadToS3Operator(
    task_id='upload_to_s3',
    dag=dag,
    aws_credentials_id="aws_credentials",
    s3_bucket = Variable.get('s3_bucket')
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql = "create_tables.sql" 
)

extract_transform_spark_task_1 = BashOperator(
    task_id="extract_transform_spark_1",
    bash_command='/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit /home/workspace/accident_fact_dimension.py',
    dag=dag)

extract_transform_spark_task_2 = BashOperator(
    task_id="extract_transform_spark_2",
    bash_command='/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit /home/workspace/aadf_aggregates.py',
    dag=dag)

###### LOAD ACCIDENT TABLES ######
load_location_table = LoadToRedshiftOperator(
    task_id='load_location_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'location',
    s3_path = "s3://uk-accidents/loc_table.csv",
    file_format = "CSV"
)

load_pedestrian_table = LoadToRedshiftOperator(
    task_id='load_pedestrian_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'pedestrian',
    s3_path = "s3://uk-accidents/pedestrian_table.csv",
    file_format = "CSV"
)

load_road_dimension_table = LoadToRedshiftOperator(
    task_id='load_road_dimension_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'road_table',
    s3_path = "s3://uk-accidents/rd_table.csv",
    file_format = "CSV"
)

load_condition_table = LoadToRedshiftOperator(
    task_id='load_condition_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'condition',
    s3_path = "s3://uk-accidents/condition_table.csv",
    file_format = "CSV"
)

load_local_authority_table = LoadToRedshiftOperator(
    task_id='load_local_authority_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'local_authority',
    s3_path = "s3://uk-accidents/LAD_table.csv",
    file_format = "CSV"
)

load_time_table = LoadToRedshiftOperator(
    task_id='load_time_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'time',
    s3_path = "s3://uk-accidents/time_table.csv",
    file_format = "CSV"
)

load_accident_fact_table = LoadToRedshiftOperator(
    task_id='load_accident_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'accident',
    s3_path = "s3://uk-accidents/accident_fact_table.csv",
    file_format = "CSV"
)

###### DATA QUALITY CHECKS ######
accident_tables_quality_checks = DataQualityOperator(
    task_id='Run_accident_tables_quality_checks',
    dag=dag,
    table = ["accident", "time", "local_authority", "condition", "road_table", "pedestrian", "location"],
    redshift_conn_id = "redshift"
)

###### LOAD TRAFFIC AGGREGATED VEHICLE TABLES #######
load_pedal_cycle_table = LoadToRedshiftOperator(
    task_id='load_pedal_cycle_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'pedal_cycle',
    s3_path = "s3://uk-accidents/pedal_cycle.csv",
    file_format = "CSV"
)

load_motor_vehicles_table = LoadToRedshiftOperator(
    task_id='load_motor_vehicles_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'motor_vehicles',
    s3_path = "s3://uk-accidents/motor.csv",
    file_format = "CSV"
)

load_all_motorvehicles_table = LoadToRedshiftOperator(
    task_id='load_all_motorvehicles_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'all_motorvehicles',
    s3_path = "s3://uk-accidents/allmotor.csv",
    file_format = "CSV"
)

load_rigid_axle_heavygood_vehicles_table = LoadToRedshiftOperator(
    task_id='load_rigid_axle_heavygood_vehicles_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'rhgv',
    s3_path = "s3://uk-accidents/rhgv.csv",
    file_format = "CSV"
)

load_axle_articulated_heavygood_vehicles_table = LoadToRedshiftOperator(
    task_id='load_axle_articulated_heavygood_vehicles_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'ahgv',
    s3_path = "s3://uk-accidents/ahgv.csv",
    file_format = "CSV"
)

load_all_heavygood_vehicles_table = LoadToRedshiftOperator(
    task_id='load_all_heavygood_vehicles_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'all_hgv',
    s3_path = "s3://uk-accidents/allhgv.csv",
    file_format = "CSV"
)

load_road_table = LoadToRedshiftOperator(
    task_id='load_road_category_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table = 'road',
    s3_path = "s3://uk-accidents/road_table.csv",
    file_format = "CSV"
)


###### DATA QUALITY CHECKS ######
traffic_tables_quality_checks = DataQualityOperator(
    task_id='Run_traffic_tables_quality_checks',
    dag=dag,
    table = ["pedal_cycle", "motor_vehicles", "all_motorvehicles", "rhgv", "ahgv", "all_hgv", "road"],
    redshift_conn_id = "redshift"
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> download_from_s3 >> extract_transform_spark_task_1 >> extract_transform_spark_task_2 >> upload_to_s3 >> create_table

create_table >> [load_location_table, load_pedestrian_table, load_road_dimension_table, load_condition_table, load_local_authority_table, load_time_table, load_accident_fact_table]  >> accident_tables_quality_checks

create_table >> [load_pedal_cycle_table, load_motor_vehicles_table, load_all_motorvehicles_table, load_rigid_axle_heavygood_vehicles_table, load_axle_articulated_heavygood_vehicles_table, load_all_heavygood_vehicles_table, load_road_table] >> traffic_tables_quality_checks

[accident_tables_quality_checks, traffic_tables_quality_checks] >> end_operator