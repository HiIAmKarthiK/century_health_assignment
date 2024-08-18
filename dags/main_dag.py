from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id='my_dag',
    description='patients data analysis dag',
    start_date=datetime.now(),
    schedule_interval=None
) as dag:
    
    spark_submit_task = SparkSubmitOperator(
    task_id='1',
    application='D:\\personal\\century_health_assignment\\include\\app.py',  # Path to your Spark application
    name='connect_task',
    conn_id='spark_default',  # Connection ID configured in Airflow
    verbose=False,
    conf={
        'spark.executor.memory': '2g',
        'spark.driver.memory': '2g',
        'spark.executor.cores': '1',          # Number of cores per executor
        'spark.cores.max': '1',              # Total executor cores
        'spark.executor.instances': '1'
    },
    )

    

    spark_submit_task 