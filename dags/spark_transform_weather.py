"""
Airflow DAG: Spark Weather Transform
Orchestrates the Spark job that transforms raw MongoDB weather data
into structured PostgreSQL tables.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'spark_transform_weather',
    default_args=default_args,
    description='Transform MongoDB weather data to PostgreSQL using Spark',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['spark', 'weather', 'transform', 'etl'],
)


def check_postgres_ready(**context):
    """Check if PostgreSQL weather database is accessible."""
    import time
    max_retries = 5
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host='postgres-weather',
                port=5432,
                user='weather',
                password='weather123',
                database='weather_db',
                connect_timeout=5
            )
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM dim_cities")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            print(f"PostgreSQL ready. Cities in dim_cities: {count}")
            return True
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries}: PostgreSQL not ready - {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

    raise Exception("PostgreSQL weather database not accessible")


def process_staging_data(**context):
    """
    Process any remaining data in staging table to fact table.
    This is a safety step in case Spark job didn't complete the transfer.
    """
    try:
        conn = psycopg2.connect(
            host='postgres-weather',
            port=5432,
            user='weather',
            password='weather123',
            database='weather_db',
            connect_timeout=10
        )
        cursor = conn.cursor()

        # Call the staging processing function
        cursor.execute("SELECT process_staging_data()")
        rows_processed = cursor.fetchone()[0]
        conn.commit()

        # Get final counts
        cursor.execute("SELECT COUNT(*) FROM fact_weather_hourly")
        total_fact = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(*) FROM fact_weather_hourly_staging WHERE processed = FALSE"
        )
        remaining_staging = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        print(f"Post-processing complete:")
        print(f"  - Rows processed: {rows_processed}")
        print(f"  - Total in fact table: {total_fact}")
        print(f"  - Remaining in staging: {remaining_staging}")

        return {
            'rows_processed': rows_processed,
            'total_fact': total_fact,
            'remaining_staging': remaining_staging
        }

    except Exception as e:
        print(f"Error in post-processing: {e}")
        raise


# Task 1: Check PostgreSQL readiness
check_postgres = PythonOperator(
    task_id='check_postgres_ready',
    python_callable=check_postgres_ready,
    dag=dag,
)

# Task 2: Submit Spark job via spark-submit
# Using BashOperator to call spark-submit on the spark-master container
spark_transform = BashOperator(
    task_id='spark_transform_job',
    bash_command='''
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,org.postgresql:postgresql:42.6.0 \
        --conf spark.executor.memory=1g \
        --conf spark.driver.memory=1g \
        /opt/spark-jobs/transform_weather.py \
        2>&1
    ''',
    dag=dag,
)

# Task 3: Post-process staging data
post_process = PythonOperator(
    task_id='post_process_staging',
    python_callable=process_staging_data,
    dag=dag,
)

# Define task dependencies
check_postgres >> spark_transform >> post_process
