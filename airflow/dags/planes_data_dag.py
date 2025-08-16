from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as sf

def etl_planes():

    # Create a Spark session
    spark = SparkSession \
        .builder \
        .master("spark://spark-master:7077") \
        .config(
        "spark.jars",
        "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar,"
        "/opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.42.4.jar"
        ) \
        .appName('planes-gcs-bq-pyspark') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', "/opt/keys/credentials.json")

    # File information
    bucket_name = "data_expo_bucket"
    file_name = "plane_data.csv"
    file_path = f"gs://{bucket_name}/{file_name}"
    planes_data = spark.read.option("inferSchema", "true").option("header", "true").csv(file_path)

    # Filter NULL values
    planes_data = planes_data.filter(planes_data.type.isNotNull())

    # Convert columns to suitable data type
    planes_data = planes_data.withColumns({'issue_date': sf.to_date(planes_data.issue_date, "MM/dd/yyyy"), 'year': sf.col('year').cast('int')})

    # Clean data, can be uploaded to BigQuery as Table
    spark.conf.set('temporaryGcsBucket', 'data_expo_temp_bucket')
    output_dataset = "data-expo-pipeline.data_expo_dataset.plane_table"
    planes_data.write.format('bigquery').option('table', output_dataset).mode("overwrite").save()

    # When done, stop the session
    spark.stop()

with DAG(
    dag_id='planes_data_gcs_bigquery',
    schedule=None,
    start_date=datetime(2025, 8, 16),
    catchup=False,
    tags=['GCS', 'BigQuery', 'ETL']
) as dag:

    task_etl = PythonOperator(
        task_id='etl_planes',
        python_callable=etl_planes
    )