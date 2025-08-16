from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as sf

def etl_variables():

    # Create a Spark session
    spark = SparkSession \
        .builder \
        .master("spark://spark-master:7077") \
        .config(
        "spark.jars",
        "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar,"
        "/opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.42.4.jar"
        ) \
        .appName('variables-gcs-bq-pyspark') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', "/opt/keys/credentials.json")

    # File information
    bucket_name = "data_expo_bucket"
    file_name = "variable_descriptions_data.csv"
    file_path = f"gs://{bucket_name}/{file_name}"
    vardes_data = spark.read.option("inferSchema", "true").option("header", "true").csv(file_path)

    # Rename the DataFrame
    vardes_data = vardes_data.withColumnRenamed("_c1", "Name").withColumnRenamed("_c2", "Description")

    # Remove rows with NULL
    vardes_data = vardes_data.filter(sf.col("Variable descriptions").isNotNull())

    # Push data after modification to BigQuery as Table
    spark.conf.set('temporaryGcsBucket', 'data_expo_temp_bucket')
    output_dataset = "data-expo-pipeline.data_expo_dataset.variable_description_table"
    vardes_data.write.format('bigquery').option('table', output_dataset).mode("overwrite").save()

    # Stop the Spark instance
    spark.stop()

with DAG(
    dag_id='variables_data_gcs_bigquery',
    schedule=None,
    start_date=datetime(2025, 8, 16),
    catchup=False,
    tags=['GCS', 'BigQuery', 'ETL']
) as dag:

    task_etl = PythonOperator(
        task_id='etl_variables',
        python_callable=etl_variables
    )