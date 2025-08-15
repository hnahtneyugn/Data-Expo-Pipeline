from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as sf



def etl_airports():

    # Create a Spark session
    spark = SparkSession \
        .builder \
        .master("spark://spark-master:7077") \
        .config(
        "spark.jars",
        "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar,"
        "/opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.42.4.jar"
        ) \
        .appName('airports-gcs-bq-pyspark') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', "/opt/keys/credentials.json")

    # File information
    bucket_name = "data_expo_bucket"
    file_name = "airports_data.csv"
    file_path = f"gs://{bucket_name}/{file_name}"

    airports_data = spark.read.option("inferSchema", "true").option("header", "true").csv(file_path)
    airports_data = airports_data.withColumnRenamed("iata", "IATA").withColumnRenamed("lat", "latitude").withColumnRenamed("long", "longitude")

    # Create mapping to fill in NA values
    MAPPING = {"CLD": ["San Diego", "CA"], 
            "HHH": ["Hilton Head Island", "SC"],
            "MIB": ["Minot", "ND"],
            "MQT": ["Marquette", "MI"], 
            "RCA": ["Rapid City", "SD"], 
            "RDR": ["Grand Forks", "ND"],
            "ROP": ["Mueang Prachinburi", "PC"],
            "ROR": ["Airai", "PW350"],
            "SCE": ["State College", "PA"],
            "SKA": ["Spokane", "WA"],
            "SPN": ["San Jose", "TI"],
            "YAP": ["Colonia", "FMYAP"]}


    for key, values in MAPPING.items():
        airports_data = airports_data.withColumns({'city': sf.when(sf.col('IATA') == key, values[0]).otherwise(sf.col('city')), 'state': sf.when(sf.col('IATA') == key, values[1]).otherwise(sf.col('state'))})

    # Write to BigQuery
    spark.conf.set('temporaryGcsBucket', 'data_expo_temp_bucket')
    output_dataset = "data-expo-pipeline.data_expo_dataset.airport_table"
    airports_data.write.format('bigquery').option('table', output_dataset).mode("overwrite").save()

    # When done, stop the session
    spark.stop()

with DAG(
    dag_id='airports_data_gcs_bigquery',
    schedule=None,
    start_date=datetime(2025, 8, 14),
    catchup=False,
    tags=['GCS', 'BigQuery', 'ETL']
) as dag:

    task_etl = PythonOperator(
        task_id='etl_airports',
        python_callable=etl_airports
    )