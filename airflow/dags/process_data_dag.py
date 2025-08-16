from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as sf
from functools import reduce

def etl_fulldata():

    # Create a Spark session
    spark = SparkSession \
        .builder \
        .master("spark://spark-master:7077") \
        .config(
        "spark.jars",
        "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar,"
        "/opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.42.4.jar"
        ) \
        .appName('fulldata-gcs-bq-pyspark') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', "/opt/keys/credentials.json")

    # Read from CSV files
    bucket_name = "data_expo_bucket"
    files = [f"gs://{bucket_name}/{year}_data.csv" for year in range(2000, 2009)]
    df = spark.read.option("header", "true").csv(files)

    # Drop NULL columns
    cols_to_drop = ['CancellationCode', 'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay']
    df = df.drop(*cols_to_drop)

    # Repartition for safe parallelism
    df = df.repartition(40)

    # Write to Parquet in GCS
    df.write.mode("overwrite").parquet(f"gs://{bucket_name}/all_years_parquet")

    # Read Parquet from GCS for better speed
    df_parquet = spark.read.parquet(f"gs://{bucket_name}/all_years_parquet")

    # Filter missing TaxiIn and TaxiOut
    df_parquet = df_parquet.filter(~(sf.col("TaxiIn").isNull() | sf.col("TaxiOut").isNull() | sf.col("TaxiIn").isin(['null', 'NULL', 'NA', 'NaN']) | sf.col("TaxiOut").isin(['null', 'NULL', 'NA', 'NaN'])))

    # Filter missing CRS Elapsed Time
    df_parquet = df_parquet.filter(~(sf.col("CRSElapsedTime").isNull() | sf.col("CRSElapsedTime").isin(['null', 'NULL', 'NA', 'NaN'])))


    # Modify Tail Num information for unknown tail number
    df_parquet = df_parquet.withColumn("TailNum", sf.when((sf.col("TailNum") == "UNKNOW") | sf.col("TailNum").isin(['null', 'NULL', 'NA', 'NaN']) | sf.col("TailNum").isNull(), "Unknown Tail Number") \
        .otherwise(sf.col("TailNum")))


    # Remove non-ASCII characters
    df_parquet = df_parquet.withColumn(
        "TailNum",
        sf.regexp_replace("TailNum", "[^\x20-\x7E]", "")
    )


    # Define condition to create a new column
    missing_values = ['null', 'NULL', 'NA', 'NaN']

    cols_to_check = [
        "DepTime", "ArrTime", "ActualElapsedTime", 
        "AirTime", "ArrDelay", "DepDelay"
    ]

    conditions = [
        (sf.col(c).isNull()) | (sf.col(c).isin(missing_values))
        for c in cols_to_check
    ]


    # (((cond1 | cond2) | cond3) | cond4) | cond5) | cond6
    combined_condition = reduce(lambda a, b: a | b, conditions)


    # Create a new column based on our condition
    df_parquet = df_parquet.withColumn(
        "MissingData",
        sf.when(combined_condition, 1).otherwise(0)
    )

    # Fill in NULL values from CRS columns data
    df_parquet = df_parquet.withColumns({
        "DepTime":
        sf.when(sf.col("DepTime").isNull() | sf.col("DepTime").isin(missing_values), sf.col("CRSDepTime")).otherwise(sf.col("DepTime")),
        "ArrTime":
        sf.when(sf.col("ArrTime").isNull() | sf.col("ArrTime").isin(missing_values), sf.col("CRSArrTime")).otherwise(sf.col("ArrTime")),
        "ActualElapsedTime":
        sf.when(sf.col("ActualElapsedTime").isNull() | sf.col("ActualElapsedTime").isin(missing_values), sf.col("CRSElapsedTime")).otherwise(sf.col("ActualElapsedTime")),
        "DepDelay":
        sf.when(sf.col("DepDelay").isNull() | sf.col("DepDelay").isin(missing_values), "0").otherwise(sf.col("DepDelay")),
        "ArrDelay":
        sf.when(sf.col("ArrDelay").isNull() | sf.col("ArrDelay").isin(missing_values), "0").otherwise(sf.col("ArrDelay"))
    })

    # Filling Air Time
    df_parquet = df_parquet.withColumn(
        "AirTime",
        sf.when(sf.col("AirTime").isNull() | sf.col("AirTime").isin(missing_values), (sf.col("ActualElapsedTime").cast('int') - sf.col("TaxiIn").cast('int') - sf.col("TaxiOut").cast('int')).cast('string')).otherwise(sf.col("AirTime"))
    )

    # Columns to convert to integer
    int_cols = [
        "Year", "Month", "DayofMonth", "DayOfWeek",
        "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime",
        "FlightNum", "ActualElapsedTime", "CRSElapsedTime",
        "AirTime", "ArrDelay", "DepDelay", "Distance",
        "TaxiIn", "TaxiOut", "Cancelled", "Diverted"
    ]


    # Convert columns to int
    for c in int_cols:
        df_parquet = df_parquet.withColumn(c, sf.col(c).cast("int"))

    # Create Date column
    df_parquet = df_parquet.withColumn(
        "FlightDate",
        sf.to_date(
            sf.concat_ws("-", 
                        sf.col("Year"), 
                        sf.lpad(sf.col("Month").cast("string"), 2, "0"), 
                        sf.lpad(sf.col("DayOfMonth").cast("string"), 2, "0"))
        )
    )

    # Write partitioned, clustered data to BigQuery
    spark.conf.set('temporaryGcsBucket', 'data_expo_temp_bucket')
    output_dataset = "data-expo-pipeline.data_expo_dataset.final_data_table"

    df_parquet.write \
        .format("bigquery") \
        .option("table", output_dataset) \
        .option("partitionField", "FlightDate") \
        .option("clusteredFields", "Origin,Dest,UniqueCarrier") \
        .mode("overwrite") \
        .save()

    # Stop the Spark instance
    spark.stop()

with DAG(
    dag_id='fulldata_data_gcs_bigquery',
    schedule=None,
    start_date=datetime(2025, 8, 16),
    catchup=False,
    tags=['GCS', 'BigQuery', 'ETL']
) as dag:

    task_etl = PythonOperator(
        task_id='etl_fulldata',
        python_callable=etl_fulldata
    )