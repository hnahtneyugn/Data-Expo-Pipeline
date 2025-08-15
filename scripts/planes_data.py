import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as sf

spark = SparkSession \
        .builder \
        .master("spark://spark-master:7077") \
        .config(
        "spark.jars",
        "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar,"
        "/opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.42.4.jar"
        ) \
        .appName('gcs-bq-pyspark') \
        .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', "/opt/keys/credentials.json")


# File information
bucket_name = "data_expo_bucket"
file_name = "plane_data.csv"
file_path = f"gs://{bucket_name}/{file_name}"


# Read CSV file
planes_data = spark.read.option("inferSchema", "true").option("header", "true").csv(file_path)


# Show the data
planes_data.show()


# Read file schema
planes_data.schema


# Count number of records
planes_data.count()


# Check the number of null in each column
planes_data_test_null = planes_data.select([sf.count(sf.when(sf.col(c).isin(['null', 'NULL', 'NA', 'NaN']) | sf.col(c).isNull(), c)).alias(c) for c in planes_data.columns])


planes_data_test_null.show()


# Filter NULL values and show result
planes_data = planes_data.filter(planes_data.type.isNotNull())


planes_data.show(truncate=False)


# Check number of NULL values after removing them
planes_data.select([sf.count(sf.when(sf.col(c).isin(['null', 'NULL', 'NA', 'NaN']) | sf.col(c).isNull(), c)).alias(c) for c in planes_data.columns]).show()


# Check number of records after removing NULL values
planes_data.count()


# Check unique tailnum values
planes_data.select(sf.count(sf.col('tailnum'))).show()


# Convert columns to suitable data type
planes_data = planes_data.withColumns({'issue_date': sf.to_date(planes_data.issue_date, "MM/dd/yyyy"), 'year': sf.col('year').cast('int')})


planes_data.show(truncate=False)


# Check schema after type casting
planes_data.schema


# Clean data, can be uploaded to BigQuery as Table
spark.conf.set('temporaryGcsBucket', 'data_expo_temp_bucket')
output_dataset = "data-expo-pipeline.data_expo_dataset.plane_table"
planes_data.write.format('bigquery').option('table', output_dataset).mode("overwrite").save()


spark.stop()





