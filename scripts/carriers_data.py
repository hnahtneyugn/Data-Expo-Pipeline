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
file_name = "carriers_data.csv"
file_path = f"gs://{bucket_name}/{file_name}"


# Read CSV file
carriers_data = spark.read.option("inferSchema", "true").option("header", "true").csv(file_path)


# show the data
carriers_data.show(truncate=False)


# Count number of records
carriers_data.count()


# Count number of unique carrier code
carriers_data.select(sf.count(sf.col("Code"))).show()


# Read file schema
carriers_data.schema


# Test null values
carriers_data_test_null = carriers_data.select([sf.count(sf.when(sf.col(c).isin(['null', 'NULL', 'NA', 'NaN']) | sf.col(c).isNull(), c)).alias(c) for c in carriers_data.columns])

carriers_data_test_null.show()


# NA here can be a specific carrier code
carriers_data.filter(carriers_data.Code == 'NA').show(truncate=False)


# Data is clean, so no modification is needed
# Push data to BigQuery as Table
spark.conf.set('temporaryGcsBucket', 'data_expo_temp_bucket')
output_dataset = "data-expo-pipeline.data_expo_dataset.carrier_table"
carriers_data.write.format('bigquery').option('table', output_dataset).mode("overwrite").save()


# Stop the Spark instance
spark.stop()




