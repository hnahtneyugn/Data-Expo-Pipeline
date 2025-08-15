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
file_name = "variable_descriptions_data.csv"
file_path = f"gs://{bucket_name}/{file_name}"


# Read CSV file
vardes_data = spark.read.option("inferSchema", "true").option("header", "true").csv(file_path)


# Show the data
vardes_data.show(truncate=False)


# Read file schema
vardes_data.schema


# Count number of records
vardes_data.count()


# Rename the DataFrame
vardes_data = vardes_data.withColumnRenamed("_c1", "Name").withColumnRenamed("_c2", "Description")


# DataFrame after renaming
vardes_data.show(truncate=False)


# Remove rows with NULL
vardes_data = vardes_data.filter(sf.col("Variable descriptions").isNotNull())


# DataFrame after removing NULL values
vardes_data.show(truncate=False)


# Number of records after removing NULL values
vardes_data.count()


# Push data after modification to BigQuery as Table
spark.conf.set('temporaryGcsBucket', 'data_expo_temp_bucket')
output_dataset = "data-expo-pipeline.data_expo_dataset.variable_description_table"
vardes_data.write.format('bigquery').option('table', output_dataset).mode("overwrite").save()


spark.stop()




