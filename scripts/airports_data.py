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
file_name = "airports_data.csv"
file_path = f"gs://{bucket_name}/{file_name}"


# Read CSV file
airports_data = spark.read.option("inferSchema", "true").option("header", "true").csv(file_path)


# Show the data
airports_data.show()


# Read file schema
airports_data.schema


# Rename columns
airports_data = airports_data.withColumnRenamed("iata", "IATA").withColumnRenamed("lat", "latitude").withColumnRenamed("long", "longitude")


# DataFrame with new columns
airports_data.show()


# Count number of records
airports_data.count()


# Test null in numeric columns
double_cols = airports_data.select(['latitude', 'longitude'])
double_cols.show()


double_cols_test_null = double_cols.select([sf.count(sf.when(sf.isnan(c) | sf.col(c).isNull(), c)).alias(c) for c in double_cols.columns])


double_cols_test_null.show()


# Test null in string columns
string_cols = airports_data.select(['IATA', 'airport', 'city', 'state', 'country'])
string_cols.show()


string_cols_test_null = string_cols.select([sf.count(sf.when(sf.col(c).isin(['null', 'NULL', 'NA', 'NaN']) | sf.col(c).isNull(), c)).alias(c) for c in string_cols.columns])


string_cols_test_null.show()


# Show null values
airports_data.filter(sf.col('city').isin(['null', 'NULL', 'NA', 'NaN']) | sf.col('city').isNull()).show()


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


# Check null values after filling NA values
airports_data.filter(sf.col('city').isin(['null', 'NULL', 'NA', 'NaN']) | sf.col('city').isNull()).show()


# Test some filled values
airports_data.filter((airports_data.IATA == 'CLD') | (airports_data.IATA == 'ROR')).show()


spark.conf.set('temporaryGcsBucket', 'data_expo_temp_bucket')
output_dataset = "data-expo-pipeline.data_expo_dataset.airport_table"
airports_data.write.format('bigquery').option('table', output_dataset).mode("overwrite").save()


# Stop the Spark instance
spark.stop()




