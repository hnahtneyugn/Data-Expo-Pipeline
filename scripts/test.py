import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

spark = SparkSession \
        .builder \
        .appName('gcs-pyspark') \
        .getOrCreate()

spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', "/opt/keys/credentials.json")

bucket_name = "data_expo_bucket"
file_name = "taxi_zone_lookup.csv"
file_path = f"gs://{bucket_name}/{file_name}"

df = spark.read.option("inferSchema", "true").option("header", "true").csv(file_path)
df.count()
df.head(5)

spark.stop()