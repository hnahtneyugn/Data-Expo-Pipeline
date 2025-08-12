FROM bitnami/spark:3.5.6

USER root

RUN install_packages curl

USER 1001

RUN curl https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
     --output /opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar

RUN curl https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases/download/0.42.4/spark-bigquery-with-dependencies_2.12-0.42.4.jar \
     --output /opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.42.4.jar
