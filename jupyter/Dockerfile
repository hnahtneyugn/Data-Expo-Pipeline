FROM quay.io/jupyter/base-notebook

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

RUN mkdir -p /opt/bitnami/spark/jars/ && chown jovyan:users /opt/bitnami/spark/jars/

RUN wget -O /opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

RUN wget -O /opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.42.4.jar \
    https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases/download/0.42.4/spark-bigquery-with-dependencies_2.12-0.42.4.jar

USER jovyan
RUN pip install --no-cache-dir google-cloud-storage pyspark==3.5.6