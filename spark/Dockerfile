# Image Spark 3.5 + Iceberg 1.6 + connecteur S3A (lecture des CSV bruts sur MinIO)
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
ARG ICEBERG_VERSION=1.6.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG MVN=https://repo1.maven.org/maven2

ADD --chmod=644 ${MVN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar /opt/spark/jars/

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY waba_spark /opt/waba/waba_spark
COPY jobs /opt/waba/jobs

ENV PYTHONPATH=/opt/waba \
    PYSPARK_PYTHON=python3 \
    SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:${PATH}"
WORKDIR /opt/waba
USER spark
