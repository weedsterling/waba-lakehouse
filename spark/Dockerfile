# Image Spark 3.5 + Iceberg 1.6 + connecteur S3A (lecture des CSV bruts sur MinIO)
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# JARs Iceberg + S3A pré-téléchargés et vérifiés (SHA-1) par scripts/download-jars.sh :
# build reproductible et indépendant de la qualité du réseau.
COPY jars/*.jar /opt/spark/jars/
RUN for j in iceberg-spark-runtime-3.5_2.12-1.6.1 iceberg-aws-bundle-1.6.1 hadoop-aws-3.3.4 aws-java-sdk-bundle-1.12.262; do \
      test -s /opt/spark/jars/$j.jar || { echo "JAR manquant : $j — lancer ./scripts/download-jars.sh"; exit 1; }; \
    done && chmod 644 /opt/spark/jars/*.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --retries 10 --timeout 60 -r /tmp/requirements.txt

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY waba_spark /opt/waba/waba_spark
COPY jobs /opt/waba/jobs

ENV PYTHONPATH=/opt/waba \
    PYSPARK_PYTHON=python3 \
    SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:${PATH}"
WORKDIR /opt/waba
USER spark
