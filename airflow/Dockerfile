# Image Airflow 3 capable de soumettre les jobs PySpark au cluster Spark standalone.
# Le client Spark (binaires + JARs Iceberg/S3A + conf) et le JRE sont COPIÉS depuis l'image
# Spark officielle déjà présente localement : aucun téléchargement lourd supplémentaire,
# et versions strictement identiques entre le driver (Airflow) et les executors (spark-worker).
# Contexte de build = racine du dépôt (cf. docker-compose.yml).
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu AS spark

FROM apache/airflow:3.3.2-python3.10

USER root
COPY --from=spark /opt/spark /opt/spark
COPY --from=spark /opt/java/openjdk /opt/java/openjdk
COPY spark/jars/*.jar /opt/spark/jars/
COPY spark/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
RUN chmod -R a+rX /opt/spark /opt/java/openjdk \
    && apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*          # `ps` requis par les scripts spark-submit

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:/opt/java/openjdk/bin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/waba

USER airflow
COPY airflow/requirements.txt /tmp/requirements.txt
# apache-airflow est ré-épinglé pour qu'aucun provider ne puisse le mettre à jour implicitement
RUN pip install --no-cache-dir --retries 10 --timeout 60 "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
