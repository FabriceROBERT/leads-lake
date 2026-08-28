# Spark + the two jars needed to read/write s3a:// (Wasabi).
# Spark 3.5.3 bundles Hadoop 3.3.4 -> matching hadoop-aws / aws-java-sdk-bundle.
FROM spark:3.5.3-python3

USER root

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG SPARK_VERSION=3.5.3
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl; \
    rm -rf /var/lib/apt/lists/*; \
    cd /opt/spark/jars; \
    M=https://repo1.maven.org/maven2; \
    curl -fLO "$M/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fLO "$M/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    curl -fLO "$M/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fLO "$M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fLO "$M/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fLO "$M/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"

# pyproj: Lambert-93 -> WGS84 in a pandas_udf. pandas/pyarrow: Arrow UDFs.
# Versions pinned for the image's Python 3.8.
RUN pip install --no-cache-dir "pyproj==3.5.0" "pandas==2.0.3" "pyarrow==15.0.2"

# bake the jobs in (Airflow runs this image without bind mounts); local dev still
# overrides ./jobs via docker-compose.spark.yml
COPY jobs/ /opt/spark/work-dir/jobs/

USER spark
WORKDIR /opt/spark/work-dir
