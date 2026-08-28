"""Spark Structured Streaming: Kafka `france_travail.offres` -> bronze/source=france_travail/.

    docker compose -f docker-compose.spark.yml run --rm spark jobs/stream_france_travail.py
    docker compose -f docker-compose.spark.yml run --rm spark jobs/stream_france_travail.py --continuous

Default trigger = availableNow (drain what's in Kafka, then stop — Airflow-friendly).
--continuous = keep running, 30s micro-batches.

Bronze keeps the raw JSON payload untouched, + Kafka metadata + ingest_date partition.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    endpoint = os.environ["S3_ENDPOINT_URL"].split("://")[-1]
    return (
        SparkSession.builder.appName("stream_france_travail")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def main() -> None:
    bucket = os.environ["LAKE_ROOT"].split("://", 1)[-1].split("/", 1)[0]
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC_FT", "france_travail.offres")
    continuous = "--continuous" in sys.argv

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        # tolerate Kafka retention / topic recreation: resume from what's there,
        # Silver dedups on offre_id anyway
        .option("failOnDataLoss", "false")
        .load()
    )

    out = raw.select(
        F.col("key").cast("string").alias("offre_id"),
        F.col("value").cast("string").alias("json"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    ).withColumn("ingest_date", F.date_format(F.current_timestamp(), "yyyy-MM-dd"))

    writer = (
        out.writeStream.format("parquet")
        .option("path", f"s3a://{bucket}/bronze/source=france_travail")
        .option("checkpointLocation", f"s3a://{bucket}/_checkpoints/france_travail")
        .partitionBy("ingest_date")
        .outputMode("append")
    )
    writer = writer.trigger(processingTime="30 seconds") if continuous else writer.trigger(availableNow=True)

    query = writer.start()
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
