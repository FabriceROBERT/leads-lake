"""Gold: zone aggregates of silver/cabinet, for the map layers.

    docker compose -f docker-compose.spark.yml run --rm spark jobs/gold_cabinet_zone.py

Writes (partitionBy data_version, mode overwrite):
  s3a://<bucket>/gold/cabinet_par_departement/   departement, segment, nb
  s3a://<bucket>/gold/cabinet_par_commune/       code_commune, commune, departement, segment, nb, lat, lon
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    endpoint = os.environ["S3_ENDPOINT_URL"].split("://")[-1]
    return (
        SparkSession.builder.appName("gold_cabinet_zone")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def main() -> None:
    bucket = os.environ["LAKE_ROOT"].split("://", 1)[-1].split("/", 1)[0]
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    cab = spark.read.parquet(f"s3a://{bucket}/silver/cabinet")
    dv = cab.agg(F.max("data_version")).first()[0]
    cab = cab.filter(F.col("data_version") == dv)
    print(f"silver/cabinet data_version = {dv}")

    dep = (
        cab.groupBy("departement", "segment")
        .agg(F.count(F.lit(1)).alias("nb"))
        .withColumn("data_version", F.lit(dv))
    )
    (
        dep.repartition(1)
        .write.mode("overwrite")
        .partitionBy("data_version")
        .parquet(f"s3a://{bucket}/gold/cabinet_par_departement")
    )

    com = (
        cab.groupBy("code_commune", "commune", "departement", "segment")
        .agg(
            F.count(F.lit(1)).alias("nb"),
            F.avg("latitude").alias("lat"),
            F.avg("longitude").alias("lon"),
        )
        .withColumn("data_version", F.lit(dv))
    )
    (
        com.repartition(1)
        .write.mode("overwrite")
        .partitionBy("data_version")
        .parquet(f"s3a://{bucket}/gold/cabinet_par_commune")
    )

    print("\n=== par departement (top 10) ===")
    dep.orderBy(F.desc("nb")).show(10, truncate=False)
    print("=== par commune (top 10) ===")
    com.orderBy(F.desc("nb")).show(10, truncate=False)
    print(f"lignes departement: {dep.count()}   communes: {com.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()
