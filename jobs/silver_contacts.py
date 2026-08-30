"""Silver: bronze/source=crawl/dataset=contacts -> silver/contacts (1 row / siren).

    docker compose -f docker-compose.spark.yml run --rm spark jobs/silver_contacts.py

Across every crawl run, keep the freshest record per siren, preferring one where
the target SIREN was verified on the site (`/mentions-legales`).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    endpoint = os.environ["S3_ENDPOINT_URL"].split("://")[-1]
    return (
        SparkSession.builder.appName("silver_contacts")
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

    src = f"s3a://{bucket}/bronze/source=crawl/dataset=contacts"
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(src)
    if not p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p):
        raise SystemExit("no bronze/source=crawl yet — run ingestion.crawl_worker first")

    df = spark.read.parquet(src)
    w = Window.partitionBy("siren").orderBy(
        F.desc(F.col("siren_verifie_sur_site").cast("int")),
        F.desc("crawled_at"),
    )
    best = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1)

    out = best.select(
        F.col("siren").cast("string").alias("siren"),
        F.element_at("telephones", 1).alias("telephone"),
        F.element_at("emails", 1).alias("email"),
        F.col("telephones"),
        F.col("emails"),
        F.col("domaine").alias("site_web"),
        F.col("url_source"),
        F.coalesce(F.col("siren_verifie_sur_site"), F.lit(False)).alias("contact_verifie"),
        F.to_timestamp("crawled_at").alias("contact_crawled_at"),
    )

    out.repartition(1).write.mode("overwrite").parquet(f"s3a://{bucket}/silver/contacts")

    n = out.count()
    v = out.filter(F.col("contact_verifie")).count()
    tel = out.filter(F.col("telephone").isNotNull()).count()
    print(f"\n=== silver/contacts : {n:,} sirens  ({v:,} vérifiés, {tel:,} avec téléphone) ===")
    out.filter(F.col("contact_verifie")).select(
        "siren", "site_web", "telephone", "email"
    ).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
