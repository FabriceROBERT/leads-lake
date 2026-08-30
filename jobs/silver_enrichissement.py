"""Silver: flatten the on-demand enrichment Bronze into one row per SIREN.

    docker compose -f docker-compose.spark.yml run --rm spark jobs/silver_enrichissement.py

Reads
    s3a://<bucket>/bronze/source=recherche_entreprises/dataset=unites_legales/data_version=<latest>/
    s3a://<bucket>/bronze/source=bodacc/dataset=annonces/data_version=<latest>/
Writes
    s3a://<bucket>/silver/enrichissement/data_version=<dv>/   (+ _SUCCESS by Spark)

One firm-level row keyed by SIREN: financials (CA, CA N-1, croissance, résultat),
RNE attributes (catégorie, effectif, convention, dirigeant principal), BODACC
typed event dates, and the boolean `flag_*` columns consumed by
`gold_leads_scored` for the `signal_now` component and the caller flags.

Either Bronze source may be missing (backfill still running) — the job produces
whatever it can from the partitions that exist.
"""

from __future__ import annotations

import datetime as dt
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# BODACC modification wording that signals a governance change
_DIRIGEANT_RE = r"(?i)(dirigeant|g[ée]rant|pr[ée]sident|repr[ée]sentant|administrateur|directeur g[ée]n[ée]ral)"


def build_spark() -> SparkSession:
    endpoint = os.environ["S3_ENDPOINT_URL"].split("://")[-1]
    return (
        SparkSession.builder.appName("silver_enrichissement")
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


def _latest_partition(spark: SparkSession, bucket: str, rel: str) -> str | None:
    """Max `data_version=` under s3a://<bucket>/<rel>, or None if nothing there."""
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    base = jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{rel}")
    fs = base.getFileSystem(hconf)
    if not fs.exists(base):
        return None
    versions = sorted(
        st.getPath().getName().split("data_version=")[-1]
        for st in fs.listStatus(base)
        if "data_version=" in st.getPath().getName()
    )
    return versions[-1] if versions else None


def _read_ratios(spark: SparkSession, bucket: str) -> DataFrame | None:
    """bronze/source=ratios_financiers — bulk CA / résultat per siren (primary
    financial source; the recherche-entreprises API bans bulk callers)."""
    rel = "bronze/source=ratios_financiers/dataset=comptes"
    dv = _latest_partition(spark, bucket, rel)
    if not dv:
        print("no ratios_financiers Bronze partition — skipping bulk financials")
        return None
    print(f"ratios_financiers data_version = {dv}")
    df = spark.read.parquet(f"s3a://{bucket}/{rel}/data_version={dv}/*.parquet")
    return df.select(
        F.col("siren").cast("string").alias("siren"),
        F.col("ca").cast("long").alias("ca"),
        F.col("ca_n1").cast("long").alias("ca_n1"),
        F.col("resultat_net").cast("long").alias("resultat_net"),
        F.col("resultat_n1").cast("long").alias("resultat_n1"),
        F.col("annee_comptes").cast("int").alias("annee_comptes"),
        F.col("annee_comptes_n1").cast("int").alias("annee_comptes_n1"),
    ).dropDuplicates(["siren"])


def _read_rne(spark: SparkSession, bucket: str) -> DataFrame | None:
    rel = "bronze/source=recherche_entreprises/dataset=unites_legales"
    dv = _latest_partition(spark, bucket, rel)
    if not dv:
        print("no recherche_entreprises Bronze partition — skipping RNE display fields")
        return None
    print(f"recherche_entreprises data_version = {dv}")
    df = spark.read.parquet(f"s3a://{bucket}/{rel}/data_version={dv}/*.parquet")

    def col(name: str):
        return F.col(name) if name in df.columns else F.lit(None)

    # financials kept with a _rne suffix -> only a fallback behind ratios_financiers
    return df.select(
        F.col("siren").cast("string").alias("siren"),
        col("ca").cast("long").alias("ca_rne"),
        col("ca_n1").cast("long").alias("ca_n1_rne"),
        col("resultat_net").cast("long").alias("resultat_net_rne"),
        col("resultat_n1").cast("long").alias("resultat_n1_rne"),
        col("annee_comptes").cast("int").alias("annee_comptes_rne"),
        col("annee_comptes_n1").cast("int").alias("annee_comptes_n1_rne"),
        col("categorie_entreprise").alias("categorie_entreprise"),
        col("tranche_effectif_rne").alias("tranche_effectif_rne"),
        col("annee_effectif_rne").cast("string").alias("annee_effectif_rne"),
        col("nb_dirigeants").cast("int").alias("nb_dirigeants"),
        col("dirigeant_principal").alias("dirigeant_principal"),
        col("dirigeants").alias("dirigeants"),
        col("est_ess_rne").cast("boolean").alias("est_ess_rne"),
        col("est_qualiopi").cast("boolean").alias("est_qualiopi"),
        col("convention_collective").alias("convention_collective"),
        col("nb_etablissements_ouverts").cast("int").alias("nb_etablissements_ouverts"),
        col("date_maj_rne").cast("string").alias("date_maj_rne"),
    ).dropDuplicates(["siren"])


def _read_bodacc(spark: SparkSession, bucket: str) -> DataFrame | None:
    rel = "bronze/source=bodacc/dataset=annonces"
    dv = _latest_partition(spark, bucket, rel)
    if not dv:
        print("no bodacc Bronze partition — skipping BODACC")
        return None
    print(f"bodacc data_version = {dv}")
    df = spark.read.parquet(f"s3a://{bucket}/{rel}/data_version={dv}/*.parquet")

    def col(name: str):
        return F.col(name) if name in df.columns else F.lit(None)

    return df.select(
        F.col("siren").cast("string").alias("siren"),
        col("bodacc_en_procedure").cast("boolean").alias("bodacc_en_procedure"),
        col("bodacc_procedure_detail").alias("bodacc_procedure_detail"),
        F.to_date(col("bodacc_procedure_date")).alias("bodacc_procedure_date"),
        col("bodacc_a_depose_comptes").cast("boolean").alias("bodacc_a_depose_comptes"),
        F.to_date(col("bodacc_derniere_annonce")).alias("bodacc_derniere_annonce"),
        F.to_date(col("bodacc_derniere_modif_date")).alias("bodacc_derniere_modif_date"),
        col("bodacc_derniere_modif_detail").alias("bodacc_derniere_modif_detail"),
        F.to_date(col("bodacc_vente_cession_date")).alias("bodacc_vente_cession_date"),
        F.to_date(col("bodacc_radiation_date")).alias("bodacc_radiation_date"),
        F.to_date(col("bodacc_dernier_depot_date")).alias("bodacc_dernier_depot_date"),
        col("bodacc_nb_annonces_24m").cast("int").alias("bodacc_nb_annonces_24m"),
        col("bodacc_evenements").alias("bodacc_evenements"),
    ).dropDuplicates(["siren"])


def main() -> None:
    bucket = os.environ["LAKE_ROOT"].split("://", 1)[-1].split("/", 1)[0]
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    fin = _read_ratios(spark, bucket)
    rne = _read_rne(spark, bucket)
    bod = _read_bodacc(spark, bucket)
    parts = [p for p in (fin, rne, bod) if p is not None]
    if not parts:
        raise SystemExit("no enrichment Bronze at all — run ingestion.ratios_financiers / ingestion.bodacc first")

    df = parts[0]
    for p in parts[1:]:
        df = df.join(p, "siren", "full_outer")

    # financials: ratios_financiers (bulk) first, recherche-entreprises as fallback
    for c in ("ca", "ca_n1", "resultat_net", "resultat_n1", "annee_comptes", "annee_comptes_n1"):
        rne_c = f"{c}_rne"
        if c in df.columns and rne_c in df.columns:
            df = df.withColumn(c, F.coalesce(F.col(c), F.col(rne_c))).drop(rne_c)
        elif rne_c in df.columns:
            df = df.withColumnRenamed(rne_c, c)
    ca = F.col("ca").cast("double") if "ca" in df.columns else F.lit(None).cast("double")
    ca_n1 = F.col("ca_n1").cast("double") if "ca_n1" in df.columns else F.lit(None).cast("double")
    df = df.withColumn(
        "ca_croissance_pct",
        F.when(
            ca.isNotNull() & ca_n1.isNotNull() & (F.abs(ca_n1) > 0),
            F.round((ca - ca_n1) / F.abs(ca_n1), 4),
        ),
    )

    today = F.current_date()
    b = lambda c: F.coalesce(F.col(c), F.lit(False)) if c in df.columns else F.lit(False)  # noqa: E731
    d = lambda c: F.col(c) if c in df.columns else F.lit(None).cast("date")  # noqa: E731
    growth = F.col("ca_croissance_pct") if "ca_croissance_pct" in df.columns else F.lit(None)

    df = (
        df.withColumn("flag_redressement", b("bodacc_en_procedure"))
        .withColumn("flag_radiee", d("bodacc_radiation_date").isNotNull())
        .withColumn("flag_ca_hausse", F.coalesce(growth > 0.10, F.lit(False)))
        .withColumn("flag_ca_baisse", F.coalesce(growth < -0.10, F.lit(False)))
        .withColumn(
            "flag_nouveau_dirigeant",
            F.coalesce(
                d("bodacc_derniere_modif_date").isNotNull()
                & (d("bodacc_derniere_modif_date") >= F.add_months(today, -12))
                & F.col("bodacc_derniere_modif_detail").rlike(_DIRIGEANT_RE),
                F.lit(False),
            )
            if "bodacc_derniere_modif_detail" in df.columns
            else F.lit(False),
        )
        .withColumn(
            "flag_acquisition",
            F.coalesce(
                d("bodacc_vente_cession_date").isNotNull()
                & (d("bodacc_vente_cession_date") >= F.add_months(today, -9)),
                F.lit(False),
            ),
        )
        .withColumn(
            "flag_comptes_non_deposes",
            ~b("bodacc_a_depose_comptes") if "bodacc_a_depose_comptes" in df.columns else F.lit(False),
        )
    )

    data_version = dt.date.today().isoformat()
    df = df.withColumn("data_version", F.lit(data_version)).repartition(4)

    out = f"s3a://{bucket}/silver/enrichissement"
    df.write.mode("overwrite").partitionBy("data_version").parquet(out)

    n = df.count()
    print(f"\n=== silver/enrichissement : {n:,} SIREN  (data_version={data_version}) ===")
    df.select(
        F.sum(F.col("flag_redressement").cast("int")).alias("redressement"),
        F.sum(F.col("flag_ca_hausse").cast("int")).alias("ca_hausse"),
        F.sum(F.col("flag_ca_baisse").cast("int")).alias("ca_baisse"),
        F.sum(F.col("flag_nouveau_dirigeant").cast("int")).alias("nouveau_dirigeant"),
        F.sum(F.col("flag_acquisition").cast("int")).alias("acquisition"),
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
