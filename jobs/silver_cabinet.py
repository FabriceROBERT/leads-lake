"""Silver: build `silver/cabinet/` from the Bronze SIRENE Parquet files.

Run inside the Spark container (see docker-compose.spark.yml):
    docker compose -f docker-compose.spark.yml run --rm spark

Reads   s3a://<bucket>/bronze/source=sirene/dataset={stock_unite_legale,stock_etablissement}/
        data_version=<latest>/*.parquet
Writes  s3a://<bucket>/silver/cabinet/data_version=<dv>/   (+ _SUCCESS written by Spark)

Filter: établissements SIEGE, actifs, APE NAFRev2 in the 4 target codes,
joined to their active unité légale on SIREN.
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
from pyproj import Transformer
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StructField, StructType

_LATLON = StructType([StructField("lat", DoubleType()), StructField("lon", DoubleType())])

# INSEE stores the SIRENE geolocation in Lambert-93 for métropole, but in a
# *local* projection for each DOM. Reprojecting a DOM point as if it were
# Lambert-93 throws it into the North Sea / Gulf of Guinea. Source EPSG by
# département code (first 3 digits of codeCommune):
#   971/977/978 Guadeloupe, St-Barth, St-Martin -> RGAF09 / UTM 20N   (5490)
#   972         Martinique                       -> RGAF09 / UTM 20N   (5490)
#   973         Guyane                           -> RGFG95 / UTM 22N   (2972)
#   974         La Réunion                       -> RGR92  / UTM 40S   (2975)
#   976         Mayotte                          -> RGM04  / UTM 38S   (4471)
SRC_EPSG_METROPOLE = 2154
DOM_SRC_EPSG = {
    "971": 5490, "972": 5490, "977": 5490, "978": 5490,
    "973": 2972,
    "974": 2975,
    "976": 4471,
}


@pandas_udf(_LATLON)
def to_wgs84(x: pd.Series, y: pd.Series, epsg: pd.Series) -> pd.DataFrame:
    """Projected (x, y) in source CRS `epsg` -> WGS84 lon/lat (EPSG:4326)."""
    xs = pd.to_numeric(x, errors="coerce").to_numpy(dtype="float64")
    ys = pd.to_numeric(y, errors="coerce").to_numpy(dtype="float64")
    src = pd.to_numeric(epsg, errors="coerce").fillna(SRC_EPSG_METROPOLE).astype(int).to_numpy()
    lat = np.full(xs.shape, np.nan)
    lon = np.full(xs.shape, np.nan)
    cache: dict[int, Transformer] = {}
    for code in np.unique(src):
        m = src == code
        tr = cache.get(int(code))
        if tr is None:
            tr = Transformer.from_crs(int(code), 4326, always_xy=True)
            cache[int(code)] = tr
        lo, la = tr.transform(xs[m], ys[m])
        lon[m] = lo
        lat[m] = la
    # null-island guard: INSEE writes 0/blank for ungeocoded rows
    bad = (
        ~np.isfinite(lat)
        | ~np.isfinite(lon)
        | ((np.abs(lat) < 0.01) & (np.abs(lon) < 0.01))
    )
    lat = np.where(bad, np.nan, lat)
    lon = np.where(bad, np.nan, lon)
    return pd.DataFrame({"lat": lat, "lon": lon})

# NAF de rattachement -> segment métier (voir README). 69.10Z est aussi utilisé
# par des promoteurs, mais on le classe avocat_notaire (usage dominant).
SEGMENT = {
    "41.10A": "promoteur",
    "41.10B": "promoteur",
    "41.10C": "promoteur",
    "82.11Z": "domiciliation",
    "74.90B": "domiciliation",
    "69.10Z": "avocat_notaire",
    "69.20Z": "expert_comptable",
    "66.12Z": "cgp",
    "66.19B": "cgp",
}
NAF = list(SEGMENT)


def build_spark() -> SparkSession:
    endpoint = os.environ["S3_ENDPOINT_URL"].split("://")[-1]
    return (
        SparkSession.builder.appName("silver_cabinet")
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
        # data.gouv Parquet holds pre-1900 placeholder dates written with the
        # legacy calendar -> read/write the stored values as-is.
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )


def latest_data_version(spark: SparkSession, bucket: str, dataset: str) -> str:
    """List bronze/source=sirene/dataset=<dataset>/ via the Hadoop FS and return the max data_version."""
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    base = jvm.org.apache.hadoop.fs.Path(
        f"s3a://{bucket}/bronze/source=sirene/dataset={dataset}"
    )
    fs = base.getFileSystem(hconf)
    versions = sorted(
        st.getPath().getName().split("data_version=")[-1]
        for st in fs.listStatus(base)
        if "data_version=" in st.getPath().getName()
    )
    if not versions:
        raise SystemExit(f"No Bronze partition under s3a://{bucket}/bronze/source=sirene/dataset={dataset}")
    return versions[-1]


def read_bronze(spark: SparkSession, bucket: str, dataset: str, dv: str):
    df = spark.read.parquet(
        f"s3a://{bucket}/bronze/source=sirene/dataset={dataset}/data_version={dv}/*.parquet"
    )
    # SIRENE masks non-diffusible rows with the literal string "[ND]" -> null it
    return df.select(
        *[
            F.when(F.col(c) == "[ND]", F.lit(None)).otherwise(F.col(c)).alias(c)
            if t == "string"
            else F.col(c)
            for c, t in df.dtypes
        ]
    )


def main() -> None:
    # image ships Python 3.8: no str.removeprefix
    bucket = os.environ["LAKE_ROOT"].split("://", 1)[-1].split("/", 1)[0]

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    dv_ul = latest_data_version(spark, bucket, "stock_unite_legale")
    dv_etab = latest_data_version(spark, bucket, "stock_etablissement")
    if dv_ul != dv_etab:
        raise SystemExit(f"data_version mismatch: unite_legale={dv_ul} etablissement={dv_etab}")
    data_version = dv_ul
    print(f"Bronze data_version = {data_version}")

    ul = read_bronze(spark, bucket, "stock_unite_legale", data_version)
    etab = read_bronze(spark, bucket, "stock_etablissement", data_version)

    # every ACTIVE establishment on a target NAF (not just the siège) -> a branch
    # office of a network matches the offer posted there
    etab_f = etab.filter(
        F.col("activitePrincipaleEtablissement").isin(NAF)
        & (F.col("nomenclatureActivitePrincipaleEtablissement") == "NAFRev2")
        & (F.col("etatAdministratifEtablissement") == "A")
    ).select(
        "siren",
        "siret",
        "activitePrincipaleEtablissement",
        (F.col("etablissementSiege") == "true").alias("est_siege"),
        "numeroVoieEtablissement",
        "indiceRepetitionEtablissement",
        "typeVoieEtablissement",
        "libelleVoieEtablissement",
        "complementAdresseEtablissement",
        "codePostalEtablissement",
        "libelleCommuneEtablissement",
        "codeCommuneEtablissement",
        "enseigne1Etablissement",
        "coordonneeLambertAbscisseEtablissement",
        "coordonneeLambertOrdonneeEtablissement",
        "identifiantAdresseEtablissement",
        "statutDiffusionEtablissement",
        F.col("dateDebut").alias("dateDebutEtab"),
    )

    ul_f = ul.filter(F.col("etatAdministratifUniteLegale") == "A").select(
        "siren",
        "denominationUniteLegale",
        "nomUniteLegale",
        "prenomUsuelUniteLegale",
        "categorieJuridiqueUniteLegale",
        "dateCreationUniteLegale",
        "trancheEffectifsUniteLegale",
        "economieSocialeSolidaireUniteLegale",
        "statutDiffusionUniteLegale",
    )

    joined = ul_f.join(F.broadcast(etab_f), "siren", "inner")

    # forme juridique label from the official INSEE nomenclature (niveau III)
    cj = spark.read.parquet(f"s3a://{bucket}/silver/_reference/categories_juridiques")
    joined = joined.join(
        F.broadcast(cj), joined["categorieJuridiqueUniteLegale"] == cj["code"], "left"
    )

    seg_map = F.create_map([F.lit(x) for kv in SEGMENT.items() for x in kv])

    dep3 = F.col("codeCommuneEtablissement").substr(1, 3)
    departement = F.when(
        F.col("codeCommuneEtablissement").rlike("^9[78]"), dep3
    ).otherwise(F.col("codeCommuneEtablissement").substr(1, 2))

    src_epsg = F.lit(SRC_EPSG_METROPOLE)
    for code, epsg in DOM_SRC_EPSG.items():
        src_epsg = F.when(dep3 == code, F.lit(epsg)).otherwise(src_epsg)

    creation = F.to_date("dateCreationUniteLegale")

    shaped = joined.select(
        F.col("siren"),
        F.col("siret"),
        F.col("est_siege"),
        F.coalesce(
            F.col("denominationUniteLegale"),
            F.nullif(F.trim(F.concat_ws(" ", "nomUniteLegale", "prenomUsuelUniteLegale")), F.lit("")),
        ).alias("raison_sociale"),
        F.coalesce(seg_map[F.col("activitePrincipaleEtablissement")], F.lit("autre")).alias("segment"),
        F.col("activitePrincipaleEtablissement").alias("code_ape"),
        F.col("categorieJuridiqueUniteLegale").alias("categorie_juridique"),
        F.coalesce(
            F.col("libelle"),
            F.concat(F.lit("Cat. "), F.col("categorieJuridiqueUniteLegale")),
        ).alias("forme_juridique"),
        creation.alias("date_creation"),
        (F.year(F.current_date()) - F.year(creation)).alias("anciennete_annees"),
        F.col("trancheEffectifsUniteLegale").alias("tranche_effectif"),
        (F.col("economieSocialeSolidaireUniteLegale") == "O").alias("est_ess"),
        F.nullif(
            F.trim(
                F.concat_ws(
                    " ",
                    "numeroVoieEtablissement",
                    "indiceRepetitionEtablissement",
                    "typeVoieEtablissement",
                    "libelleVoieEtablissement",
                )
            ),
            F.lit(""),
        ).alias("adresse"),
        F.col("complementAdresseEtablissement").alias("complement_adresse"),
        F.col("codePostalEtablissement").alias("code_postal"),
        F.col("libelleCommuneEtablissement").alias("commune"),
        F.col("codeCommuneEtablissement").alias("code_commune"),
        departement.alias("departement"),
        F.col("enseigne1Etablissement").alias("enseigne"),
        F.col("coordonneeLambertAbscisseEtablissement").cast("double").alias("x_lambert93"),
        F.col("coordonneeLambertOrdonneeEtablissement").cast("double").alias("y_lambert93"),
        src_epsg.alias("src_epsg"),
        F.col("identifiantAdresseEtablissement").alias("ban_id"),
        (
            (F.col("statutDiffusionUniteLegale") == "O")
            & (F.col("statutDiffusionEtablissement") == "O")
        ).alias("est_diffusible"),
        F.col("dateDebutEtab"),
    )

    shaped = (
        shaped.withColumn(
            "_ll",
            to_wgs84(F.col("x_lambert93"), F.col("y_lambert93"), F.col("src_epsg")),
        )
        .withColumn("latitude", F.col("_ll.lat"))
        .withColumn("longitude", F.col("_ll.lon"))
        .drop("_ll", "src_epsg")
    )

    # sanity box: métropole + DOM. A point that lands outside its territory's
    # window (bad source row, wrong CRS) is dropped rather than mismapped.
    is_dom = F.col("code_commune").rlike("^97[1-8]")
    in_metropole = (
        F.col("longitude").between(-5.6, 10.2) & F.col("latitude").between(41.0, 51.6)
    )
    in_dom = (
        (F.col("longitude").between(-63.3, -50.5) & F.col("latitude").between(1.8, 18.3))
        | (F.col("longitude").between(44.8, 56.1) & F.col("latitude").between(-21.5, -12.5))
    )
    keep_geo = F.when(is_dom, in_dom).otherwise(in_metropole)
    shaped = shaped.withColumn(
        "latitude", F.when(keep_geo, F.col("latitude"))
    ).withColumn("longitude", F.when(keep_geo, F.col("longitude")))

    # one row per SIRET: keep the most recent establishment period
    latest = Window.partitionBy("siret").orderBy(F.col("dateDebutEtab").desc_nulls_last())
    cabinet = (
        shaped.withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "dateDebutEtab")
        .withColumn("data_version", F.lit(data_version))
    )

    cabinet = cabinet.persist()
    total = cabinet.count()
    n_siege = cabinet.filter(F.col("est_siege")).count()

    target = f"s3a://{bucket}/silver/cabinet"
    (
        cabinet.repartition(4)
        .write.mode("overwrite")
        .partitionBy("data_version")
        .parquet(target)
    )

    print(f"\n=== silver/cabinet/data_version={data_version} : {total:,} établissements ({n_siege:,} sièges) ===")
    cabinet.groupBy("segment").count().orderBy(F.desc("count")).show(truncate=False)
    cabinet.groupBy("departement").count().orderBy(F.desc("count")).show(10, truncate=False)
    cabinet.groupBy("forme_juridique").count().orderBy(F.desc("count")).show(12, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
