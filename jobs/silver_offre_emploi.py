"""Silver: parse bronze/source=france_travail -> silver/offre_emploi.

    docker compose -f docker-compose.spark.yml run --rm spark jobs/silver_offre_emploi.py

- parse the raw FT JSON
- resolve `siren` by fuzzy-matching entreprise.nom vs silver/cabinet (raison_sociale
  / enseigne), disambiguated by commune then département -> `niveau_rattachement`
- tag `metier` from the job title
- dedup on offre_id (latest kafka_ts)
- overwrite silver/offre_emploi (small table, full refresh)
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

OFFRE_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("intitule", StringType()),
        StructField("description", StringType()),
        StructField("dateCreation", StringType()),
        StructField("dateActualisation", StringType()),
        StructField("romeCode", StringType()),
        StructField("romeLibelle", StringType()),
        StructField("appellationlibelle", StringType()),
        StructField("typeContrat", StringType()),
        StructField("natureContrat", StringType()),
        StructField("experienceExige", StringType()),
        StructField("experienceLibelle", StringType()),
        StructField("alternance", BooleanType()),
        StructField("nombrePostes", IntegerType()),
        StructField("dureeTravailLibelleConverti", StringType()),
        StructField("qualificationLibelle", StringType()),
        StructField("codeNAF", StringType()),
        StructField("secteurActivite", StringType()),
        StructField("secteurActiviteLibelle", StringType()),
        StructField("trancheEffectifEtab", StringType()),
        StructField(
            "entreprise",
            StructType([StructField("nom", StringType()), StructField("description", StringType())]),
        ),
        StructField(
            "lieuTravail",
            StructType(
                [
                    StructField("libelle", StringType()),
                    StructField("latitude", DoubleType()),
                    StructField("longitude", DoubleType()),
                    StructField("codePostal", StringType()),
                    StructField("commune", StringType()),
                ]
            ),
        ),
        StructField("salaire", StructType([StructField("libelle", StringType())])),
        StructField("origineOffre", StructType([StructField("urlOrigine", StringType())])),
    ]
)

_LEGAL = (
    r"\b(SELARL|SELAS|SELAFA|SELCA|SARL|SASU|SAS|EURL|SCP|SPFPL|SA|SCI|SCM|SNC|AARPI|"
    r"CABINET|CAB|ETUDE|MAITRE|SOCIETE|STE|GROUPE|HOLDING)\b"
)

GRID_DEG = 0.003  # ~330 m N-S / ~230 m E-W at French latitudes


def _cell(lat, lon):
    return F.concat_ws("_", F.floor(lat / GRID_DEG).cast("int"), F.floor(lon / GRID_DEG).cast("int"))


def _haversine_m(lat1, lon1, lat2, lon2):
    dphi = F.radians(lat2 - lat1)
    dlmb = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dphi / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlmb / 2), 2)
    return F.lit(6371000.0) * 2 * F.asin(F.sqrt(a))


def _jaccard(a_str, b_str):
    ta = F.array_distinct(F.split(a_str, " "))
    tb = F.array_distinct(F.split(b_str, " "))
    inter = F.size(F.array_intersect(ta, tb))
    uni = F.size(F.array_union(ta, tb))
    return F.when(uni > 0, inter / uni).otherwise(F.lit(0.0))


def build_spark() -> SparkSession:
    endpoint = os.environ["S3_ENDPOINT_URL"].split("://")[-1]
    return (
        SparkSession.builder.appName("silver_offre_emploi")
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


def norm_name(col):
    c = F.upper(F.coalesce(col, F.lit("")))
    c = F.translate(c, "ÀÁÂÃÄÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝ", "AAAAACEEEEIIIINOOOOOUUUUY")
    c = F.regexp_replace(c, _LEGAL, " ")
    c = F.regexp_replace(c, r"[^A-Z0-9]+", " ")
    c = F.trim(F.regexp_replace(c, r"\s+", " "))
    return c


def tag_metier(intitule, appellation):
    t = F.lower(F.concat_ws(" ", F.coalesce(intitule, F.lit("")), F.coalesce(appellation, F.lit(""))))
    return (
        F.when(t.rlike("paie|paye"), "paie")
        .when(t.rlike("comptab|audit|expertise comptable|commissaire aux comptes"), "comptabilite")
        .when(t.rlike("jurist|juridiqu|clerc|avocat|notari|fiscalist|paralegal"), "juridique")
        .when(t.rlike("patrimoin|patrimonial|gestion de fortune|allocation d.?actifs"), "patrimoine")
        .when(t.rlike("immobili|promoti|metreur|m.treur|vefa|fonci|programme neuf|maitrise d.?ouvrage"), "immobilier")
        .otherwise("autre")
    )


def main() -> None:
    bucket = os.environ["LAKE_ROOT"].split("://", 1)[-1].split("/", 1)[0]
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.parquet(f"s3a://{bucket}/bronze/source=france_travail")
    # keep the most recent copy of each offre_id
    w_last = Window.partitionBy("offre_id").orderBy(F.col("kafka_ts").desc())
    bronze = (
        bronze.withColumn("_r", F.row_number().over(w_last))
        .filter(F.col("_r") == 1)
        .drop("_r")
    )

    o = F.from_json(F.col("json"), OFFRE_SCHEMA)
    code_commune = o["lieuTravail"]["commune"]
    dept = F.when(code_commune.rlike("^9[78]"), code_commune.substr(1, 3)).otherwise(code_commune.substr(1, 2))

    offres = bronze.select(
        F.col("offre_id"),
        o["intitule"].alias("intitule"),
        tag_metier(o["intitule"], o["appellationlibelle"]).alias("metier"),
        F.to_timestamp(o["dateCreation"]).alias("date_creation"),
        F.to_timestamp(o["dateActualisation"]).alias("date_actualisation"),
        o["romeCode"].alias("code_rome"),
        o["appellationlibelle"].alias("appellation"),
        o["typeContrat"].alias("type_contrat"),
        o["experienceExige"].alias("experience_exige"),
        F.coalesce(o["alternance"], F.lit(False)).alias("alternance"),
        o["nombrePostes"].alias("nombre_postes"),
        o["dureeTravailLibelleConverti"].alias("duree_travail"),
        o["codeNAF"].alias("code_ape"),
        o["trancheEffectifEtab"].alias("tranche_effectif_etab"),
        o["entreprise"]["nom"].alias("entreprise_nom"),
        o["lieuTravail"]["libelle"].alias("lieu_libelle"),
        o["lieuTravail"]["latitude"].alias("latitude"),
        o["lieuTravail"]["longitude"].alias("longitude"),
        o["lieuTravail"]["codePostal"].alias("code_postal"),
        code_commune.alias("code_commune"),
        dept.alias("departement"),
        o["salaire"]["libelle"].alias("salaire_libelle"),
        o["origineOffre"]["urlOrigine"].alias("url_origine"),
        o["description"].alias("description"),
        F.col("ingest_date"),
    ).withColumn("n_emp", norm_name(F.col("entreprise_nom")))

    # --- cabinet reference points (all establishments, latest data_version) ---
    cab = spark.read.parquet(f"s3a://{bucket}/silver/cabinet")
    cab = cab.filter(F.col("data_version") == cab.agg(F.max("data_version")).first()[0])
    cab_pts = cab.select(
        "siren",
        "siret",
        norm_name(F.col("raison_sociale")).alias("n_raison"),
        norm_name(F.col("enseigne")).alias("n_enseigne"),
        F.col("latitude").alias("cab_lat"),
        F.col("longitude").alias("cab_lon"),
        F.col("code_commune").alias("cab_commune"),
        F.col("departement").alias("cab_dept"),
    ).withColumn("cab_cell", _cell(F.col("cab_lat"), F.col("cab_lon")))

    CAND_COLS = [
        "offre_id", "siren", "siret", "n_emp", "n_raison", "n_enseigne",
        "o_commune", "o_dept", "cab_commune", "cab_dept", "dist_m",
    ]

    # geo candidates: offer's 3x3 grid cells x cabinet cell, then haversine <= 250 m
    deltas = [(dx, dy) for dx in (-1, 0, 1) for dy in (-1, 0, 1)]
    off_geo = offres.filter(F.col("latitude").isNotNull())
    cx = F.floor(F.col("latitude") / GRID_DEG).cast("int")
    cy = F.floor(F.col("longitude") / GRID_DEG).cast("int")
    off_cells = off_geo.select(
        "offre_id", "n_emp",
        F.col("latitude").alias("o_lat"), F.col("longitude").alias("o_lon"),
        F.col("code_commune").alias("o_commune"), F.col("departement").alias("o_dept"),
        F.explode(F.array(*[F.concat_ws("_", cx + dx, cy + dy) for dx, dy in deltas])).alias("cell"),
    )
    geo = (
        off_cells.join(F.broadcast(cab_pts), off_cells["cell"] == cab_pts["cab_cell"])
        .withColumn("dist_m", _haversine_m(F.col("o_lat"), F.col("o_lon"), F.col("cab_lat"), F.col("cab_lon")))
        .filter(F.col("dist_m") <= 250)
        .select(*CAND_COLS)
    )

    # name candidates: exact normalized name (raison or enseigne), any location
    name_c = (
        offres.select(
            "offre_id", "n_emp",
            F.col("code_commune").alias("o_commune"), F.col("departement").alias("o_dept"),
            F.col("latitude").alias("o_lat"), F.col("longitude").alias("o_lon"),
        )
        .join(
            F.broadcast(cab_pts),
            (F.col("n_emp") == F.col("n_raison"))
            | ((F.col("n_emp") == F.col("n_enseigne")) & (F.col("n_enseigne") != "")),
        )
        .withColumn(
            "dist_m",
            F.when(
                F.col("o_lat").isNotNull() & F.col("cab_lat").isNotNull(),
                _haversine_m(F.col("o_lat"), F.col("o_lon"), F.col("cab_lat"), F.col("cab_lon")),
            ),
        )
        .select(*CAND_COLS)
    )

    cand = (
        geo.unionByName(name_c)
        .withColumn("name_exact", ((F.col("n_emp") == F.col("n_raison")) | (F.col("n_emp") == F.col("n_enseigne"))).cast("int"))
        .withColumn("jac", _jaccard(F.col("n_emp"), F.col("n_raison")))
        .groupBy("offre_id", "siret")
        .agg(
            F.first("siren", ignorenulls=True).alias("siren"),
            F.min("dist_m").alias("dist_m"),
            F.max("name_exact").alias("name_exact"),
            F.max("jac").alias("jac"),
            F.max((F.col("o_commune") == F.col("cab_commune")).cast("int")).alias("same_commune"),
            F.max((F.col("o_dept") == F.col("cab_dept")).cast("int")).alias("same_dept"),
        )
        .withColumn(
            "niveau_rattachement",
            F.when((F.col("dist_m") <= 200) & ((F.col("name_exact") == 1) | (F.col("jac") >= 0.4)), "geo_nom")
            .when((F.col("name_exact") == 1) & (F.col("same_commune") == 1), "nom_commune")
            .when(F.col("dist_m") <= 120, "geo_seul")
            .when((F.col("name_exact") == 1) & (F.col("same_dept") == 1), "nom_dept")
            .when(F.col("name_exact") == 1, "nom_seul")
            .otherwise("faible"),
        )
    )

    tier = (
        F.when(F.col("niveau_rattachement") == "geo_nom", 5)
        .when(F.col("niveau_rattachement") == "nom_commune", 4)
        .when(F.col("niveau_rattachement") == "geo_seul", 3)
        .when(F.col("niveau_rattachement") == "nom_dept", 2)
        .when(F.col("niveau_rattachement") == "nom_seul", 1)
        .otherwise(0)
    )
    best = Window.partitionBy("offre_id").orderBy(
        tier.desc(), F.col("dist_m").asc_nulls_last(), F.col("jac").desc()
    )
    winner = (
        cand.filter(tier > 0)
        .withColumn("_r", F.row_number().over(best))
        .filter(F.col("_r") == 1)
        .select(
            "offre_id", "siren", "siret", "niveau_rattachement",
            F.round("dist_m").cast("int").alias("distance_m"),
        )
    )

    final = (
        offres.drop("n_emp")
        .join(winner, "offre_id", "left")
        .withColumn("niveau_rattachement", F.coalesce(F.col("niveau_rattachement"), F.lit("aucun")))
    )

    final = final.persist()
    total = final.count()
    final.write.mode("overwrite").parquet(f"s3a://{bucket}/silver/offre_emploi")

    print(f"\n=== silver/offre_emploi : {total:,} offres ===")
    final.groupBy("metier").count().orderBy(F.desc("count")).show(truncate=False)
    final.groupBy("niveau_rattachement").count().orderBy(F.desc("count")).show(truncate=False)
    print("exemples de rattachements géo :")
    final.filter(F.col("niveau_rattachement").isin("geo_nom", "geo_seul")).select(
        "entreprise_nom", "siren", "niveau_rattachement", "distance_m", "metier", "lieu_libelle"
    ).show(12, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
