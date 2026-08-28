"""Gold: silver/cabinet + silver/offre_emploi -> gold/leads_scored (+ signaux_du_jour).

    docker compose -f docker-compose.spark.yml run --rm spark jobs/gold_leads_scored.py

A ranked call list: one row per diffusible cabinet, with the recent hiring signal
(offers matched by commune/departement only), a 0-100 score, and motifs_score in
plain French for the caller's opening hook.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# SIRENE tranche-effectif codes meaning "has real staff" (>= 3 salariés)
HAS_STAFF = ["02", "03", "11", "12", "21", "22", "31", "32", "41", "42", "51", "52", "53"]

# which offer métier is "on target" for each cabinet segment
SEGMENT_METIERS = {
    "expert_comptable": ["paie", "comptabilite"],
    "avocat_notaire": ["juridique"],
    "cgp": ["patrimoine"],
    "promoteur": ["immobilier"],
    "domiciliation": ["paie", "comptabilite", "juridique", "immobilier", "patrimoine", "autre"],
}


def build_spark() -> SparkSession:
    endpoint = os.environ["S3_ENDPOINT_URL"].split("://")[-1]
    return (
        SparkSession.builder.appName("gold_leads_scored")
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

    cab_src = spark.read.parquet(f"s3a://{bucket}/silver/cabinet")
    data_version = cab_src.agg(F.max("data_version")).first()[0]
    cab_src = cab_src.filter(F.col("data_version") == data_version)

    # count every establishment of the firm (siège + secondaires) for the
    # "réseau / multi-sites" filter and the click-to-link map view
    etab_counts = cab_src.groupBy("siren").agg(
        F.count(F.lit(1)).alias("nb_etablissements")
    )

    # lead rows are firm-level: the siège establishment of each diffusible firm
    cab = cab_src.filter(
        (F.col("est_diffusible") == True)  # noqa: E712
        & (F.col("est_siege") == True)  # noqa: E712
    )

    off = spark.read.parquet(f"s3a://{bucket}/silver/offre_emploi")
    # trusted attributions only (exclude nom_seul / faible / aucun)
    off = off.filter(
        F.col("niveau_rattachement").isin("geo_nom", "nom_commune", "geo_seul", "nom_dept")
        & F.col("siren").isNotNull()
    )

    run_date = off.agg(F.max(F.to_date("date_creation"))).first()[0]  # "as of" the freshest offer
    d90 = F.date_sub(F.lit(run_date), 90)
    d30 = F.date_sub(F.lit(run_date), 30)
    off = off.withColumn("offer_date", F.to_date("date_creation")).filter(F.col("offer_date") >= d90)

    # --- aggregate offers per cabinet SIREN ---
    per_metier = (
        off.groupBy("siren")
        .pivot("metier", ["paie", "comptabilite", "juridique", "patrimoine", "immobilier"])
        .agg(F.count(F.lit(1)))
    )
    for m in ["paie", "comptabilite", "juridique", "patrimoine", "immobilier"]:
        per_metier = per_metier.withColumnRenamed(m, f"a_offre_{m}") if m in per_metier.columns else per_metier

    sig = off.groupBy("siren").agg(
        F.count(F.lit(1)).alias("nb_offres_90j"),
        F.sum(F.when(F.col("offer_date") >= d30, 1).otherwise(0)).alias("nb_offres_30j"),
        F.sum(F.when(F.col("offer_date") >= d30, F.coalesce("nombre_postes", F.lit(1))).otherwise(0)).alias("nb_postes_30j"),
        F.sum(F.when(F.col("type_contrat") == "CDI", 1).otherwise(0)).alias("nb_cdi_90j"),
        F.max("offer_date").alias("derniere_offre_date"),
        F.collect_set("metier").alias("metiers_recents"),
        F.max(F.struct(F.col("offer_date"), F.col("intitule"), F.col("url_origine"))).alias("_last"),
    ).select(
        "siren", "nb_offres_90j", "nb_offres_30j", "nb_postes_30j", "nb_cdi_90j",
        "derniere_offre_date", "metiers_recents",
        F.col("_last.intitule").alias("derniere_offre_intitule"),
        F.col("_last.url_origine").alias("derniere_offre_url"),
    )
    sig = sig.join(per_metier, "siren", "left")

    # --- join onto cabinets ---
    df = cab.join(F.broadcast(sig), "siren", "left")
    df = df.join(F.broadcast(etab_counts), "siren", "left")
    df = df.withColumn("nb_etablissements", F.coalesce(F.col("nb_etablissements"), F.lit(1)))
    zero_cols = ["nb_offres_90j", "nb_offres_30j", "nb_postes_30j", "nb_cdi_90j"]
    for c in zero_cols:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    for m in ["paie", "comptabilite", "juridique", "patrimoine", "immobilier"]:
        col = f"a_offre_{m}"
        df = df.withColumn(col, F.coalesce(F.col(col), F.lit(0)) > 0) if col in df.columns else df.withColumn(col, F.lit(False))
    df = df.withColumn(
        "recence_jours",
        F.when(F.col("derniere_offre_date").isNotNull(), F.datediff(F.lit(run_date), F.col("derniere_offre_date"))),
    )
    df = df.withColumn("metiers_recents", F.coalesce(F.col("metiers_recents"), F.array()))

    # --- score ---
    struct_forme = ~F.col("forme_juridique").rlike("^(Entrepreneur individuel|Cat\\. )")
    has_staff = F.col("tranche_effectif").isin(HAS_STAFF)
    _seg_metiers_map = F.create_map(
        *sum(([F.lit(k), F.array(*[F.lit(v) for v in vs])] for k, vs in SEGMENT_METIERS.items()), [])
    )
    metier_pertinent = F.arrays_overlap(F.col("metiers_recents"), _seg_metiers_map[F.col("segment")])

    is_domicil = F.col("segment") == "domiciliation"
    # franchises/networks in domiciliation hire continuously -> cap their offer leverage
    nb30_eff = F.when(is_domicil, F.least(F.col("nb_offres_30j"), F.lit(2))).otherwise(F.col("nb_offres_30j"))

    fit = (  # 0..25 -- cold-prospect ceiling
        F.lit(5)
        + F.when(struct_forme, 8).otherwise(0)
        + F.when(has_staff, 6).otherwise(0)
        + F.when(F.col("anciennete_annees").between(2, 15), 6)
        .when(F.col("anciennete_annees").isNotNull(), 2)
        .otherwise(0)
    )
    contact = (  # 0..10
        F.when(F.col("adresse").isNotNull() & F.col("code_postal").isNotNull(), 6).otherwise(0)
        + F.when(F.col("latitude").isNotNull(), 4).otherwise(0)
    )
    sig_score = F.when(  # 0..65 ; exactly 0 for cold prospects
        F.col("nb_offres_90j") > 0,
        F.least(
            F.lit(65),
            F.lit(12)
            + F.when(F.col("nb_offres_30j") > 0, 10).otherwise(0)
            + F.when(F.col("recence_jours") <= 3, 18)
            .when(F.col("recence_jours") <= 7, 14)
            .when(F.col("recence_jours") <= 14, 9)
            .when(F.col("recence_jours") <= 21, 5)
            .when(F.col("recence_jours") <= 30, 2)
            .otherwise(0)
            + F.when(F.coalesce(metier_pertinent, F.lit(False)), 10).otherwise(0)
            + F.when(F.col("nb_cdi_90j") >= 1, 6).otherwise(0)
            + F.when(nb30_eff >= 3, 6).otherwise(0),
        ),
    ).otherwise(0)

    df = df.withColumn("score", F.least(F.lit(100), (fit + contact + sig_score)).cast("int"))
    df = df.withColumn(
        "bande_score",
        F.when(F.col("score") >= 70, "chaud").when(F.col("score") >= 45, "tiede").otherwise("froid"),
    )

    recence_txt = F.concat(F.lit("il y a "), F.col("recence_jours").cast("string"), F.lit(" j"))
    reasons = F.array_compact(
        F.array(
            F.when(F.col("a_offre_paie"), F.concat(F.lit("recrute en paie ("), recence_txt, F.lit(")"))),
            F.when(F.col("a_offre_comptabilite"), F.lit("recrute un collaborateur comptable")),
            F.when(F.col("a_offre_juridique"), F.lit("recrute un profil juridique")),
            F.when(F.col("a_offre_patrimoine"), F.lit("recrute un conseiller patrimonial")),
            F.when(F.col("a_offre_immobilier"), F.lit("recrute sur un poste immobilier")),
            F.when(F.col("nb_offres_30j") >= 2, F.concat(F.col("nb_offres_30j").cast("string"), F.lit(" offres ce mois"))),
            F.when(struct_forme, F.col("forme_juridique")),
            F.when((F.col("anciennete_annees") >= 1) & (F.col("anciennete_annees") <= 40),
                   F.concat(F.lit("créé il y a "), F.col("anciennete_annees").cast("string"), F.lit(" ans"))),
        )
    )
    df = df.withColumn("motifs_score", reasons)

    leads = (
        df.select(
            F.col("siren"), F.col("siret"),
            F.col("raison_sociale"),
            "segment",
            F.col("code_ape"),
            "categorie_juridique", "forme_juridique",
            "date_creation", "anciennete_annees", "tranche_effectif", "est_ess",
            "adresse", "complement_adresse", "code_postal", "commune", "code_commune", "departement",
            "latitude", "longitude", "ban_id", "est_diffusible", "nb_etablissements",
            "nb_offres_30j", "nb_offres_90j", "nb_postes_30j", "nb_cdi_90j",
            "derniere_offre_date", "recence_jours", "metiers_recents",
            "a_offre_paie", "a_offre_comptabilite", "a_offre_juridique",
            "a_offre_patrimoine", "a_offre_immobilier",
            "derniere_offre_intitule", "derniere_offre_url",
            "score", "bande_score", "motifs_score",
        )
        .withColumn("run_date", F.lit(str(run_date)))
        .withColumn("data_version", F.lit(data_version))
    )

    leads = leads.persist()
    total = leads.count()
    with_signal = leads.filter(F.col("nb_offres_90j") > 0).count()

    (
        leads.repartition(4)
        .write.mode("overwrite")
        .partitionBy("data_version")
        .parquet(f"s3a://{bucket}/gold/leads_scored")
    )

    # signaux du jour = fresh + actionable
    (
        leads.filter(F.col("recence_jours") <= 2)
        .orderBy(F.desc("score"), F.asc("recence_jours"), F.desc("nb_offres_30j"))
        .select(
            "siren", "raison_sociale", "segment", "commune", "departement", "adresse",
            "latitude", "longitude", "metiers_recents", "derniere_offre_intitule",
            "derniere_offre_url", "score", "motifs_score",
        )
        .withColumn("run_date", F.lit(str(run_date)))
        .withColumn("data_version", F.lit(data_version))
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("data_version")
        .parquet(f"s3a://{bucket}/gold/signaux_du_jour")
    )

    print(f"\n=== gold/leads_scored : {total:,} cabinets  ({with_signal:,} avec signal d'embauche) ===")
    leads.groupBy("bande_score").count().orderBy(F.desc("count")).show(truncate=False)
    leads.filter(F.col("nb_offres_90j") > 0).groupBy("segment").agg(
        F.count(F.lit(1)).alias("n_signal"), F.round(F.avg("score"), 1).alias("score_moy")
    ).orderBy(F.desc("n_signal")).show(truncate=False)
    print("Top 20 leads avec signal :")
    leads.filter(F.col("nb_offres_90j") > 0).orderBy(
        F.desc("score"), F.asc("recence_jours"), F.desc("nb_offres_30j")
    ).select("raison_sociale", "segment", "departement", "score", "bande_score", "recence_jours", "motifs_score").show(20, truncate=60)

    spark.stop()


if __name__ == "__main__":
    main()
