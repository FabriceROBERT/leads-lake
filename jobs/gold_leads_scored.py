"""Gold: silver/cabinet + silver/offre_emploi + silver/enrichissement
       -> gold/leads_scored (+ signaux_du_jour).

    docker compose -f docker-compose.spark.yml run --rm spark jobs/gold_leads_scored.py

A ranked call list: one row per diffusible cabinet, with a 0-100 score built from
**segment-weighted components** (see SCORING_V2.md), the plain-French
`motifs_score` for the caller's opening hook, and `flags` for the caller's eye
(en redressement, nouveau dirigeant, a racheté un cabinet...).

Score = 100 * Σ(poids[c] * composante[c]) / Σ(poids[c])   over the components
available this run. `activite_metier` is not wired yet (Phase C) -> it is left
out of both sums so the score is not deflated; adding it later needs no rescale.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession, Window
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

# --- scoring V2 -------------------------------------------------------------
# Points max per component, per segment (≈100). The score normalises by the
# weights of the components actually computed, so absolute totals need not be
# exactly 100. Keys must match the segments produced by silver_cabinet today
# (avocat_notaire not split yet -> its row is the avocat/notaire average).
SEGMENT_WEIGHTS = {
    "expert_comptable": {"fit": 10, "contact": 10, "taille": 25, "rh": 20, "now": 15, "metier": 5, "reforme": 15},
    "avocat_notaire":   {"fit": 9,  "contact": 10, "taille": 18, "rh": 14, "now": 15, "metier": 18, "reforme": 8},
    "cgp":              {"fit": 10, "contact": 10, "taille": 12, "rh": 18, "now": 25, "metier": 8,  "reforme": 15},
    "promoteur":        {"fit": 8,  "contact": 10, "taille": 25, "rh": 12, "now": 20, "metier": 15, "reforme": 0},
    "domiciliation":    {"fit": 8,  "contact": 10, "taille": 15, "rh": 12, "now": 20, "metier": 25, "reforme": 0},
    "_default":         {"fit": 10, "contact": 10, "taille": 20, "rh": 20, "now": 15, "metier": 10, "reforme": 10},
}

# "pourquoi maintenant" réglementaire, 0..1 par segment
REFORME_VALUE = {
    "expert_comptable": 1.0,   # facturation électronique 2026-2027
    "cgp": 1.0,                # DDA / LCB-FT / DORA
    "avocat_notaire": 0.5,     # acte authentique / acte d'avocat électronique
    "promoteur": 0.0,
    "domiciliation": 0.0,
    "_default": 0.3,
}

# components computed this run (metier = Phase C, left out on purpose)
AVAILABLE_COMPONENTS = ["fit", "contact", "taille", "rh", "now", "reforme"]

# SIRENE tranche-effectif code -> 0..1 size proxy
EFF_SCALE = {
    "00": 0.0, "01": 0.10, "02": 0.20, "03": 0.30, "11": 0.45, "12": 0.60,
    "21": 0.75, "22": 0.85, "31": 0.90, "32": 0.95,
    "41": 1.0, "42": 1.0, "51": 1.0, "52": 1.0, "53": 1.0,
}

# BODACC modif wording -> "governance change"
_DIRIGEANT_RE = r"(?i)(dirigeant|g[ée]rant|pr[ée]sident|repr[ée]sentant)"


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


def _read_enrichissement(spark: SparkSession, bucket: str):
    """Latest silver/enrichissement partition, or None if not produced yet."""
    path = f"s3a://{bucket}/silver/enrichissement"
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(p):
        print("silver/enrichissement not found — scoring without financial / event signals")
        return None
    vs = sorted(
        st.getPath().getName().split("data_version=")[-1]
        for st in fs.listStatus(p)
        if "data_version=" in st.getPath().getName()
    )
    if not vs:
        return None
    print(f"silver/enrichissement data_version = {vs[-1]}")
    return spark.read.parquet(f"{path}/data_version={vs[-1]}").drop("data_version")


def _read_silver_contacts(spark: SparkSession, bucket: str):
    """silver/contacts (phone / e-mail per siren), or None if not produced yet."""
    path = f"s3a://{bucket}/silver/contacts"
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    if not p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p):
        print("silver/contacts not found — scoring without crawl contacts")
        return None
    return spark.read.parquet(path).select(
        "siren", "telephone", "email", "site_web", "contact_verifie"
    )


def main() -> None:
    bucket = os.environ["LAKE_ROOT"].split("://", 1)[-1].split("/", 1)[0]
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    cab_src = spark.read.parquet(f"s3a://{bucket}/silver/cabinet")
    data_version = cab_src.agg(F.max("data_version")).first()[0]
    cab_src = cab_src.filter(F.col("data_version") == data_version)

    # count every establishment of the firm (siège + secondaires)
    etab_counts = cab_src.groupBy("siren").agg(
        F.count(F.lit(1)).alias("nb_etablissements")
    )

    # lead rows are firm-level: the siège establishment of each diffusible firm
    cab = cab_src.filter(
        (F.col("est_diffusible") == True)  # noqa: E712
        & (F.col("est_siege") == True)  # noqa: E712
    )

    off = spark.read.parquet(f"s3a://{bucket}/silver/offre_emploi")
    off = off.filter(
        F.col("niveau_rattachement").isin("geo_nom", "nom_commune", "geo_seul", "nom_dept")
        & F.col("siren").isNotNull()
    )

    run_date = off.agg(F.max(F.to_date("date_creation"))).first()[0]
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

    # --- join everything onto cabinets ---
    df = cab.join(F.broadcast(sig), "siren", "left")
    df = df.join(F.broadcast(etab_counts), "siren", "left")
    df = df.withColumn("nb_etablissements", F.coalesce(F.col("nb_etablissements"), F.lit(1)))

    enr = _read_enrichissement(spark, bucket)
    if enr is not None:
        df = df.join(enr, "siren", "left")

    con = _read_silver_contacts(spark, bucket)
    if con is not None:
        df = df.join(F.broadcast(con), "siren", "left")

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

    # helpers tolerant to a missing silver/enrichissement
    cols = set(df.columns)

    def opt(name: str, default=None):
        return F.col(name) if name in cols else F.lit(default)

    def flag(name: str):
        return F.coalesce(F.col(name), F.lit(False)) if name in cols else F.lit(False)

    def clip01(expr):
        return F.least(F.greatest(F.coalesce(expr, F.lit(0.0)), F.lit(0.0)), F.lit(1.0))

    def decay(date_col, half_life_days: int):
        age = F.datediff(F.current_date(), date_col)
        return F.when(
            date_col.isNotNull() & (age >= 0),
            F.pow(F.lit(0.5), age / F.lit(float(half_life_days))),
        ).otherwise(F.lit(0.0))

    # === composantes (0..1) =========================================
    struct_forme = ~F.col("forme_juridique").rlike("^(Entrepreneur individuel|Cat\\. )")
    has_staff = F.col("tranche_effectif").isin(HAS_STAFF)
    _seg_metiers_map = F.create_map(
        *sum(([F.lit(k), F.array(*[F.lit(v) for v in vs])] for k, vs in SEGMENT_METIERS.items()), [])
    )
    metier_pertinent = F.arrays_overlap(F.col("metiers_recents"), _seg_metiers_map[F.col("segment")])

    fit_raw = (  # 0..25
        F.lit(5)
        + F.when(struct_forme, 8).otherwise(0)
        + F.when(has_staff, 6).otherwise(0)
        + F.when(F.col("anciennete_annees").between(2, 15), 6)
        .when(F.col("anciennete_annees").isNotNull(), 2)
        .otherwise(0)
    )
    contact_raw = (  # 0..10
        F.when(F.col("adresse").isNotNull() & F.col("code_postal").isNotNull(), 5).otherwise(0)
        + F.when(F.col("latitude").isNotNull(), 3).otherwise(0)
        + F.when(flag("contact_verifie"), 2).otherwise(0)  # crawl: phone verified on the site
    )
    is_domicil = F.col("segment") == "domiciliation"
    nb30_eff = F.when(is_domicil, F.least(F.col("nb_offres_30j"), F.lit(2))).otherwise(F.col("nb_offres_30j"))
    rh_raw = F.when(  # 0..65 ; exactly 0 without any offer
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

    # taille : proxy du volume d'actes -- CA (percentile intra-segment) + effectif + réseau
    ca_src = F.coalesce(opt("ca").cast("double"), F.lit(0.0))
    ca_pct = F.percent_rank().over(Window.partitionBy("segment").orderBy(ca_src))
    eff_map = F.create_map(*sum(([F.lit(k), F.lit(v)] for k, v in EFF_SCALE.items()), []))
    eff_score = F.coalesce(eff_map[F.col("tranche_effectif")], F.lit(0.15))
    etab_score = F.least(
        F.lit(1.0),
        F.log1p(F.greatest(F.col("nb_etablissements") - F.lit(1), F.lit(0))) / F.log1p(F.lit(9.0)),
    )
    c_taille = clip01(F.lit(0.55) * ca_pct + F.lit(0.30) * eff_score + F.lit(0.15) * etab_score)

    # signal_now : max des fenêtres d'événements datés
    c_now = clip01(
        F.greatest(
            decay(F.col("derniere_offre_date"), 40),
            decay(opt("bodacc_derniere_modif_date").cast("date"), 180),
            decay(opt("bodacc_vente_cession_date").cast("date"), 240),
            F.when(flag("flag_ca_hausse"), F.lit(0.5)).otherwise(F.lit(0.0)),
        )
    )

    reforme_map = F.create_map(*sum(([F.lit(k), F.lit(float(v))] for k, v in REFORME_VALUE.items() if k != "_default"), []))
    c_reforme = clip01(F.coalesce(reforme_map[F.col("segment")], F.lit(float(REFORME_VALUE["_default"]))))

    comp = {
        "fit": clip01(fit_raw / F.lit(25.0)),
        "contact": clip01(contact_raw / F.lit(10.0)),
        "taille": c_taille,
        "rh": clip01(rh_raw / F.lit(65.0)),
        "now": c_now,
        "reforme": c_reforme,
    }

    # === somme pondérée par segment, normalisée par les poids disponibles ===
    def wmap(component: str):
        pairs = sum(
            ([F.lit(seg), F.lit(float(w[component]))] for seg, w in SEGMENT_WEIGHTS.items() if seg != "_default"),
            [],
        )
        return F.create_map(*pairs)

    num = F.lit(0.0)
    den = F.lit(0.0)
    for c in AVAILABLE_COMPONENTS:
        w = F.coalesce(wmap(c)[F.col("segment")], F.lit(float(SEGMENT_WEIGHTS["_default"][c])))
        df = df.withColumn(f"_w_{c}", w).withColumn(f"_c_{c}", comp[c])
        num = num + F.col(f"_w_{c}") * F.col(f"_c_{c}")
        den = den + F.col(f"_w_{c}")

    df = df.withColumn("_num", num).withColumn("_den", F.greatest(den, F.lit(1.0)))
    df = df.withColumn("score", F.least(F.lit(100), F.greatest(F.lit(0), F.round(F.lit(100.0) * F.col("_num") / F.col("_den")))).cast("int"))
    df = df.withColumn(
        "score_detail",
        F.create_map(
            *sum(
                (
                    [F.lit(c), F.round(F.lit(100.0) * F.col(f"_w_{c}") * F.col(f"_c_{c}") / F.col("_den"), 1)]
                    for c in AVAILABLE_COMPONENTS
                ),
                [],
            )
        ),
    )
    df = df.withColumn(
        "bande_score",
        F.when(F.col("score") >= 70, "chaud").when(F.col("score") >= 45, "tiede").otherwise("froid"),
    )

    # exclusion : entité radiée (SIRENE état déjà filtré en Silver)
    df = df.filter(~flag("flag_radiee"))

    # === flags (œil du commercial) ===
    df = df.withColumn(
        "flags",
        F.array_compact(
            F.array(
                F.when(flag("flag_redressement"), F.lit("en redressement")),
                F.when(flag("flag_nouveau_dirigeant"), F.lit("nouveau dirigeant")),
                F.when(flag("flag_acquisition"), F.lit("a racheté un cabinet")),
                F.when(flag("flag_ca_hausse"), F.lit("CA en hausse")),
                F.when(flag("flag_ca_baisse"), F.lit("CA en baisse")),
                # flag_comptes_non_deposes dropped: the BODACC backfill only kept
                # the 30 latest annonces per siren, so "no dpc seen" is a false
                # negative for any active firm.
            )
        ),
    )

    # === motifs_score (accroche d'appel) ===
    recence_txt = F.concat(F.lit("il y a "), F.col("recence_jours").cast("string"), F.lit(" j"))
    croissance_txt = F.concat(
        F.lit("CA en hausse de "),
        F.round(opt("ca_croissance_pct") * F.lit(100.0), 0).cast("int").cast("string"),
        F.lit(" %"),
    )
    reasons = F.array_compact(
        F.array(
            F.when(F.col("a_offre_paie"), F.concat(F.lit("recrute en paie ("), recence_txt, F.lit(")"))),
            F.when(F.col("a_offre_comptabilite"), F.lit("recrute un collaborateur comptable")),
            F.when(F.col("a_offre_juridique"), F.lit("recrute un profil juridique")),
            F.when(F.col("a_offre_patrimoine"), F.lit("recrute un conseiller patrimonial")),
            F.when(F.col("a_offre_immobilier"), F.lit("recrute sur un poste immobilier")),
            F.when(F.col("nb_offres_30j") >= 2, F.concat(F.col("nb_offres_30j").cast("string"), F.lit(" offres ce mois"))),
            F.when(flag("flag_nouveau_dirigeant"), F.lit("nouveau dirigeant récemment")),
            F.when(flag("flag_acquisition"), F.lit("a racheté un cabinet récemment")),
            F.when(flag("flag_ca_hausse") & opt("ca_croissance_pct").isNotNull(), croissance_txt),
            F.when(struct_forme, F.col("forme_juridique")),
            F.when((F.col("anciennete_annees") >= 1) & (F.col("anciennete_annees") <= 40),
                   F.concat(F.lit("créé il y a "), F.col("anciennete_annees").cast("string"), F.lit(" ans"))),
        )
    )
    df = df.withColumn("motifs_score", reasons)

    base_cols = [
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
        "score", "bande_score", "score_detail", "flags", "motifs_score",
        # explicit booleans so the map endpoints can filter on them cheaply
        flag("flag_ca_hausse").alias("flag_ca_hausse"),
        flag("flag_ca_baisse").alias("flag_ca_baisse"),
        flag("flag_acquisition").alias("flag_acquisition"),
        flag("flag_redressement").alias("flag_redressement"),
        flag("flag_nouveau_dirigeant").alias("flag_nouveau_dirigeant"),
    ]
    enrich_passthrough = [
        "ca", "ca_n1", "ca_croissance_pct", "resultat_net", "resultat_n1", "annee_comptes",
        "categorie_entreprise", "tranche_effectif_rne", "dirigeant_principal", "nb_dirigeants",
        "convention_collective", "est_qualiopi", "est_ess_rne",
        "bodacc_en_procedure", "bodacc_procedure_detail", "bodacc_procedure_date",
        "bodacc_derniere_modif_date", "bodacc_vente_cession_date", "bodacc_radiation_date",
        "bodacc_nb_annonces_24m",
        "telephone", "email", "site_web", "contact_verifie",
        # bodacc_evenements / dirigeants stay JSON strings in Silver -> the fiche
        # gets them as real lists from the live _enrich(), not from Gold.
    ]
    leads = (
        df.select(*base_cols, *[c for c in enrich_passthrough if c in df.columns])
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
            "derniere_offre_url", "score", "flags", "motifs_score",
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
    leads.groupBy("segment").agg(
        F.count(F.lit(1)).alias("n"), F.round(F.avg("score"), 1).alias("score_moy")
    ).orderBy(F.desc("n")).show(truncate=False)
    print("Top 20 leads :")
    leads.orderBy(F.desc("score")).select(
        "raison_sociale", "segment", "departement", "score", "bande_score", "flags", "motifs_score"
    ).show(20, truncate=60)

    spark.stop()


if __name__ == "__main__":
    main()
