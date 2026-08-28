from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    env: Literal["local", "staging", "production"] = "local"

    # API
    api_title: str = "Papperless Leads API"
    api_version: str = "0.1.0"
    front_url: str = "http://localhost:3000"

    # Medallion lake root. Examples:
    #   ./_lake                            -> local dev / HDFS gateway mount
    #   s3://papperlesspreprod-leads-lake  -> Wasabi (S3)
    lake_root: str = "./_lake"
    gold_leads_path: str = "gold/leads_scored"
    gold_kpi_prefix: str = "gold"

    # Ingestion (writes to Bronze). Requires lake_root to be an s3:// URI.
    bronze_prefix: str = "bronze"
    sirene_dataset_api_url: str = (
        "https://www.data.gouv.fr/api/1/datasets/"
        "base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/"
    )
    insee_cj_url: str = (
        "https://www.insee.fr/fr/statistiques/fichier/2028129/cj_septembre_2022.xls"
    )

    # France Travail — API Offres d'emploi v2 (francetravail.io)
    ft_client_id: str | None = None
    ft_client_secret: str | None = None
    ft_token_url: str = (
        "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire"
    )
    ft_api_base: str = "https://api.francetravail.io/partenaire/offresdemploi/v2"
    ft_scope: str = "api_offresdemploiv2 o2dsoffre"

    # Kafka (host poller -> localhost:29092 ; Spark in compose -> kafka:9092)
    kafka_bootstrap: str = "localhost:29092"
    kafka_topic_ft: str = "france_travail.offres"

    # Wasabi / S3 credentials. Only used when lake_root starts with "s3://".
    s3_endpoint_url: str | None = "https://s3.eu-west-2.wasabisys.com"
    s3_region: str | None = "eu-west-2"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def is_s3(self) -> bool:
        return self.lake_root.startswith("s3://")

    @property
    def s3_bucket(self) -> str:
        if not self.is_s3:
            raise ValueError("LAKE_ROOT must be an s3://<bucket> URI for ingestion")
        return self.lake_root[len("s3://") :].split("/", 1)[0]

    @property
    def storage_options(self) -> dict:
        """fsspec storage_options passed to pandas.read_parquet for S3 backends."""
        if not self.is_s3:
            return {}
        client_kwargs: dict = {}
        if self.s3_endpoint_url:
            client_kwargs["endpoint_url"] = self.s3_endpoint_url
        if self.s3_region:
            client_kwargs["region_name"] = self.s3_region
        opts: dict = {
            "client_kwargs": client_kwargs,
            "config_kwargs": {"s3": {"addressing_style": "path"}},  # Wasabi
        }
        if self.aws_access_key_id and self.aws_secret_access_key:
            opts["key"] = self.aws_access_key_id
            opts["secret"] = self.aws_secret_access_key
        return opts


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


settings = get_settings()
