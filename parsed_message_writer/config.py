# src/streamer/config.py
import os
from functools import lru_cache

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseModel):
    bootstrap_servers: str
    topic: str
    group_id: str
    schema_registry_url: str | None = None
    schema_file_path: str | None = None


class IcebergSettings(BaseModel):
    catalog_name: str
    catalog_uri: str
    catalog_warehouse: str
    target_table: str


class ApiSettings(BaseModel):
    enrichment_api_url: str


class SparkSettings(BaseModel):
    app_name: str
    master: str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    env: str = Field("dev", description="실행 환경 (dev or prod)")
    log_level: str = "INFO"

    # Nested settings
    kafka: KafkaSettings
    iceberg: IcebergSettings
    api: ApiSettings
    spark: SparkSettings

    # 민감 정보 (DB 접속정보 등)
    db_user: str = Field(..., env="DB_USER")
    db_password: str = Field(..., env="DB_PASSWORD")


@lru_cache()
def load_settings() -> Settings:
    """
    환경에 맞는 설정 파일을 읽어 Pydantic 모델을 반환합니다.
    환경 변수 `ENV`를 통해 'dev' 또는 'prod'를 구분합니다.
    """
    env = os.getenv("ENV", "dev")
    config_path = f"config/config.{env}.yml"

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    return Settings(**config_data)
