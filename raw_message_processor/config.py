from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from utils.logger import logger

CONFIG_PATH = Path(__file__).parent.parent / "config" / "processor" / "dev.yaml"


def yaml_config_settings_source() -> dict[str, Any]:
    """yaml 파일에서 설정을 로드하는 커스텀 소스 함수입니다."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        logger.error(f"Configuration file not found at {CONFIG_PATH}")
        exit(1)


class LogSettings(BaseModel):
    console_level: str
    file_level: str
    file_path: str


class KafkaConsumerSettings(BaseModel):
    topic: str
    bootstrap_servers: str
    group_id: str


class KafkaProducerSettings(BaseModel):
    master_topic: str
    detail_topic: str
    bootstrap_servers: str
    compression_type: str | None = None
    max_request_size_mb: int = 1


class SchemaRegistrySettings(BaseModel):
    url: str


class AppSettings(BaseSettings):
    """raw_message_processor 어플리케이션의 전체 설정을 관리합니다."""

    debug_mode: bool = False

    log: LogSettings
    consumer: KafkaConsumerSettings
    producer: KafkaProducerSettings
    schema_registry: SchemaRegistrySettings

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """
        설정 소스의 우선 순위를 정의합니다.
        1. 클래스 생성 시 직접 전달된 인수
        2. '.env' 파일에서 불러온 값
        3. 실제 환경 변수
        4. yaml 파일에서 불러온 값
        5. Docker 시크릿 등 파일 기반 시크릿
        """
        return (
            init_settings,
            dotenv_settings,
            env_settings,
            yaml_config_settings_source,  # type: ignore
            file_secret_settings,
        )


# 설정 인스턴스 생성
settings = AppSettings()  # type: ignore
