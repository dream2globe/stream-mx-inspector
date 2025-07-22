from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "loader" / "gumi_dev.yaml"


def yaml_config_settings_source(settings: BaseSettings) -> dict:
    """gumi_dev.yaml 파일에서 raw_message_processor 섹션을 로드합니다."""
    try:
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f)
            # 'raw_message_processor' 키 아래의 설정을 반환합니다.
            return config.get("raw_message_processor", {})
    except FileNotFoundError:
        print(f"Configuration file not found at {CONFIG_PATH}")
        return {}


class LogSettings(BaseModel):
    level: str = "INFO"
    path: Path = "logs/raw_message_processor.log"


class KafkaConsumerSettings(BaseModel):
    topic: str
    bootstrap_servers: list[str] | str
    group_id: str


class KafkaProducerSettings(BaseModel):
    master_topic: str
    detail_topic: str
    bootstrap_servers: list[str] | str
    compression_type: str | None = None


class AppSettings(BaseSettings):
    """raw_message_processor 어플리케이션의 전체 설정을 관리합니다."""

    log: LogSettings = Field(default_factory=LogSettings)
    consumer: KafkaConsumerSettings
    producer: KafkaProducerSettings

    model_config = SettingsConfigDict(
        # .env 파일 로드 (필요 시)
        env_file=".env",
        env_nested_delimiter="__",
        # 사용자 정의 소스 추가
        custom_config_sources={"yaml_config": yaml_config_settings_source},
    )


# 설정 인스턴스 생성
settings = AppSettings()
