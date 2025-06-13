from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

CONFIG_FILE_PATH = Path(__file__).parent / "configs" / "env_gumi_dev"


class ProducerSetting(BaseModel):
    """프로듀서 설정"""

    bootstrap_servers: str
    master_topic: str
    detail_topic: str
    compression_type: str | None = None


class ConsumerSetting(BaseModel):
    """컨슈머 설정"""

    bootstrap_servers: str
    topic: str
    group_id: str


class AppSettings(BaseSettings):
    """어플리케이션 전체 설정"""

    producer: ProducerSetting
    consumer: ConsumerSetting
    model_config = SettingsConfigDict(
        env_file=CONFIG_FILE_PATH,
        env_nested_delimiter="__",
    )


settings = AppSettings()
