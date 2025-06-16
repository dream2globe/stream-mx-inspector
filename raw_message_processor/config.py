import tomllib
from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import BaseSettings

CONFIG_FILE_PATH = Path(__file__).parent / "configs" / "gumi_dev.toml"


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

    @classmethod
    def from_toml(cls, toml_path: Path) -> "AppSettings":
        with open(toml_path, "rb") as f:
            config_data = tomllib.load(f)
        return cls(**config_data)


settings = AppSettings.from_toml(CONFIG_FILE_PATH)
