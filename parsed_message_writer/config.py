import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# --- 환경 변수 로드 모델 (.env 파일) ---
class EnvSettings(BaseSettings):
    """
    .env 파일에서 민감 정보를 로드합니다.
    """

    # Pydantic v2 스타일: model_config 사용
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # .env 파일에 추가적인 변수가 있어도 무시
    )

    # PostgreSQL DB 접속 정보
    postgres_user: str = Field(..., description="PostgreSQL 사용자 이름")
    postgres_password: str = Field(..., description="PostgreSQL 비밀번호")

    # S3 호환 오브젝트 스토리지 접속 정보
    s3_access_key: str = Field(..., description="S3 Access Key")
    s3_secret_key: str = Field(..., description="S3 Secret Key")

    # 외부 API 키
    enrichment_api_key: str = Field(..., description="데이터 보강 API 키")


# --- YAML 파일 구조에 매핑되는 모델 ---
class SparkSettings(BaseModel):
    app_name: str
    packages: List[str]
    configs: Dict[str, Any]


class KafkaSettings(BaseModel):
    bootstrap_servers: str
    topic: str
    group_id: str
    starting_offsets: str = "latest"
    schema_registry_url: str | None = None  # Prod 환경에서만 사용


class IcebergSettings(BaseModel):
    catalog_name: str
    warehouse_path: str
    db_name: str
    table_name: str
    jdbc_url: str


class ApiSettings(BaseModel):
    base_url: str


class AppSettings(BaseModel):
    """YAML 설정 파일의 전체 구조"""

    env: Literal["development", "production"]
    spark: SparkSettings
    kafka: KafkaSettings
    iceberg: IcebergSettings
    api: ApiSettings
    log_file_path: str


# --- 최종 설정 통합 클래스 ---
class Settings(BaseModel):
    app: AppSettings
    env_vars: EnvSettings


def load_yaml_config(path: Path) -> Dict[str, Any]:
    """YAML 파일을 로드하여 딕셔너리로 반환합니다."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@lru_cache
def get_settings() -> Settings:
    """
    애플리케이션 설정을 로드하고 캐싱합니다.
    YAML 파일과 .env 파일을 결합하여 최종 설정을 생성합니다.
    """
    # 프로젝트 루트 경로를 기준으로 설정 파일 경로 설정
    config_path = Path(__file__).parent.parent.parent / "config" / "config.yml"

    # YAML 파일 로드
    yaml_config = load_yaml_config(config_path)

    # 현재 실행 환경 결정 (환경 변수 우선)
    current_env = os.getenv("APP_ENV", "development")

    # 환경별 설정 선택
    env_specific_config = yaml_config[current_env]

    return Settings(
        app=AppSettings(env=current_env, **env_specific_config), env_vars=EnvSettings()
    )


# 전역적으로 사용할 설정 객체
settings = get_settings()
