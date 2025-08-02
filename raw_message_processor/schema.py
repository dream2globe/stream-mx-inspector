import json
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from .config import settings

MASTER_SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "master_message.json"
FULL_SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "full_message.json"


def get_avro_schema_from_file(file_path: Path) -> str:
    """JSON 파일에서 Avro 스키마를 문자열로 읽어옵니다."""
    with open(file_path, "r") as f:
        return json.dumps(json.load(f))


# Schema Registry 클라이언트 설정
schema_registry_conf = {"url": settings.schema_registry.url}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro 스키마 로드
master_schema_str = get_avro_schema_from_file(MASTER_SCHEMA_PATH)
full_schema_str = get_avro_schema_from_file(FULL_SCHEMA_PATH)

# master_schema.json을 파싱하여 필드 이름 목록을 글로벌 변수로 추출
master_schema_dict = json.loads(master_schema_str)
master_fields = {field["name"] for field in master_schema_dict.get("fields")}


# AvroSerializer 인스턴스 생성
serializer_conf = {"auto.register.schemas": False}
avro_master_serializer = AvroSerializer(
    schema_registry_client,
    master_schema_str,
    serializer_conf,
    to_dict=lambda msg, ctx: msg,
)
avro_full_serializer = AvroSerializer(
    schema_registry_client,
    full_schema_str,
    serializer_conf,
    to_dict=lambda msg, ctx: msg,
)
