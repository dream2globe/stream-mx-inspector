import json

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from util.logger import logger

from .config import settings

# Schema Registry 클라이언트 설정
schema_registry_client = SchemaRegistryClient({"url": settings.schema_registry.url})


def get_schema_from_registry(schema_id: int) -> Schema:
    """스키마 레지스트리에서 스키마를 읽어오는 함수입니다."""
    try:
        schema = schema_registry_client.get_schema(schema_id)
        return schema
    except Exception as e:
        logger.error(f"Schema(id: {schema_id}) not found. - {e}")
        exit(1)


def get_avro_serializer(schema: Schema) -> AvroSerializer:
    """스키마 레지스트리에서 스키마를 읽어오는 함수입니다."""
    try:
        avro_serializer = AvroSerializer(
            schema_registry_client,
            schema.schema_str,
        )
        return avro_serializer
    except Exception as e:
        logger.error(f"Schema serializer not initialized. - {e}")
        exit(1)


# 직렬화/역직렬화에 사용할 스키마 정의
master_schema = get_schema_from_registry(11)
master_default_dict = {
    field["name"]: None for field in json.loads(master_schema.schema_str)["fields"]
}
full_schema = get_schema_from_registry(10)


# Avro 직렬화기 생성
master_serializer = get_avro_serializer(master_schema)
full_serializer = get_avro_serializer(full_schema)
master_deserializer = None
full_deserializer = None

if __debug__:
    from confluent_kafka.schema_registry.avro import AvroDeserializer

    master_deserializer = AvroDeserializer(
        schema_registry_client,
        master_schema.schema_str,
    )
    full_deserializer = AvroDeserializer(
        schema_registry_client,
        full_schema.schema_str,
    )
