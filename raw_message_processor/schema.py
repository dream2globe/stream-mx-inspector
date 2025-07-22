import io
from pathlib import Path

from avro import Schema
from avro.io import BinaryEncoder, DatumWriter
from avro.schema import RecordSchema, SchemaParseException, UnionSchema, parse
from loguru import logger

SUMMARY_SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "summary_message.avsc"
FULL_SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "full_message.avsc"


def get_avro_schema(file_path: Path) -> Schema:
    """avsc 파일을 읽어 avro 스키마로 변환합니다."""
    try:
        with open(file_path, "r") as f:
            avro_schema = parse(f.read())
            logger.info("Avro schema loaded successfully.")
            return avro_schema
    except FileNotFoundError:
        logger.error(f"Avro schema file not found at {file_path}")
        exit(1)
    except SchemaParseException as e:
        logger.error(f"Error parsing Avro schema: {e}")
        exit(1)


def serialize_to_avro(message, schema: Schema) -> bytes:
    """메시지를 Avro 형식으로 직렬화합니다."""
    try:
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write(message, encoder)
        return bytes_writer.getvalue()
    except Exception as e:
        logger.error(
            f"Avro serialzation error: {type(e).__name__} - {e}", exc_info=True
        )


def get_mandatory_keys(schema: RecordSchema) -> set[str]:
    """
    Avro 레코드 스키마에서 필수 필드의 이름 집합을 반환합니다.
    필수 필드는 타입에 'null'이 포함되지 않은 필드로 간주합니다.
    """
    mandatory_keys = set()
    for field in schema.fields:
        if isinstance(field.type, UnionSchema):
            # 타입이 Union일 경우, 'null'을 포함하지 않으면 필수 필드입니다.
            if not any(s.type == "null" for s in field.type.schemas):
                mandatory_keys.add(field.name)
        else:
            # Union 타입이 아니면 항상 필수 필드입니다.
            mandatory_keys.add(field.name)
    return mandatory_keys


summary_schema = get_avro_schema(SUMMARY_SCHEMA_PATH)
full_schema = get_avro_schema(FULL_SCHEMA_PATH)
