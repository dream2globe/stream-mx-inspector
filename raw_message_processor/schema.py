import io
from pathlib import Path

from avro import Schema
from avro.io import BinaryEncoder, DatumWriter
from avro.schema import SchemaParseException, parse
from loguru import logger

PARTIAL_SCHEMA_PATH = (
    Path(__file__).parent.parent / "schema" / "partial_inspector_message.avsc"
)
FULL_SCHEMA_PATH = (
    Path(__file__).parent.parent / "schema" / "full_inspector_message.avsc"
)


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


def get_mandatory_keys(schema: Schema) -> set[str]:
    """avro 스키마에서 필수 키를 찾아 리턴합니다."""
    return {field.name for field in schema.fields if field.default is None}


partial_schema = get_avro_schema(PARTIAL_SCHEMA_PATH)
full_schema = get_avro_schema(FULL_SCHEMA_PATH)
