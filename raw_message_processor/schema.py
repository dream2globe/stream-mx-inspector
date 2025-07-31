import io
import json
from pathlib import Path

from fastavro import parse_schema, writer
from fastavro.schema import SchemaParseException
from fastavro.types import Schema

from utils.logger import logger

MASTER_SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "master_message.json"
FULL_SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "full_message.json"


def get_avro_schema(file_path: Path) -> Schema:
    """avsc 파일을 읽어 avro 스키마로 변환합니다."""
    try:
        with open(file_path, "r") as f:
            schema_json = json.load(f)
            avro_schema = parse_schema(schema_json)
            logger.info(f"Avro schema at {file_path} loaded successfully.")
            return avro_schema
    except FileNotFoundError:
        logger.error(f"Avro schema file not found at {file_path}")
        exit(1)
    except SchemaParseException as e:
        logger.error(f"Error parsing Avro schema: {e}")
        exit(1)


def serialize_to_avro(message: dict, schema: Schema) -> bytes:
    """메시지를 Avro 형식으로 직렬화합니다."""
    bytes_writer = io.BytesIO()
    writer(bytes_writer, schema, [message])
    return bytes_writer.getvalue()


# 글로벌 인스턴스
master_schema = get_avro_schema(MASTER_SCHEMA_PATH)
master_fields = set(field["name"] for field in master_schema["fields"])  # type: ignore
full_schema = get_avro_schema(FULL_SCHEMA_PATH)