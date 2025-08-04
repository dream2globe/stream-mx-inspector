import asyncio

import uvloop
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from confluent_kafka.serialization import MessageField, SerializationContext

from util.logger import logger, setup_logger

from .config import settings
from .exceptions import ParsingError, TestCodeExtractionError, UnsupportedTestCodeError
from .parser import get_parser_for, get_test_code
from .schema import (
    full_deserializer,
    full_serializer,
    master_deserializer,
    master_serializer,
)


async def serialize_and_send(
    producer: AIOKafkaProducer,
    serializer,
    deserializer,
    topic: str,
    data: dict,
    test_code: str,
):
    """주어진 데이터를 직렬화하고 Kafka 토픽으로 전송합니다. 디버그 모드에서는 역직렬화를 통해 데이터를 검증합니다."""
    try:
        # 직렬화 컨텍스트 생성
        context = SerializationContext(topic, MessageField.VALUE)
        payload = await asyncio.to_thread(serializer, data, context)

        # 역직렬화를 메시지 전송이 정상적인지 테스트 (python -O 옵션을 통해 실행 여부 결정)
        if __debug__:
            deserialized_data = await asyncio.to_thread(deserializer, payload, context)
            if data != deserialized_data:
                logger.warning(
                    f"[{test_code}] Data mismatch after serialization/deserialization for topic {topic}.",
                    extra={"original": data, "deserialized": deserialized_data},
                )
            else:
                logger.debug(
                    f"[{test_code}] Data for topic {topic} successfully validated."
                )

        # Kafka로 메시지 전송
        await producer.send(topic, payload)

    except Exception as e:
        # 직렬화 또는 전송 실패 시 에러 로깅
        logger.error(
            f"[{test_code}] Failed to serialize/send message to topic {topic}: {e}",
            extra={"data": data},
        )


async def process_message(message: bytes, producer: AIOKafkaProducer):
    """카프카의 원 메시지를 처리하여 새로운 토픽으로 전달합니다."""
    decoded_message = message.decode("utf-8", errors="ignore")
    test_code = "UNKNOWN"  # 초기값 설정

    try:
        # 1. 테스트 코드(공정)에 맞는 파서를 선택
        test_code = get_test_code(message)
        parser = get_parser_for(test_code)

        # 2. 데이터를 딕셔너리 타입으로 파싱
        parsed_message = parser.parse(message)
        master_data = parsed_message.get("MASTER")

        if not master_data:
            logger.warning(
                f"[{test_code}] 'MASTER' data not found in parsed message. Skipping.",
                extra={"raw_message": decoded_message},
            )
            return

        # 3. 마스터 데이터와 전체 데이터를 각각 직렬화 후 병렬로 전송
        master_task = serialize_and_send(
            producer,
            master_serializer,
            master_deserializer,  # 역직렬화기 전달
            settings.producer.master_topic,
            master_data,
            test_code,
        )
        full_task = serialize_and_send(
            producer,
            full_serializer,
            full_deserializer,  # 역직렬화기 전달
            settings.producer.detail_topic,
            parsed_message,
            test_code,
        )
        await asyncio.gather(master_task, full_task)

    except TestCodeExtractionError as e:
        logger.error(f"{e}. Skipping message.", extra={"raw_message": decoded_message})
    except UnsupportedTestCodeError as e:
        logger.error(f"{e}. Skipping message.", extra={"raw_message": decoded_message})
    except ParsingError as e:
        logger.error(
            f"[{test_code}] Message Parsing error: {e}",
            extra={"raw_message": decoded_message},
        )
    except Exception as e:
        logger.exception(
            f"[{test_code}] An unexpected error occurred during message processing: {e}.",
            extra={"raw_message": decoded_message},
        )


async def main():
    """어플리케이션 초기화 및 실행을 담당합니다."""
    # 로거 세팅
    setup_logger(
        console_level=settings.log.console_level,
        file_level=settings.log.file_level,
        log_file=settings.log.file_path,
    )

    logger.info("Application starting with the following settings:")
    # __debug__ 값을 통해 현재 디버그 모드 활성화 여부를 로깅
    logger.info(f"  - Debug Mode: {__debug__}")
    logger.info(
        f"  - Log Level: [Console] {settings.log.console_level} / [File] {settings.log.file_level}, Path: {settings.log.file_path}"
    )
    logger.info(f"  - Kafka Consumer: {settings.consumer.model_dump()}")
    logger.info(f"  - Kafka Producer: {settings.producer.model_dump()}")
    logger.info(f"  - Schema Registry: {settings.schema_registry.model_dump()}")

    # Kafka 클라이언트 초기화
    consumer = AIOKafkaConsumer(
        settings.consumer.topic,
        bootstrap_servers=settings.consumer.bootstrap_servers,
        group_id=settings.consumer.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.producer.bootstrap_servers,
        compression_type=settings.producer.compression_type,
        max_request_size=settings.producer.max_request_size_mb * 1048576,
    )

    await consumer.start()
    await producer.start()
    logger.info("Kafka consumer and producer started successfully.")

    try:
        while True:
            result = await consumer.getmany(timeout_ms=1000, max_records=200)
            if not result:
                continue

            for tp, messages in result.items():
                logger.info(f"Fetched {len(messages)} messages from partition {tp}.")
                tasks = [
                    process_message(msg.value, producer)
                    for msg in messages
                    if msg.value is not None
                ]
                if tasks:
                    await asyncio.gather(*tasks)

                # __debug__가 False일 때 (프로덕션 모드)만 오프셋을 커밋합니다.
                if __debug__:
                    continue
                await consumer.commit()
                logger.info("Offset committed successfully for the processed batch.")

    finally:
        logger.info("Application shutting down.")
        await producer.stop()
        await consumer.stop()
        logger.info("All resources have been cleaned up. Application terminated.")


def run():
    uvloop.install()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application terminated by user.")
    except Exception:
        logger.exception("A fatal error occurred during application execution.")