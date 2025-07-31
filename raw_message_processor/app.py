import asyncio

import uvloop
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from utils.logger import logger, setup_logger

from .config import settings
from .exceptions import ParsingError, TestCodeExtractionError, UnsupportedTestCodeError
from .parser import get_parser_for, get_test_code
from .schema import full_schema, master_schema, serialize_to_avro


async def process_message(message: bytes, producer: AIOKafkaProducer):
    """카프카의 원 메시지를 처리하여 새로운 토픽으로 전달합니다."""
    decoded_message = message.decode("utf-8", errors="ignore")
    try:
        # 1. 테스크 코드(공정)에 맞는 파서를 선택
        test_code = get_test_code(message)
        parser = get_parser_for(test_code)

        # 2. 데이터를 딕셔너리 타입으로 파싱함
        parsed_message = parser.parse(message)

        # 3. 마스터 데이터(일부 정보)와 디테일 데이터(모든 정보)를 avro타입으로 직렬화 후 각각의 토픽으로 전송
        await producer.send(
            settings.producer.master_topic,
            serialize_to_avro(parsed_message["MASTER"], master_schema),
        )
        await producer.send(
            settings.producer.detail_topic,
            serialize_to_avro(parsed_message, full_schema),
        )

    except TestCodeExtractionError as e:
        logger.error(f"{e}. Skipping message.", extra={"raw_message": decoded_message})
    except UnsupportedTestCodeError as e:
        logger.error(f"{e}. Skipping message.", extra={"raw_message": decoded_message})
    except ParsingError as e:
        logger.error(
            f"[{test_code}] Message Parsing error of {e}",
            extra={"raw_message": decoded_message},
        )
    except Exception as e:
        logger.error(
            f"[{test_code}] An unexpected error occurred during message processing of {e}.",
            extra={"raw_message": decoded_message},
        )


async def main():
    """어플리케이션 초기화 및 실행을 담당합니다."""
    # 로거 세팅 (설정 파일 기반)
    setup_logger(
        console_level=settings.log.console_level,
        file_level=settings.log.file_level,
        log_file=settings.log.file_path,
    )

    logger.info("Application starting with the following settings:")
    logger.info(
        f"  - Log Level: [Console] {settings.log.console_level} / [File] {settings.log.file_level}, Path: {settings.log.file_path}"
    )
    logger.info(f"  - Kafka Consumer: {settings.consumer.model_dump()}")
    logger.info(f"  - Kafka Producer: {settings.producer.model_dump()}")

    # 컨슈머 및 프로듀서 실행
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

    # 카프카 메시지 프로세싱
    try:
        while True:
            result = await consumer.getmany(timeout_ms=10000, max_records=200)
            if not result:
                continue

            tasks = []
            for tp, messages in result.items():
                logger.info(f"Fetched {len(messages)} messages from partition {tp}.")
                tasks.extend(
                    [
                        process_message(message.value, producer)
                        for message in messages
                        if message.value is not None
                    ]
                )
            if tasks:
                await asyncio.gather(*tasks)
            if not settings.debug_mode:
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
