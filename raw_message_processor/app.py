import asyncio
import json
from pathlib import Path

import uvloop
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from utils.logger import logger, setup_logger

from .config import settings
from .exceptions import ParsingError
from .parsers import get_parser, get_test_code

LOG_FILE_PATH = Path(__file__).parent.parent / "logs" / "raw_message_processor.log"


async def process_message(message, producer: AIOKafkaProducer):
    """카프카의 원 메시지를 처리하여 새로운 토픽으로 전달합니다."""
    try:
        # 1. 테스크 코드(공정)에 맞는 파서를 선택
        test_code = get_test_code(message.value)
        parser = get_parser(test_code)

        # 2.디테일 데이터를 파싱하여 전송
        parsed_message = parser.parse(message.value)
        await producer.send(
            settings.producer.detail_topic, json.dumps(parsed_message).encode("utf-8")
        )

        # 3.마스터 데이터를 파싱하여 전송 (바디 없이 헤더, 테일 정보만 포함)
        del parsed_message["BODY"]
        await producer.send(
            settings.producer.detail_topic, json.dumps(parsed_message).encode("utf-8")
        )

    except ParsingError as e:
        logger.error(
            f"Message Parsing error: {e}",
            extra={"raw_message": message.value.decode(errors="ignore")},
        )
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during message processing: {e}.",
            extra={"raw_message": message.value.decode(errors="ignore")},
        )


async def main():
    """어플리케이션 초기화 및 실행을 담당합니다."""
    # 로커 세팅
    setup_logger(log_level="INFO", log_file=LOG_FILE_PATH)

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
        max_request_size=1048576 * 5,
    )
    await consumer.start()
    await producer.start()

    # 카프카 메시지 프로세싱
    try:
        while True:
            message_batch = await consumer.getmany(timeout_ms=10000, max_records=200)
            if not message_batch:
                continue

            for tp, messages in message_batch.items():
                logger.info(f"Fetched {len(messages)} messages from partition {tp}.")
                tasks = [process_message(message, producer) for message in messages]

            if tasks:
                await asyncio.gather(*tasks)

            # await consumer.commit()
            logger.info("Offset committed succuessfully for the processed batch.")

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