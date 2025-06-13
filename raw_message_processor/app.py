import asyncio
import json

import uvloop
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .config import settings
from .exceptions import ParserNotFoundError, ParsingError
from .logger import logger
from .parsers import get_parser


async def process_message(message, producer: AIOKafkaProducer):
    """카프가 메시지 전처리"""
    try:
        # 1. 메시지 타입에 적절한 파서를 선택
        message_type = "inspector"
        parser = get_parser(message_type)

        # 2. 메시지를 파싱함
        parsed_message = parser.parse(message.value)

        # 3. 배치 정보를 가져옴
        # 이 부분은 저장용 consumer에서 구현하는 것이 좋을 것 같음
        # (pandas 테이블 활용 배치단위 처리 가능)
        # batch_info = await context.get_data()

        # 4. 실시간 데이터에 배치 정보를 포함
        detail_message = json.dumps(parsed_message).encode("utf-8")
        logger.debug(detail_message)

        # 5. 다른 토픽으로 다시 프로듀싱함
        await producer.send_and_wait(settings.producer.detail_topic, detail_message)
        # await producer.send_and_wait(settings.producer.master_topic, final_message)
        logger.info(
            "Message processed and produced successfully.",
            extra={"produced_message": parsed_message},
        )

    except ParserNotFoundError as e:
        logger.error(f"parser selection error: {e}", extra={"message_type": message_type})
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
    """어플리케이션 초기화 및 실행"""

    # 컨슈머 및 프로듀서 실행
    consumer = AIOKafkaConsumer(
        settings.consumer.topic,
        bootstrap_servers=settings.consumer.bootstrap_servers,
        group_id=settings.consumer.group_id,
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.producer.bootstrap_servers,
        compression_type=settings.producer.compression_type,
    )
    await consumer.start()
    await producer.start()

    # 카프카 메시지 프로세싱
    try:
        while True:
            message_batch = await consumer.getmany(timeout_ms=10000, max_records=100)
            if not message_batch:
                continue

            tasks = []
            for tp, messages in message_batch.items():
                logger.info(f"Fetched {len(messages)} messages from partition {tp}.")
                for message in messages:
                    tasks.append(process_message(message, producer))
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
