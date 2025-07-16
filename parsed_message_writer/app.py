import json
from pathlib import Path

from pyspark.sql.functions import col, from_avro

from .config import get_settings
from .logger import log
from .processing import foreach_batch_processor
from .spark_session import get_spark_session


def run():
    """애플리케이션 메인 실행 함수"""
    try:
        settings = get_settings()
        spark = get_spark_session()

        # Kafka 소스 스트림 읽기
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.app.kafka.bootstrap_servers)
            .option("subscribe", settings.app.kafka.topic)
            .option("startingOffsets", settings.app.kafka.starting_offsets)
            .option("kafka.group.id", settings.app.kafka.group_id)
            .load()
        )

        # Avro 역직렬화
        if settings.app.env == "production":
            # 프로덕션: Schema Registry 사용
            from pyspark.sql.avro.functions import from_avro

            # Confluent Schema Registry는 value의 첫 5바이트를 사용하므로, 이를 제외하고 전달
            # from_avro 함수는 Spark 3.4+ 에서 사용 가능
            avro_df = kafka_df.select(
                from_avro(
                    col("value"),
                    settings.app.kafka.schema_registry_url,
                    settings.app.kafka.topic + "-value",
                ).alias("data")
            )
        else:
            # 개발: 로컬 avsc 파일 사용
            schema_path = (
                Path(__file__).parent.parent.parent / "schemas" / "user_events.avsc"
            )
            with open(schema_path, "r") as f:
                avsc = f.read()

            avro_df = kafka_df.select(from_avro(col("value"), avsc).alias("data"))

        # 데이터 구조 펼치기
        # user_events.avsc 스키마에 정의된 필드들을 컬럼으로 변환
        # 예시: {"event_id": "...", "user_id": "...", ...}
        parsed_df = avro_df.select("data.*")

        # foreachBatch를 사용하여 스트리밍 쿼리 실행
        query = (
            parsed_df.writeStream.outputMode("append")
            .trigger(processingTime="1 minute")  # 1분마다 배치 처리
            .foreachBatch(foreach_batch_processor)
            .start()
        )

        log.info("Streaming query started. Waiting for termination...")
        query.awaitTermination()

    except Exception as e:
        log.exception(f"An unexpected error occurred in the streaming application: {e}")
        # 필요 시 리소스 정리 로직 추가
    finally:
        log.info("Streaming application is shutting down.")
        if "spark" in locals() and spark:
            spark.stop()
