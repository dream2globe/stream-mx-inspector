import os

import httpx
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_avro, from_json, struct
from pyspark.sql.types import StringType, StructField, StructType
from streamer.config import Settings, load_settings
from streamer.logger import setup_logging
from streamer.processing import process_batch
from streamer.spark_session import create_spark_session


def enrich_with_api_data(partition_iterator, api_url: str):
    """
    파티션 단위로 외부 API를 호출하여 데이터를 보강합니다.
    """
    api_client = httpx.Client()  # 파티션별로 클라이언트 생성
    for row in partition_iterator:
        user_id = row.user_id
        enriched_data = {"extra_info": "default_value"}  # 기본값
        try:
            response = api_client.get(f"{api_url}/users/{user_id}")
            response.raise_for_status()
            enriched_data = response.json()
        except httpx.HTTPStatusError as e:
            logger.warning(f"API call failed for user_id {user_id}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during API call: {e}")

        # 원본 row와 보강된 데이터를 합쳐서 반환
        original_dict = row.asDict()
        original_dict.update(enriched_data)
        yield original_dict
    api_client.close()


def process_batch(df: DataFrame, epoch_id: int, settings: Settings, avro_schema_json: str):
    """
    마이크로배치별 데이터 처리 로직 (foreachBatch에서 호출)
    """
    if df.isEmpty():
        logger.info(f"Epoch {epoch_id}: Batch is empty, skipping.")
        return

    logger.info(f"Epoch {epoch_id}: Processing batch with {df.count()} records.")

    # 1. Avro 데이터 역직렬화
    # Confluent Schema Registry를 사용하는 경우, from_avro는 자동으로 스키마를 가져옵니다.
    # 로컬 파일 스키마를 사용하는 경우, 스키마를 직접 제공해야 합니다.
    if settings.env == "prod":
        from_avro_options = {
            "mode": "PERMISSIVE",
            "schema.registry.url": settings.kafka.schema_registry_url,
            "schema.registry.subject": f"{settings.kafka.topic}-value",
        }
        deserialized_df = (
            df.select(col("value").cast("binary").alias("avro_data"))
            .select(from_avro("avro_data", from_avro_options).alias("data"))
            .select("data.*")
        )
    else:  # dev 환경
        deserialized_df = df.select(from_avro(col("value"), avro_schema_json).alias("data")).select(
            "data.*"
        )

    deserialized_df.persist()

    # 2. 데이터 보강 (API 호출)
    # mapPartitions를 사용하여 효율적으로 API 호출
    enriched_rdd = deserialized_df.rdd.mapPartitions(
        lambda p: enrich_with_api_data(p, settings.api.enrichment_api_url)
    )

    # 보강된 데이터의 스키마를 정의해야 함
    # 원본 스키마 + 추가된 컬럼
    enriched_schema = StructType(
        deserialized_df.schema.fields
        + [
            StructField("extra_info", StringType(), True)  # 예시: API 결과로 추가된 컬럼
        ]
    )
    enriched_df = enriched_rdd.toDF(schema=enriched_schema)

    # 3. Iceberg 테이블에 저장
    logger.info(
        f"Epoch {epoch_id}: Writing {enriched_df.count()} enriched records to Iceberg table '{settings.iceberg.target_table}'."
    )
    try:
        enriched_df.writeTo(settings.iceberg.target_table).append()
        logger.info(f"Epoch {epoch_id}: Successfully wrote data to Iceberg.")
    except Exception as e:
        logger.error(f"Epoch {epoch_id}: Failed to write to Iceberg. Error: {e}")
        # 실패 시 재시도 또는 알림 로직 추가 가능

    deserialized_df.unpersist()


def main():
    """
    Spark 스트리밍 애플리케이션 메인 함수
    """
    # 1. 설정 및 로거 초기화
    settings = load_settings()
    setup_logging(settings.log_level)
    logger.info(f"Starting application in '{settings.env}' mode.")
    logger.info(f"Loaded settings: {settings.model_dump_json(indent=2)}")

    # 2. SparkSession 생성
    spark = create_spark_session(settings)

    # 개발 환경용 로컬 Avro 스키마 로드
    avro_schema_json = None
    if settings.env == "dev":
        try:
            with open(settings.kafka.schema_file_path, "r") as f:
                avro_schema_json = f.read()
            logger.info(f"Loaded local Avro schema from {settings.kafka.schema_file_path}")
        except Exception as e:
            logger.error(f"Failed to load local Avro schema: {e}")
            return

    # 3. Kafka 소스 스트림 생성
    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 4. 스트리밍 쿼리 정의 및 실행
    query = (
        stream_df.writeStream.trigger(processingTime="1 minute")
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, settings, avro_schema_json))
        .option(
            "checkpointLocation",
            f"{settings.iceberg.catalog_warehouse}/_checkpoints/{settings.spark.app_name}",
        )
        .start()
    )

    logger.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()
