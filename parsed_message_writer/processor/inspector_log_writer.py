import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, lit, struct, to_date, to_json, udf
from pyspark.sql.types import StringType, StructType

from .config import get_settings
from .logger import log

settings = get_settings()


def enrich_data_with_api(user_ids: list[str]) -> dict[str, str]:
    """
    사용자 ID 목록을 받아 외부 API를 호출하고 보강된 정보를 반환하는 함수 (예시)
    실제 프로덕션에서는 비동기 I/O, 재시도 로직 등을 고려해야 합니다.
    """
    api_url = f"{settings.app.api.base_url}/users"
    headers = {"Authorization": f"Bearer {settings.env_vars.enrichment_api_key}"}

    try:
        # 실제 API는 보통 POST로 여러 ID를 한번에 조회하는 기능을 제공합니다.
        response = requests.post(
            api_url, json={"user_ids": user_ids}, headers=headers, timeout=5
        )
        response.raise_for_status()
        # API 응답이 {"user_id_1": "info_1", "user_id_2": "info_2"} 형태라고 가정
        return response.json()
    except requests.exceptions.RequestException as e:
        log.error(f"API call failed: {e}")
        # 실패 시, 각 user_id에 대해 기본값 반환
        return {user_id: "API_CALL_FAILED" for user_id in user_ids}


def foreach_batch_processor(df: DataFrame, batch_id: int):
    """
    Spark 스트리밍의 각 마이크로배치를 처리하는 함수.
    1. 데이터 보강 (API 호출)
    2. Iceberg 테이블에 저장
    """
    spark = df.sparkSession
    if df.isEmpty():
        log.info(f"Batch {batch_id} is empty. Skipping.")
        return

    log.info(f"Processing batch {batch_id} with {df.count()} records.")

    # --- 1. 데이터 보강 ---
    # API 호출 오버헤드를 줄이기 위해, 배치 내 고유한 user_id만 추출
    unique_user_ids = [row.user_id for row in df.select("user_id").distinct().collect()]

    if not unique_user_ids:
        log.warning(f"Batch {batch_id} has no user_ids to enrich.")
        enriched_df = df.withColumn("enriched_user_info", lit("NO_USER_ID"))
    else:
        log.info(f"Enriching data for {len(unique_user_ids)} unique users.")
        enriched_data = enrich_data_with_api(unique_user_ids)

        # 보강된 데이터를 Spark DataFrame으로 변환
        enriched_map_df = spark.createDataFrame(
            enriched_data.items(), ["user_id_map", "enriched_user_info"]
        )

        # 원본 DataFrame과 조인하여 'enriched_user_info' 컬럼 추가
        enriched_df = df.join(
            enriched_map_df,
            df["user_id"] == enriched_map_df["user_id_map"],
            "left_outer",
        ).drop("user_id_map")

    # event_timestamp를 기준으로 event_date 파티션 키 생성
    final_df = enriched_df.withColumn("event_date", to_date(col("event_timestamp")))

    # --- 2. Iceberg 테이블에 저장 ---
    target_table = f"{settings.app.iceberg.catalog_name}.{settings.app.iceberg.db_name}.{settings.app.iceberg.table_name}"
    log.info(f"Writing {final_df.count()} records to Iceberg table: {target_table}")

    # 컬럼 순서 및 타입을 테이블 스키마에 맞게 정렬
    # 실제 스키마에 맞게 select 구문 조정 필요
    final_df.select(
        "event_id",
        "user_id",
        "event_timestamp",
        "event_date",
        "payload",
        "serial_number",
        "enriched_user_info",
    ).writeTo(target_table).append()

    log.info(f"Successfully processed batch {batch_id}.")
