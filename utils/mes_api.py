import requests
from loguru import logger
from pyspark.sql import Row

from .config import ApiSettings


def enrich_data_via_api(partition, api_settings: ApiSettings):
    """
    파티션 단위로 API를 호출하여 데이터를 보강합니다.
    mapPartitions 내에서 실행되어 커넥션 재사용.
    """
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {api_settings.api_key}"})

    for row in partition:
        row_dict = row.asDict()
        user_id = row_dict.get("user_id")

        if user_id:
            try:
                response = session.get(f"{api_settings.base_url}/users/{user_id}")
                response.raise_for_status()
                user_info = response.json()
                # 원본 데이터에 user_name, user_age 컬럼 추가
                row_dict["user_name"] = user_info.get("name")
                row_dict["user_age"] = user_info.get("age")
            except requests.exceptions.RequestException as e:
                logger.warning(f"API call failed for user_id {user_id}: {e}")
                row_dict["user_name"] = None
                row_dict["user_age"] = None

        yield Row(**row_dict)
