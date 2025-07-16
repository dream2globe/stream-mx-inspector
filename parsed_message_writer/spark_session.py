from pyspark.sql import SparkSession

from .config import Settings, get_settings
from .logger import log


def get_spark_session() -> SparkSession:
    """
    설정 파일(config.yml)과 환경 변수(.env)를 기반으로
    개발/프로덕션 환경에 맞는 SparkSession을 생성하고 반환합니다.
    """
    settings = get_settings()
    log.info(f"Creating SparkSession for '{settings.app.env}' environment.")

    builder = SparkSession.builder.appName(settings.app.spark.app_name)

    # 1. 공통 Spark 설정 적용
    for key, value in settings.app.spark.configs.items():
        builder = builder.config(key, value)

    # 2. Iceberg SQL 확장 및 카탈로그 설정
    builder = builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    builder = builder.config(
        f"spark.sql.catalog.{settings.app.iceberg.catalog_name}",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    builder = builder.config(
        f"spark.sql.catalog.{settings.app.iceberg.catalog_name}.catalog-impl",
        "org.apache.iceberg.jdbc.JdbcCatalog",
    )
    builder = builder.config(
        f"spark.sql.catalog.{settings.app.iceberg.catalog_name}.uri",
        settings.app.iceberg.jdbc_url.format(
            user=settings.env_vars.postgres_user,
            password=settings.env_vars.postgres_password,
        ),
    )
    builder = builder.config(
        f"spark.sql.catalog.{settings.app.iceberg.catalog_name}.warehouse",
        settings.app.iceberg.warehouse_path,
    )

    # 3. 환경별 특화 설정
    if settings.app.env == "production":
        # 프로덕션 환경: S3 저장소 설정
        log.info("Applying production-specific Spark configurations for S3.")
        builder = builder.config(
            "spark.hadoop.fs.s3a.access.key", settings.env_vars.s3_access_key
        )
        builder = builder.config(
            "spark.hadoop.fs.s3a.secret.key", settings.env_vars.s3_secret_key
        )
        # S3 엔드포인트, 경로 스타일 등 추가 설정이 필요할 수 있음
        # 예: builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        # 예: builder.config("spark.hadoop.fs.s3a.path.style.access", "true")

    else:  # development
        # 개발 환경: 로컬 파일 시스템 사용. 특별한 추가 설정은 불필요.
        log.info("Applying development-specific Spark configurations.")
        # 필요한 경우 로컬 환경용 Hadoop 설정 등을 추가할 수 있습니다.
        # 예: builder.config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop")

    # 4. Spark 패키지 설정 (Maven 의존성)
    # 프로덕션에서는 Docker 이미지에 jar를 포함시키므로 packages 설정이 불필요할 수 있음
    if settings.app.spark.packages:
        builder = builder.config(
            "spark.jars.packages", ",".join(settings.app.spark.packages)
        )

    spark = builder.getOrCreate()
    log.info("SparkSession created successfully.")
    log.info(f"Spark version: {spark.version}")

    # Iceberg 테이블 생성 (존재하지 않을 경우)
    _create_iceberg_table_if_not_exists(spark, settings)

    return spark


def _create_iceberg_table_if_not_exists(spark: SparkSession, settings: Settings):
    """Iceberg 테이블이 없으면 생성합니다."""
    catalog = settings.app.iceberg.catalog_name
    db = settings.app.iceberg.db_name
    table = settings.app.iceberg.table_name

    full_table_name = f"{catalog}.{db}.{table}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db}")
    log.info(f"Database '{catalog}.{db}' ensured.")

    # 테이블 존재 여부 확인
    tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{db}")
    if tables_df.filter(tables_df.tableName == table).count() == 0:
        log.warning(f"Table '{full_table_name}' not found. Creating new table.")
        # 실제 데이터 스키마에 맞게 수정 필요
        create_table_ddl = f"""
        CREATE TABLE {full_table_name} (
            event_id STRING,
            user_id STRING,
            event_timestamp TIMESTAMP,
            event_date DATE,
            payload STRING,
            serial_number STRING,
            enriched_user_info STRING  -- API 호출로 보강된 정보
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES (
            'write.bucket-by.num-buckets'='4',
            'write.bucket-by.column-names'='serial_number'
        )
        """
        spark.sql(create_table_ddl)
        log.info(f"Table '{full_table_name}' created successfully.")
    else:
        log.info(f"Table '{full_table_name}' already exists.")
