from loguru import logger
from pyspark.sql import SparkSession
from streamer.config import Settings


def create_spark_session(settings: Settings) -> SparkSession:
    """
    환경 설정에 따라 SparkSession을 생성하고 구성합니다.
    """
    logger.info(f"Creating SparkSession for '{settings.env}' environment.")

    # 필수 JAR 파일 목록
    # Spark 3.4+, Iceberg 1.4+, Kafka 3, AWS SDK 2.x 기준
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        "org.apache.spark:spark-avro_2.12:3.4.1",
        "software.amazon.awssdk:bundle:2.17.257",  # S3 호환 스토리지용
        "org.postgresql:postgresql:42.6.0",  # Iceberg JDBC 카탈로그용
    ]

    builder = (
        SparkSession.builder.appName(settings.spark.app_name)
        .master(settings.spark.master)
        .config("spark.jars.packages", ",".join(packages))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{settings.iceberg.catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{settings.iceberg.catalog_name}.type", "jdbc")
        .config(
            f"spark.sql.catalog.{settings.iceberg.catalog_name}.uri", settings.iceberg.catalog_uri
        )
        .config(f"spark.sql.catalog.{settings.iceberg.catalog_name}.jdbc.user", settings.db_user)
        .config(
            f"spark.sql.catalog.{settings.iceberg.catalog_name}.jdbc.password", settings.db_password
        )
        .config(
            f"spark.sql.catalog.{settings.iceberg.catalog_name}.warehouse",
            settings.iceberg.catalog_warehouse,
        )
        .config("spark.sql.defaultCatalog", settings.iceberg.catalog_name)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )  # S3 파일시스템 사용 설정

    if settings.env == "prod":
        # 프로덕션 환경 추가 설정 (예: K8s 관련)
        # .config("spark.kubernetes.container.image", "your-repo/kafka-iceberg-streamer:latest")
        pass

    logger.info("SparkSession configured. Building...")
    return builder.getOrCreate()
