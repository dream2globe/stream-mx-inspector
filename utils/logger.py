import sys

from loguru import logger


def setup_logger(log_level="INFO", log_file="logs/app.log"):
    """
    Loguru 로거를 설정합니다.
    - 콘솔: 지정된 레벨 이상 출력
    - 파일: WARNING 레벨 이상만 기록, 10MB마다 교체
    """
    logger.remove()  # 기본 핸들러 제거

    # 콘솔 로거 설정
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )

    # 파일 로거 설정 (WARNING 레벨 이상)
    logger.add(
        log_file,
        level="WARNING",
        rotation="10 MB",  # 10MB 마다 파일 교체
        retention="7 days",  # 7일간 로그 보관
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True,  # 비동기, 멀티프로세스 환경에서 안전
        backtrace=True,
        diagnose=True,
    )
    logger.info("Logger setup complete.")
