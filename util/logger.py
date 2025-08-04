import sys

from loguru import logger


def setup_logger(console_level="INFO", file_level="WARNING", log_file="logs/app.log"):
    """
    Loguru 로거를 설정합니다.
    - 콘솔: 지정된 레벨 이상 출력
    - 파일: WARNING 레벨 이상만 기록, 10MB마다 교체하고 한달간 보관
    """
    logger.remove()  # 기본 핸들러 제거

    # 콘솔 로거 설정
    logger.add(
        sys.stderr,
        level=console_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )

    # 파일 로거 설정 (WARNING 레벨 이상)
    logger.add(
        log_file,
        level=file_level,
        rotation="10 MB",  # 10MB 마다 새 파일 생성
        retention="1 month",  # 한 달간 보관
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True,  # 비동기 로깅으로 성능 확보
        backtrace=True,
        diagnose=True,
        # compression="zip",
    )
    logger.info("Logger setup complete.")