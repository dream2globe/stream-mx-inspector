import sys

from loguru import logger

# 기존 로그 핸들러 제거
logger.remove()

# 콘솔(stdout)에는 INFO 레벨 이상의 로그를 출력
logger.add(
    sys.stdout,
    level="INFO",
    format="<green>{time:YYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
)

# ERROR 레벨 이상의 로그를 기록
logger.add(
    "logs/error_{time}.log",
    level="ERROR",
    rotation="10 MB",
    retention="2 weeks",
    serialize=True,  # 로그를 json 형식으로 구조화하여 저장
    enqueue=True,  # 비동기 및 멑티프로세스 환경에서 안전하게 로깅
    backtrace=True,  # 예외 발생 시 전체 스택 트레이스 추적
    diagnose=True,  # 예외 발생 시 변수 값 등 상세 진단 정보 추가
)

# DEBUG 레벨 이상의 로그를 기록(분석용)
logger.add(
    "logs/debug.log",
    level="ERROR",
    rotation="10 MB",
    retention="2 weeks",
    serialize=True,  # 로그를 json 형식으로 구조화하여 저장
    enqueue=True,  # 비동기 및 멑티프로세스 환경에서 안전하게 로깅
    backtrace=True,  # 예외 발생 시 전체 스택 트레이스 추적
    diagnose=True,  # 예외 발생 시 변수 값 등 상세 진단 정보 추가
)
