from abc import ABC, abstractmethod


class BaseParser(ABC):
    """모든 파서의 기반이 되는 추상 클래스"""

    @abstractmethod
    def parse(self, message: bytes) -> dict:
        """입력받은 바이트 메시지를 파싱하여 딕셔너리(Json) 형태로 반환"""
        raise NotImplementedError
