from typing import KeysView


class ProcessorError(Exception):
    """메시지 처리 중 발생하는 모든 예외의 기본 클래스"""

    pass


class ParsingError(ProcessorError):
    """
    메시지 파싱 과정에서 오류가 발생했을 경우 발생하는 예외
    (예: 예상과 다른 로그 형식, 구분자 누락, 컬럼 이름 누락 등)
    """

    pass


class DelimiterNotFoundError(ParsingError):
    """로그를 헤더, 바디, 테일로 구분하는 구분자를 찾지 못한 경우 발생하는 예외"""

    def __init__(self, found_delimiters: KeysView[str]):
        self.message = f"Not found 3 delimiters, Only {len(found_delimiters)} {'is' if len(found_delimiters) <= 1 else 'are'} found: {found_delimiters}"
        super().__init__(self.message)


class TestCodeExtractionError(ParsingError):
    """메시지 타입을 결정하는 'TESTCODE'값을 찾을 수 없는 경우 발생하는 예외"""

    def __init__(self):
        self.message = "Not found 'TESTCODE' to get a parser class"
        super().__init__(self.message)


class UnsupportedTestCodeError(ParsingError):
    """지원하지 않는 TESTCODE일 경우 발생하는 예외"""

    def __init__(self, test_code: str):
        self.message = f"Unsupported test code: '{test_code}'"
        super().__init__(self.message)
