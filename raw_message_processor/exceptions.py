class ProcessorError(Exception):
    """예외의 기본 클래스"""

    pass


class ParserNotFoundError(ProcessorError):
    """메시지 타입에 해당하는 파서를 찾을 수 없을 때 발생하는 예외"""

    pass


class ParsingError(ProcessorError):
    """
    메시지 파싱 과정에서 오류가 발생했을 경우 발생하는 예외
    (예: 예상과 다른 로그 형식 등)
    """

    pass


class DelimiterNotFoundError(ParsingError):
    """Log를 구분하는 3개의 Delimiter를 찾지 못하는 경우 발생하는 예외"""

    pass


class KeyExtractionError(ParsingError):
    """Log에서 Body를 구성하는 Key를 찾지 못하는 경우 발생하는 예외"""

    pass
