import re

from ..exceptions import TestCodeExtractionError
from .base import BaseParser
from .inspector_log_parser import DefaultLogParser, RFInspectorLogParser

PARSERS: dict[str, type[BaseParser]] = {
    "TOP41": RFInspectorLogParser,  # RF Final Test 공정
    "TOP42": RFInspectorLogParser,  # RF Calibration 공정
}


def get_parser(test_code: str) -> BaseParser:
    """테스트 코드에 맞는 파서 인스턴스를 반환하는 팩토리 함수"""
    parser_class = PARSERS.get(test_code, DefaultLogParser)
    return parser_class()


def get_test_code(message: bytes) -> str:
    """메시지에서 테스트 코드를 추출하는 함수"""
    pattern = r"\r\nTESTCODE\s*:(.*?)\r\n"
    raw_text = message.decode("utf-8")

    match = re.search(pattern, raw_text)
    if match is None:
        raise TestCodeExtractionError
    test_code = match.group(1).strip()
    return test_code