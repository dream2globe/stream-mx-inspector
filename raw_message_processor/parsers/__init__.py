import re

from ..exceptions import TestCodeExtractionError, UnsupportedTestCodeError
from .base import BaseParser
from .inspector_log_parser import DefaultInspectorLogParser, RFInspectorLogParser

# 지원하는 파서들을 test_code를 키로 하여 딕셔너리에 등록합니다.
# 새로운 파서를 추가할 때 이 딕셔너리만 수정하면 됩니다.
_parsers: dict[str, type[BaseParser]] = {
    "TOP42": RFInspectorLogParser,
    "TOP41": RFInspectorLogParser,
}


def get_parser_for(test_code: str) -> BaseParser:
    """
    주어진 test_code에 맞는 파서 인스턴스를 반환하는 팩토리 함수입니다.

    Args:
        test_code: 파서를 결정하기 위한 테스트 코드.

    Returns:
        test_code에 해당하는 BaseParser의 인스턴스.
    """
    parser_class = _parsers.get(test_code, DefaultInspectorLogParser)
    if not parser_class:
        raise UnsupportedTestCodeError(test_code)
    return parser_class(test_code)


def get_test_code(message: bytes) -> str:
    """메시지에서 테스트 코드를 추출하는 함수입니다."""
    pattern = re.compile(rb"\r\nTESTCODE\s*:\s*(.*?)\s*\r\n", re.IGNORECASE)
    match = pattern.search(message)

    if not match:
        raise TestCodeExtractionError()

    return match.group(1).decode("utf-8")