from ..exceptions import ParserNotFoundError
from .base import BaseParser
from .inspector_log_parser import InspectorLogParser

PARSERS: dict[str, type[BaseParser]] = {
    "inspector": InspectorLogParser,
}


def get_parser(message_type: str) -> BaseParser:
    """메시지 타입에 맞는 파서 인스턴스를 반환하는 팩토리 함수"""
    parser_class = PARSERS.get(message_type)
    if not parser_class:
        raise ParserNotFoundError(f"'{message_type}'에 해당하는 파서를 찾을 수 없습니다.")
    return parser_class()
