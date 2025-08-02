from utils.logger import logger

from ..exceptions import DelimiterNotFoundError
from ..schema import master_fields
from .base import BaseParser

DEFAULT_KEYS = [
    "Test_Conditions",
    "Measured_Value",
    "Lower_Limit",
    "Upper_Limit",
    "P_F",
    "Sec",
    "Code_Value",
    "Code_Lower_Limit",
    "Code_Upper_Limit",
]


class DefaultInspectorLogParser(BaseParser):
    def __init__(self, test_code: str) -> None:
        super().__init__(test_code)

    def parse(self, message: bytes) -> dict:
        raw_text = message.decode("utf-8")
        parsed_message = self.process(raw_text)
        return parsed_message

    def process(self, raw_text: str) -> dict:
        """휴대폰 검사기의 원 로그(text)를 딕셔너리로 변경합니다.

        Args:
            raw_text (str): 휴대폰 검사기 로그(원 데이터)를 의미함

        Raises:
            DelimiterNotFoundError: HEADER, BODY, TAIL의 구별자를 찾지 못하는 경우 발생

        Returns:
            dict: 검사 성적서의 text를 딕셔너리로 변경한 값
                BOOTING, HEADER, BODY, TAIL의 4개 부분을
                MASTER(HEADER + TAIL), DETAIL(BOOTING + BODY)로 재구성
        """
        # 기본 키만 활용하고 추후 additional key를 반영할 지 검토(25.07.28)
        # 1. 검사 로그에서 컬럼 정보 추출
        # additional_columns = self._extract_additional_keys(raw_text)

        # 2. 주석 내용을 삭제
        cleaned_text = self._remove_comment(raw_text)

        # 3. 로그의 내용을 헤더, 바디, 테일로 구분
        delimiters = {"header": "#INIT", "body": "#TEST", "tail": "#END"}
        delimiter_start_pos = {k: cleaned_text.find(v) for k, v in delimiters.items()}
        if any(v == -1 for v in delimiter_start_pos.values()):
            raise DelimiterNotFoundError(delimiter_start_pos.keys())

        # 4. 측정값 추출
        # 4-1. 헤더 앞에 부팅 로그 추출(Optional), 바디에 포함하되 검사 순서값은 0으로 고정
        booting_log = cleaned_text[: delimiter_start_pos["header"]]
        booting_record = self._log_to_record(booting_log, False)
        # 4-2. 바디 추출, 검사 순서값이 1씩 증가
        body_log = cleaned_text[delimiter_start_pos["body"] : delimiter_start_pos["tail"]]
        body_record = self._log_to_record(body_log, True)

        # 5. 헤드와 테일 부분의 검사 요약정보
        header_log = cleaned_text[delimiter_start_pos["header"] : delimiter_start_pos["body"]]
        tail_log = cleaned_text[delimiter_start_pos["tail"] :]
        summary_dict = self._log_to_dict(header_log + tail_log)

        # 6. 결과 조합 및 반환
        parsed_message = {
            "MASTER": summary_dict,
            "DETAIL": booting_record + body_record,
        }
        return parsed_message

    def _log_to_record(
        self,
        raw_text: str,
        incremental_sequence: bool = True,
    ) -> list[dict[str, str]]:
        """csv형태의 로그 내용을 dictionary record 형태로 변환함"""

        # 딕셔너리 리스트로 변환
        records = []
        sequence = 0
        for line in raw_text.split("\r\n"):
            if not line or line.isspace():
                continue
            if line[0] == "#":
                continue
            test_item = dict(zip(DEFAULT_KEYS, map(str.strip, line.split(","))))
            test_item["INSP_DTL_SEQ"] = str(sequence := sequence + 1) if incremental_sequence else str(0)
            records.append(test_item)

        # 최종 검사여부 확인
        tested_items = set()
        for record in records[::-1]:  # 역순으로 처음 등장 항목을 Y로 지정
            record["IS_FINAL"] = "Y" if record["Test_Conditions"] not in tested_items else "N"
            tested_items.add(record["Test_Conditions"])
        return records

    def _log_to_dict(
        self,
        raw_text: str,
    ) -> dict[str, str | dict[str, str]]:
        """'key:value' 형태의 로그를 딕셔너리로 변환합니다.

        Args:
            log (str): 'key:value' 형태로 줄바꿈 되는 텍스트 로그
            mandatory_columns (set[str]): 상위 레벨의 필수 키로 등록될 컬러

        Returns:
            dict[str | str]: _description_
        """
        default_dict = {}
        additional_dict = {}
        for line in raw_text.split("\r\n"):
            # empty line or delimiters
            if not line or line.isspace():
                continue
            if line[0] == "#":
                continue

            splited_line = tuple(map(str.strip, line.split(":")))
            match len(splited_line):
                # normal case
                case 2:
                    key = self._replace_invalid_key_chars(splited_line[0])
                    value = splited_line[1]
                    if key in master_fields:
                        default_dict[key] = value
                    else:
                        additional_dict[key] = value
                # abnormal case
                case length if length > 2:
                    match splited_line[0]:
                        case "TIME" | "RDM_LOT":  # 시:분:초 데이터를 포함
                            default_dict[splited_line[0]] = ":".join(splited_line[1:])
                        case "RDMLOT":
                            default_dict["RDM_LOT"] = ":".join(splited_line[1:])
                        case _:  # 마지막 데이터만 취득
                            key = self._replace_invalid_key_chars(splited_line[-2])
                            additional_dict[key] = splited_line[-1]
                            logger.warning(f"An unexpected message of {line} in {self.test_code}")
                # case of no ':'
                case _:
                    logger.warning(f"An unexpected message of {line} in {self.test_code}")
        default_dict.update({"ADDITIONAL_INFO": additional_dict})
        return default_dict

    def _remove_comment(self, raw_text: str) -> str:
        """C++ 스타일의 주석 등 불필요 문장을 제거합니다."""
        raw_text = self._remove_between(raw_text, between=("/*", "*/"))  # 범위 주석
        raw_text = self._remove_between(raw_text, between=("//", "\r\n"))  # 한줄 주석
        raw_text = self._remove_between(raw_text, between=("<<", ">>"))  # << 타이틀 >>
        raw_text = self._remove_between(raw_text, between=("===", "\r\n"))  # === 타이틀
        return raw_text

    def _remove_between(self, raw_text: str, between: tuple[str, str]):
        """between의 시작 및 종료 문자 사이 내용을 제거합니다."""
        while True:
            start_pos = raw_text.find(between[0])
            if start_pos == -1:
                break
            else:
                end_pos = start_pos + raw_text[start_pos:].find(between[1]) + len(between[1])
                raw_text = raw_text[:start_pos] + raw_text[end_pos:]
        return raw_text

    def _extract_additional_keys(self, raw_text: str) -> list[str]:
        """사전 정의된 컬럼(default_keys)보다 더 정의된 컬럼을 찾아 반환합니다."""

        # 컬럼의 시작 위치를 찾아 키 정보가 담긴 문자열 추출
        start_pos = self._find_words(
            raw_text,
            ["Test Condition", "Test item,", "Test Item,"],
        )
        if start_pos == -1:
            return []
        end_pos = start_pos + raw_text[start_pos:].find("\r\n") + 2  # \r\n 반영

        # 추출된 키가 기본 키보다 더 많은 경우 추가되는 부분만 리스트로 구성하여 반환
        extracted_keys = raw_text[start_pos:end_pos].split(",")
        additional_keys = (
            [k.strip() for k in extracted_keys[len(DEFAULT_KEYS) :]] if len(extracted_keys) > len(DEFAULT_KEYS) else []
        )
        return additional_keys

    def _find_words(self, text: str, words: list[str]) -> int:
        for word in words:
            pos = text.find(word)
            if pos != -1:
                break
        return pos

    def _replace_invalid_key_chars(self, key: str):
        """딕셔너리의 키값으로 사용할 수 없는 문자를 변경"""
        table = {47: 95, 45: 95, 40: 95}  # 대체 문자 매핑
        key = (  # 공백, 괄호 제거
            key.translate(table)
            .strip()
            .replace(" ", "_")
            .replace("(", "_")
            .replace(")", "_")
            .replace(".", "_")
            .replace("-", "_")
            .replace("/", "_")
            .replace("\\", "_")
        )
        return key


class RFInspectorLogParser(DefaultInspectorLogParser):
    def __init__(self, test_code: str) -> None:
        super().__init__(test_code)

    def process(self, raw_text: str) -> dict:
        """
        검사 항목(이름)을 참고하여 6가지 정보를 추출 후 파싱 결과(바디)에 추가 반영합니다.
        예) NR_n78_TX_636666CH_S876 R23 A54 P8_Ant54 SRS Tx Power 20dBm
            ===========================================================
            NR(Tech), n78(Band), TX(RX or TX), 636666CH(Channel),
            S876 R23 A54 P8(Signal Path), Ant54 SRS Tx Power 20dBm(Item)
        Args:
            raw_text (str): 원 검사기 로그

        Returns:
            dict[str, str | list[dict]]: 바디의 테스트 레코드에 6가지 정보를 추가한 딕셔너리
        """
        # 1. 기본 파서로 먼저 데이터 정제함
        parsed_massage = super().process(raw_text)

        # 2. test_condition의 내용을 더욱 세부적으로 구분하여 rf_info 컬럼에 추가함
        sequence = ["tech", "band", "direction", "channel", "sigpath", "item"]
        for test_record in parsed_massage["DETAIL"]:
            splited_info = test_record["Test_Conditions"].split("_", maxsplit=len(sequence) - 1)
            if len(splited_info) <= 3:
                continue  # 아랫열에서 out of range 에러 방지
            if splited_info[2].upper() == "RX" or splited_info[2].upper() == "TX":
                test_record.update(
                    {"RF_INFO": {key: value for key, value in zip(sequence, map(str.strip, splited_info))}}
                )
        return parsed_massage
