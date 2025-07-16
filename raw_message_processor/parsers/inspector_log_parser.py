from utils.logger import logger

from ..exceptions import DelimiterNotFoundError
from ..schema import get_mandatory_keys, partial_schema
from .base import BaseParser


class DefaultLogParser(BaseParser):
    def parse(self, message: bytes) -> dict[str, str | list[str]]:
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
                BOOTING, HEADER, BODY, TAIL의 4개 키로 구성되어 있으며
                부팅 과정, 검사 시작전 정보, 측정값, 최종 판정 결과를 의미함
        """
        # 1. 검사 로그에서 컬럼 정보 추출(주석 삭제 전 필요)
        predefined_columns, additional_columns = self._extract_keys(raw_text)

        # 2. 주석 내용을 삭제
        processed_text = self._remove_comment(raw_text)

        # 3. 로그의 내용을 헤더, 바디, 테일로 구분
        delimiters = {"header": "#INIT", "body": "#TEST", "tail": "#END"}
        delimiter_start_pos = {k: processed_text.find(v) for k, v in delimiters.items()}
        if any(v == -1 for v in delimiter_start_pos.values()):
            raise DelimiterNotFoundError(delimiter_start_pos.keys())

        # 4. 헤더 앞에 부팅 로그 추출(Optional), 바디에 포함하되 검사 순서값은 0으로 고정
        booting_log = processed_text[: delimiter_start_pos["header"]]
        booting_record = self.log_to_record(
            booting_log, predefined_columns, additional_columns, False
        )

        # 5. 헤더 추출
        header_log = processed_text[
            delimiter_start_pos["header"] : delimiter_start_pos["body"]
        ]
        mandatory_columns = get_mandatory_keys(partial_schema.get_field("HEADER"))
        header_dict = self.log_to_dict(header_log, mandatory_columns)

        # 6. 바디 추출
        body_log = processed_text[
            delimiter_start_pos["body"] : delimiter_start_pos["tail"]
        ]
        body_record = self.log_to_record(
            body_log, predefined_columns, additional_columns, True
        )

        # 7. 테일 추출
        tail_log = processed_text[delimiter_start_pos["tail"] :]
        mandatory_columns = get_mandatory_keys(partial_schema.get_field("TAIL"))
        tail_dict = self.log_to_dict(tail_log, mandatory_columns)

        # 8. 결과 조합 및 반환
        parsed_result = {
            "HEADER": header_dict,
            "BODY": booting_record + body_record,
            "TAIL": tail_dict,
        }
        return parsed_result

    def log_to_record(
        self,
        log: str,
        predefined_columns: list[str],
        additional_columns: list[str | None] = None,
        incremental_sequence: bool = True,
    ) -> list[dict[str | str]]:
        """csv형태의 로그 내용을 dictionary records로 변환함"""
        records = []  # 딕셔너리 리스트로 변환
        tested_items = set()  # 중복 검사 체크
        sequence = 0
        for line in log.split("\r\n"):
            if line.isspace() or line == "" or line is None:
                continue
            test_item = dict(
                zip(
                    predefined_columns,
                    map(
                        str.strip,  # 공백 제거
                        line.split(
                            ",", len(predefined_columns) + len(additional_columns) - 1
                        ),
                    ),
                )
            )
            if additional_columns is not None:
                additional_item = dict(
                    zip(
                        additional_columns,
                        map(str.strip, line.split(",")[len(predefined_columns) :]),
                    )
                )
                test_item.update({"ADDITIONAL_INFO": additional_item})
            test_item["INSP_DTL_SEQ"] = (
                str(sequence := sequence + 1) if incremental_sequence else str(0)
            )
            tested_items.add(test_item["Test_Conditions"])  # 이전 검사 여부 확인
            test_item["IS_FINAL"] = (
                "Y" if test_item["Test_Conditions"] in tested_items else "N"
            )
            records.append(test_item)

    def log_to_dict(
        self,
        log: str,
        mandatory_keys: set[str],
        incremental_sequence: bool = True,
    ) -> dict[str | str]:
        """'key:value' 구조의 스트링 로그를 딕셔너리로 변환합니다.

        Args:
            log (str): 'key:value' 형태로 줄바꿈 되는 텍스트 로그
            mandatory_columns (set[str]): 상위 레벨의 필수 키로 등록될 컬러
            incremental_sequence (bool, optional): _description_. Defaults to True.

        Returns:
            dict[str | str]: _description_
        """
        default_dict = {}
        additional_dict = {}
        for line in log.split("\r\n")[1:]:  # delimiter가 있는 첫 줄 제외
            if line.isspace() or line == "" or line is None:
                continue
            if len(splited_line := line.split(":")) == 2:
                if key := splited_line[0] not in mandatory_keys:
                    default_dict[key] = splited_line[1].strip()
                else:
                    additional_dict[self._replace_invalid_key_chars(key)] = (
                        splited_line[1].strip()
                    )
            else:
                logger.warning(f"Unexpected header message: {line}")
        return default_dict.update({"ADDITIONAL_INFO": additional_dict})

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
                end_pos = (
                    start_pos + raw_text[start_pos:].find(between[1]) + len(between[1])
                )
                raw_text = raw_text[:start_pos] + raw_text[end_pos:]
        return raw_text

    def _extract_keys(self, raw_text: str) -> (list[str], list[None | str]):
        """사전 정의된 컬럼(default_keys)과 이를 초과하는 컬럼(additional_keys)를 반환합니다."""
        default_keys = [
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

        # 컬럼의 시작 위치를 찾아 라인에서 키를 추출
        start_pos = self._find_words(
            raw_text,
            ["Test Condition", "Test item,", "Test Item,"],
        )
        if start_pos == -1:
            return default_keys
        end_pos = start_pos + raw_text[start_pos + 2 :].find("\r\n")  # +2는 \r\n 반영

        # 추출된 키가 기본 키보다 더 많은 경우 별도의 리스트로 구성하여 반환
        extracted_keys = raw_text[start_pos:end_pos].split(",")
        additional_keys = (
            [k.strip() for k in extracted_keys[len(default_keys) :]]
            if len(extracted_keys) > len(default_keys)
            else []
        )
        return default_keys, additional_keys

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
        )
        return key


class RFInspectorLogParser(DefaultLogParser):
    def process(self, raw_text: str) -> dict[str, str | list[dict]]:
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
        for test_record in parsed_massage["BODY"]:
            splited_info = test_record["Test_Conditions"].split(
                "_", maxsplit=len(sequence) - 1
            )
            if len(splited_info) <= 3:
                continue  # 아랫열에서 out of range 에러 방지
            if splited_info[2].upper() == "RX" or splited_info[2].upper() == "TX":
                test_record.update(
                    {
                        "rf_info": {
                            key: value
                            for key, value in zip(
                                sequence, map(str.strip, splited_info)
                            )
                        }
                    }
                )
        return parsed_massage
