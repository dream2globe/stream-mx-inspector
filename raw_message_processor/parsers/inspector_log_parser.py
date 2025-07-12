from utils.logger import logger

from ..exceptions import DelimiterNotFoundError
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
        # 1. 검사 로그에서 컬럼 정보 추출
        columns = self._extract_keys(raw_text)

        # 2. 주석 내용을 삭제
        processed_text = self._remove_comment(raw_text)

        # 3. 로그의 내용을 헤더, 바디, 테일로 구분
        delimiters = {"header": "#INIT", "body": "#TEST", "tail": "#END"}
        delimiter_start_pos = {k: processed_text.find(v) for k, v in delimiters.items()}
        if any(v == -1 for v in delimiter_start_pos.values()):
            raise DelimiterNotFoundError(delimiter_start_pos.keys())

        tested_items = set()
        sequence_number = 0

        # 4. 헤더 앞에 부팅 로그 추출(Optional)
        booting_log = processed_text[: delimiter_start_pos["header"]]
        booting_list = []
        for line in booting_log.split("\r\n"):
            if line.isspace() or line == "" or line is None:
                continue
            test_item = dict(zip(columns, line.split(",", maxsplit=len(columns) - 1)))
            test_item["INSP_DTL_SEQ"] = str(sequence_number)  # 부팅 항목은 0으로 초기화
            # 반복 검사 여부 확인
            tested_items.add(test_item["Test_Conditions"])
            test_item["IS_FINAL"] = (
                "Y" if test_item["Test_Conditions"] in tested_items else "N"
            )
            booting_list.append(test_item)

        # 5. 헤더 추출
        header_log = processed_text[
            delimiter_start_pos["header"] : delimiter_start_pos["body"]
        ]

        header_dict = {}
        for line in header_log.split("\r\n")[1:]:  # delimiter가 있는 첫 줄 제외
            if line.isspace() or line == "" or line is None:
                continue
            if len(splited_line := line.split(":", 1)) == 2:
                key, value = splited_line
                header_dict[self._replace_invalid_key_chars(key)] = value.strip()
            else:
                logger.warning(f"Unexpected header message: {line}")

        # 6. 바디 추출
        body_log = processed_text[
            delimiter_start_pos["body"] : delimiter_start_pos["tail"]
        ]

        body_list = []
        for line in body_log.split("\r\n")[1:]:  # delimiter가 있는 첫 줄 제외
            if line.isspace() or line == "" or line is None:
                continue
            # 추출한 키에 로그 값을 대입
            test_item = dict(
                zip(columns, map(str.strip, line.split(",", maxsplit=len(columns) - 1)))
            )  # 공백 제거
            # 검사 항목의 진행 순서 추가
            test_item["INSP_DTL_SEQ"] = str(sequence_number := sequence_number + 1)
            # 반복 검사 여부 확인
            tested_items.add(test_item["Test_Conditions"])
            test_item["IS_FINAL"] = (
                "Y" if test_item["Test_Conditions"] in tested_items else "N"
            )
            body_list.append(test_item)

        # 7. 테일 추출
        tail_log = processed_text[delimiter_start_pos["tail"] :]
        tail_dict = {}
        for line in tail_log.split("\r\n")[1:]:  # delimiter가 있는 첫 줄 제외
            if line.isspace() or line == "" or line is None:
                continue
            if len(splited_line := line.split(":", 1)) == 2:
                key, value = splited_line
                tail_dict[self._replace_invalid_key_chars(key)] = value.strip()
            else:
                logger.warning(f"Unexpected tail message: {line}")
        tail_dict["SMART_RETEST"] = "Y" if "Skip Passed" in tested_items else "N"
        tail_dict["TOTAL_TEST_ITEM_NUMBER"] = str(sequence_number)

        # 8. 결과 조합 및 반환
        parsed_result = {
            "HEADER": header_dict,
            "BODY": booting_list + body_list,
            "TAIL": tail_dict,
        }
        return parsed_result

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

    def _extract_keys(self, raw_text: str) -> list:
        """사전 정의된 컬럼보다 많은 컬럼을 로그에서 발견하는 경우 넘는 부분만 컬럼을 추가합니다."""
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

        # Test_Conditions로 시작하는 라인에서 키를 추출
        start_pos = self._find_words(
            raw_text,
            ["Test Condition", "Test item,", "Test Item,"],
        )
        if start_pos == -1:
            return default_keys
        end_pos = start_pos + raw_text[start_pos + 2 :].find("\r\n")  # +2는 \r\n 반영

        # 추출된 키가 기본 키보다 더 많은 경우 추가
        extracted_keys = raw_text[start_pos:end_pos].split(",")
        if len(extracted_keys) > len(default_keys):
            default_keys.extend(k.strip() for k in extracted_keys[len(default_keys) :])
        return default_keys

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

        # 2. test_condition의 내용을 더욱 세부적으로 구분함
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
                        key: value
                        for key, value in zip(sequence, map(str.strip, splited_info))
                    }
                )

        return parsed_massage