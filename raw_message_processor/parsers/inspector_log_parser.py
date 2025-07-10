from ..exceptions import DelimiterNotFoundError, KeyExtractionError
from ..logger import logger
from .base import BaseParser


class DefaultInspectorLogParser(BaseParser):
    def parse(self, message: bytes) -> dict:
        raw_text = message.decode("utf-8")
        parsed_message = self.process(raw_text)
        return parsed_message

    def process(self, raw_text: str) -> dict:
        """휴대폰 검사기 로그(text)를 딕셔너리로 변경

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
        raw_text = self._remove_comment(raw_text)

        # 3. 로그의 내용을 헤더, 바디, 테일로 구분
        delimiters = {"header": "#INIT", "body": "#TEST", "tail": "#END"}
        delimiter_start_pos = {k: raw_text.find(v) for k, v in delimiters.items()}
        if any(v == -1 for v in delimiter_start_pos.values()):
            raise DelimiterNotFoundError

        # 3.1 header 앞에 booting과 관련된 log가 있다면 추출
        booting_log = raw_text[: delimiter_start_pos["header"]]
        booting_list = []
        for line in booting_log.split("\r\n"):
            if line.isspace() or line == "" or line is None:
                continue
            test_item = dict(zip(columns, line.split(",")))
            booting_list.append(test_item)

        # 4. header
        header_log = raw_text[
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

        # 5. body
        body_log = raw_text[delimiter_start_pos["body"] : delimiter_start_pos["tail"]]

        body_list = []
        tested_items = set()
        for seq, line in enumerate(
            body_log.split("\r\n")[1:], start=1
        ):  # delimiter가 있는 첫 줄 제외
            if line.isspace() or line == "" or line is None:
                continue
            # 추출한 키에 로그 값을 대입
            test_item = dict(zip(columns, line.split(",")))
            # 검사 항목의 진행 순서 추가
            test_item["INSP_DTL_SEQ"] = seq
            # 중복 검사 이력이 있는지 확인하여 Y, N 추가
            tested_items.add(test_item["Test_Conditions"])
            test_item["IS_FINAL"] = (
                "Y" if test_item["Test_Conditions"] in tested_items else "N"
            )
            body_list.append(test_item)

        # 6. tail
        tail_log = raw_text[delimiter_start_pos["tail"] :]

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

        # 7. 결과 조합
        return {
            "PREHEADER": booting_list,
            "HEADER": header_dict,
            "BODY": body_list,
            "TAIL": tail_dict,
        } if booting_list else {
            "HEADER": header_dict,
            "BODY": body_list,
            "TAIL": tail_dict,
        }

    def _remove_comment(self, raw_text: str) -> str:
        """C++ 스타일의 주석 등 불필요 문장을 제거함"""
        raw_text = self._remove_between(raw_text, between=("/*", "*/"))  # 범위 주석
        raw_text = self._remove_between(raw_text, between=("//", "\r\n"))  # 한줄 주석
        raw_text = self._remove_between(raw_text, between=("<<", ">>"))  # << 타이틀 >>
        raw_text = self._remove_between(raw_text, between=("===", "\r\n"))  # === 타이틀
        return raw_text

    def _remove_between(self, raw_text: str, between: tuple[str, str]):
        """between의 시작 및 종료 문자 사이 내용을 제거"""
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
        """사전 정의된 컬럼보다 많은 컬럼을 로그에서 발견하는 경우 넘는 부분만 반영하여 확장"""
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
        start_pos = (
            self._find_words(raw_text, ["\r\nTest Item", "\r\nTest Condition"]) + 2
        )
        if start_pos == -1:
            raise KeyExtractionError
        end_pos = start_pos + raw_text[start_pos + 2 :].find("\r\n")  # +2는 \r\n 반영

        # 추출된 키가 기본 키보다 더 많은 경우 추가
        extracted_keys = raw_text[start_pos:end_pos].split(",")
        if len(extracted_keys) > len(default_keys):
            default_keys.extend(k.strip() for k in extracted_keys[len(default_keys) :])
        return default_keys

    def _find_words(self, text: str, words: list[str]) -> int | None:
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


class RFInspectorLogParser(DefaultInspectorLogParser):
    def process(self, raw_text: str) -> dict:
        # 1. 기본 파서로 먼저 데이터 정제함 
        parsed_massage = super().process(raw_text)

        # 2. test_condition의 내용을 더욱 세부적으로 구분함
        enriched_body = [self.enriched_test_item(body) for body in parsed_massage["body"]]
        parsed_massage["body"] = enriched_body
        return parsed

    def enriched_test_item(test_item: dict[str, str]) -> dict:
        """분석이 용이하도록 검화 항목의 정보를 세분화함 

        Args:
            test_item (dict): 검사기 Log의 Body부분에 기록된 검사 측정값 

        Returns:
            dict: Tech_Band_RTX_Channel_SigPath_Item 형태의 검사 항목을 "_" 기준으로 구분하여
                기존 항목에 키와 값을 추가
        """

        sequence = ["tech", "band", "rtx", "channel", "sigpath", "item"]
        splited_info = test_item["Test_Conditions"].split("_")
        if len(splited_info) != 6:
            return test_item

        enriched_body = {key:info for key, info in zip(sequence, splited_info)}
        test_item.update(enriched_body)
        return test_item