import argparse
import asyncio
import io
import json
import logging.config
import multiprocessing
import os
import socket
import time as t
from datetime import timedelta
from functools import partial, wraps
from time import time

import pandas as pd
import s3fs
import toml
import uvloop
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from pyarrow import Table
from pyarrow import parquet as pq


def get_columns_dtype_from_avro(path):
    col_names = []
    dtypes = {}
    with open(path) as fp:
        detail_schema = json.load(fp)["fields"]
        for col_schema in detail_schema:
            col_name = col_schema["name"]
            col_type = col_schema["type"][0]
            col_names.append(col_name)
            dtypes[col_name] = col_type
    return col_names, dtypes


def async_wrap(f, loop=None, executor=None):
    if loop is None:
        loop = uvloop.new_event_loop()

    @wraps(f)
    async def run(*args, **kwargs):
        p = partial(f, *args, **kwargs)
        return await loop.run_in_executor(executor, p)

    return run


class AsyncBatchConsumer:
    def __init__(self, conf, args):
        # args
        # self.fcode = args.factory
        self.data_type = args.type
        # toml configs
        self.conf = conf
        self.conf["consumer"] = [{}]
        self.conf["consumer"][0]["topic"] = ""
        self.conf["consumer"][0]["partition"] = args.partition
        self.conf["aiokafka"]["group_id"] = ""
        # code configs
        # self.gbm = conf["code"]["gbm"]
        # self.site = conf["code"]["site"]
        # self.plant = conf["code"]["plant"]
        if self.data_type == "master":
            self.root_path = conf["output"]["master_root_path"]
            self.trigger_msg_size = conf["output"]["master_trigger_msg_size"]
            self.conf["consumer"][0]["topic"] = conf["topic"]["master_topic"]
            self.conf["aiokafka"]["group_id"] = conf["consumergroup"]["master_group_id"]
            self.limit_prcs_num = conf["output"]["master_limit_prcs_num"]
        else:
            self.root_path = conf["output"]["detail_root_path"]
            self.trigger_msg_size = conf["output"]["detail_trigger_msg_size"]
            self.conf["consumer"][0]["topic"] = conf["topic"]["detail_topic"]
            self.conf["aiokafka"]["group_id"] = conf["consumergroup"]["detail_group_id"]
            self.limit_prcs_num = conf["output"]["detail_limit_prcs_num"]

        # operational variable
        self.msg_buffer = []
        self.lastest_save_time = time()
        self.topcode_map = {"TOP41": "TOP41", "TOP42": "TOP42"}
        self.offset = None
        self.saved_offset = multiprocessing.Queue()
        self.err_cnt = multiprocessing.Value("i", 0)
        self.prcs_cnt = multiprocessing.Value("i", 0)
        # data info
        self.schema = get_columns_dtype_from_avro(
            f"./config/common/schema/{self.data_type}_schema.json"
        )
        self.filesystem = self.root_path.split(":")[0]

        # object storage configs
        if self.filesystem == "s3":
            self.object_storage_endpoint = conf["output"]["object_storage_endpoint"]
            self.object_storage_key = conf["output"]["object_storage_key"]
            self.object_storage_secret_key = conf["output"]["object_storage_secret_key"]
            logger.info(f"self {self}")

    def __enter__(self):
        return self

    def __exit__(self):
        self.flush()

    def consume_m(self, num_sem):
        # nonlocal
        sem = asyncio.Semaphore(num_sem)

    @wraps(self)
    async def aconsume(consumer):
        """
        master message 소비를 위한 함수로
        dictionary(bytes)를 json 변환 후 버퍼로 저장
        """
        logger.debug("[c] entering a consuming task")
        async with sem:
            data = await consumer.getmany(timeout_ms=3000)
            for tp, msgs in data.items():
                first_offset = msgs[0].offset
                self.msg_buffer.extend([json.load(io.BytesIO(msg.value)) for msg in msgs])
            self.offset = {tp: msgs[-1].offset + 1}
            logger.debug(f"[c] offset: {first_offset} - {msgs[-1].offset}")

        return aconsume

    def consume_d(self, num_sem):
        # nonlocal
        sem = asyncio.Semaphore(num_sem)

        @wraps(self)
        async def aconsume(consumer):
            """
            detail message 소비를 위한 함수로
            dictionary list(byte)를 입력받는 점이 master와 차이점임
            """
            logger.debug("[c] entering a consuming task")
            async with sem:
                data = await consumer.getmany(timeout_ms=3000)
                for tp, msgs in data.items():
                    first_offset = msgs[0].offset
                    for msg in msgs:
                        self.msg_buffer.extend([j for j in json.load(io.BytesIO(msg.value))])
                self.offset = {tp: msg.offset + 1}
                logger.debug(f"[c] offset: {first_offset} - {msg.offset}")

        return aconsume

    async def save2s3(self, consumer):
        """
        topcode 파티션별로 메세지를 모은 후 일정 크기가 넘거나
        지정된 시간이 경과하면 Storage에 저장함
        """

        def to_parquet(copied_msg, copied_offset):
            logger.info(f"save2s3 = [ss] (pid {os.getpid()}) writing to s3, offset {copied_offset}")
            try:
                self.prcs_cnt.value += 1
                dataframe = (
                    pd.DataFrame.from_dict(copied_msg)
                    .reindex(self.schema[0], axis=1)
                    .astype("string")
                )
                # 중복 제거
                if self.data_type == "master":
                    dataframe = dataframe.drop_duplicates(keep="last")
                # dt, topcode
                dataframe["dt"] = dataframe["INSP_DT"].str[:8]
                dataframe["topcode"] = dataframe["TESTCODE"].map(self.topcode_map).fillna("others")
                # kafka timestamp와 날짜 차이가 심한 데이터는 dt기준 99년 12월 31일에 저장
                series_kdt = pd.to_datetime(dataframe["KDT"], format="%Y%m%d", errors="ignore")
                series_dt = pd.to_datetime(dataframe["dt"], format="%Y%m%d", errors="ignore")
                condition = abs(series_kdt - series_dt) > timedelta(days=14)
                dataframe.loc[condition, "dt"] = "20991231"
                # 데이터프레임 정리
                # dataframe = dataframe.rename(columns={"KDT": "kdt"})
                # dataframe = dataframe.sort_values(by=["kdt", "dt", "topcode"])
                # code 추가
                # dataframe["gbm"] = self.gbm
                # dataframe["site"] = self.site
                # dataframe["plant"] = self.plant
                # partition 분할(year, month, day)
                dataframe["k_year"] = series_kdt.dt.strftime("%Y")
                dataframe["k_mon"] = series_kdt.dt.strftime("%m")
                dataframe["k_day"] = series_kdt.dt.strftime("%d")
                dataframe = dataframe.drop(columns=["KDT"])
                dataframe = dataframe.sort_values(by=["k_year", "k_mon", "k_day", "dt", "topcode"])
                # parquet 저장
                if self.filesystem == "s3":
                    object_storage_fs = s3fs.S3FileSystem(
                        anon=False,
                        use_ssl=False,
                        client_kwargs={
                            "region_name": "ap-northeast-1",
                            "endpoint_url": self.object_storage_endpoint,
                            "aws_access_key_id": self.object_storage_key,
                            "aws_secret_access_key": self.object_storage_secret_key,
                            "verify": False,
                        },
                    )
                    pq.write_to_dataset(
                        Table.from_pandas(dataframe),
                        self.root_path,
                        # partition_cols=["kdt", "dt", "topcode"],
                        partition_cols=["k_year", "k_mon", "k_day", "dt", "topcode"],
                        filesystem=object_storage_fs,
                        use_dictionary=True,
                        compression="snappy",
                        version="2.0",
                    )
                self.saved_offset.put(copied_offset)
                self.err_cnt.value = 0
                logger.info(f"save2s3 = [ss] (pid {os.getpid()}) saved size: {len(copied_msg)}")
            except Exception as ex:
                self.err_cnt.value += 1
                logger.error(
                    f"save2s3 = [ss] (pid {os.getpid()}) raised error during saving to {self.filesystem}"
                )
                logger.error(f"save2s3 = ERROR: {ex}")
            finally:
                self.prcs_cnt.value -= 1

        logger.debug("save2s3 = [s] entering a saving task")
        logger.info(f"save2s3 = [s] checked size: {len(self.msg_buffer)}")
        # 저장 로직
        if (len(self.msg_buffer) < self.trigger_msg_size) and (  # 버퍼 크기
            (time() - self.lastest_save_time) < 60 * 10  # 시간 경과
        ):
            pass
        elif len(self.msg_buffer) == 0:  # 시간이 경과한 후에도 메세지가 없는 경우는 저장하지 않음
            pass
        else:
            copied_msg_from_buffer = self.msg_buffer.copy()
            self.msg_buffer.clear()
            copied_offset = self.offset.copy()
            # child process 생성 후 저장 작업을 할당
            p = multiprocessing.Process(
                target=to_parquet,
                args=(copied_msg_from_buffer, copied_offset),
            )
            p.start()
            self.lastest_save_time = time()
        # offset 설정
        try:
            saved_offset = self.saved_offset.get(block=False)  # Queue가 비었을 때 exception 발생
            logger.info(f"save2s3 = [s] committed offset: {saved_offset}")
            await consumer.commit(saved_offset)
        except Exception:
            logger.debug("save2s3 = Queue is empty")

    def flush(self):
        # executor = ThreadPoolExecutor(max_workers=10)
        # loop = asyncio.get_event_loop()
        loop = uvloop.new_event_loop()
        # executor = None
        loop.run_until_complete(self._aflush())

    async def _aflush(self):
        # setting consumer
        consumer = AIOKafkaConsumer(**self.conf["aiokafka"])
        topics = [TopicPartition(**topic) for topic in self.conf["consumer"]]
        consumer.assign(topics)
        await consumer.start()
        logger.info(f"_aflush = (pid {os.getpid()}) Started a consumer")
        logger.info(
            f"_aflush = (pid {os.getpid()}) Current offset: {[(topic, await consumer.committed(topic)) for topic in topics]}"
        )
        # main loop
        if self.data_type == "master":
            consume_func = self.consume_m(10)
        if self.data_type == "detail":
            consume_func = self.consume_d(5)
        save2s3_func = self.save2s3
        # save2hdfs_func = async_wrap(self.save2hdfs, loop=loop, executor=executor)에서 변경
        tasks = []
        try:
            while True:
                if self.err_cnt.value >= 5:
                    raise Exception(
                        f"_aflush = Exception has raised over 5 times during saving to {self.filesystem}"
                    )
                if self.prcs_cnt.value >= self.limit_prcs_num:
                    logger.info(
                        f"_aflush = (pid {os.getpid()}) Waiting for a child process to finish"
                    )
                    t.sleep(10)
                    continue
                tasks.append(
                    asyncio.create_task(save2s3_func(consumer))
                )  # 2*5:1로 테스크 비중을 달리함
                for _ in range(2):
                    tasks += [asyncio.create_task(consume_func(consumer)) for _ in range(5)]
                    await asyncio.sleep(0)
                await asyncio.wait(tasks)
                tasks.clear()
        except KeyboardInterrupt:
            logger.info("_aflush = Aborted by user")
        except Exception as e:
            logger.info(f"_aflush = {e}")
        finally:
            logger.info(f"_aflush = (pid {os.getpid()}) Finished by exception")
            await consumer.stop()


def run():
    # argparser
    parser = argparse.ArgumentParser(description="Kafka consumer for bigdata platform")
    # parser.add_argument("-F", "--factory", type=str, required=True)
    # parser.add_argument("-T", "--type", type=str, required=True)
    # parser.add_argument("-P", "--partition", type=int, required=True)
    args = parser.parse_args()
    # user setting
    args.factory = socket.gethostname().split("-")[1]
    args.type = socket.gethostname().split("-")[2]
    args.partition = int(socket.gethostname()[-1:])

    if args.type not in ["master", "detail"]:
        raise Exception(f"'{args.type}' is not a valid data type.")

    fcode = args.factory
    tcode = args.type
    partition = args.partition

    print(f"fcode: {fcode}")
    print(f"tcode: {tcode}")
    print(f"partition: {partition}")
    # init
    configs = toml.load(f"./config/site/configs-{fcode}.toml")[fcode]

    def get_logger(logger_name: str, fcode: str, tcode: str, partition: str):
        # logger_conf = toml.load("./config/common/logger.toml")["logger"]
        # logger_conf["handlers"]["rolling"]["filename"] = (f"./logs/{fcode}-{tcode}-consumer.log")
        logger_conf = {
            "version": 1,
            "loggers": {
                "": {
                    "level": "INFO",
                    "handlers": ["console", "rolling"],
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "INFO",
                    "formatter": "info",
                },
                "rolling": {
                    "level": "INFO",
                    "formatter": "info",
                    "class": "logging.handlers.TimedRotatingFileHandler",
                    "backupCount": 100,
                },
            },
            "formatters": {
                "info": {
                    "format": "%(asctime)s-%(levelname)s-%(name)s::%(module)s|%(lineno)s:: %(message)s"
                },
            },
        }
        logger_conf["handlers"]["rolling"]["filename"] = (
            f"./logs/{fcode}/consumer-{fcode}-{tcode}-{partition}.log"
        )

        logging.config.dictConfig(logger_conf)
        logger = logging.getLogger(logger_name)
        return logger

    logger = get_logger(f"consumer-{fcode}", fcode, tcode, partition)
    logger.info(f"consumer setting: {configs}")

    # run
    batcher = AsyncBatchConsumer(configs, args=args)
    batcher.flush()
