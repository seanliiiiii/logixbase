import os
import threading
import multiprocessing as mp
from pathlib import Path
from typing import Union
import uuid
from typing import get_type_hints

from .constant import LOG_LEVELS, LogLevel
from .schema import LoggerConfig
from .formatter import format_log
from .writer import LogWriter
from .mpwriter import MPLogWriter
from .utils import get_current_time

from ..protocol import OperationProtocol as OPP, IdentityProtocol as IDP


class LogManager(OPP, IDP):
    __instance = None
    __started = False

    @staticmethod
    def get_instance(config: Union[LoggerConfig, dict]):
        """
        获取 LogManager 的单例实例。
        如果传入 use_multiprocess 参数，则使用该参数决定日志写入模式，
        否则采用配置文件中的默认设置。
        """
        if LogManager.__instance is None:
            LogManager.__instance = LogManager(config)
        return LogManager.__instance

    def __init__(self, config: Union[LoggerConfig, dict]):
        if LogManager.__instance is not None:
            raise Exception("LogManager 为单例，请使用 get_instance() 获取实例。")

        self._name = "日志管理器"
        self._uuid: str = str(uuid.uuid4())

        self.config: LoggerConfig = self._overwrite_config(config)
        # 在主进程中初始化 MPLogWriter，子进程中不初始化写入器
        if self.config.multiprocess:
            if mp.current_process().name == "MainProcess":
                self.log_writer = MPLogWriter(self.config)
                self.log("INFO", "多进程日志日志队列已创建，待启动日志管理器")
            else:
                self.log_writer = None
            self.__started = False
        else:
            self.log_writer = LogWriter(self.config)
            self.log_writer.start()
            self.log("INFO", "单进程日志管理器已启动")
            self.__started = True

    @staticmethod
    def _overwrite_config(config: Union[LoggerConfig, dict]) -> LoggerConfig:
        if isinstance(config, dict):
            defaults = [k for k in get_type_hints(LoggerConfig) if k not in config]
            if defaults:
                print(f"[日志管理器] 指定参数未识别，使用默认参数: {defaults}")
            config = LoggerConfig(**config)

        if os.getenv("LOG_PATH"):
            config.log_path = Path(os.getenv("LOG_PATH"))
        if os.getenv("LOG_LEVEL"):
            config.log_level = LogLevel(os.getenv("LOG_LEVEL"))
        os.makedirs(config.log_path, exist_ok=True)
        return config

    def get_id(self):
        return f"{self._name}:{self._uuid}"

    def get_name(self):
        return self._name

    def start(self):
        if not self.__started:
            self.log_writer.stop_event.clear()
            self.log_writer.start()
            self.__started = True
            self.log("INFO", "日志管理器已启动")

    def join(self):
        pass

    def stop(self):
        if self.__started:
            print("日志队列剩余长度：", self.log_writer.log_queue.qsize())
            self.log_writer.stop_event.set()
            self.log("INFO", "关闭日志管理器")
            self.log_writer.join()
            self.__started = False

    def status(self):
        return self.__started

    def INFO(self, message: str, log_id: str = None):
        self.log("INFO", message, log_id)

    def DEBUG(self, message: str, log_id: str = None):
        self.log("DEBUG", message, log_id)

    def WARNING(self, message: str, log_id: str = None):
        self.log("WARNING", message, log_id)

    def CRITICAL(self, message: str, log_id: str = None):
        self.log("CRITICAL", message, log_id)

    def ERROR(self, message: str, log_id: str = None):
        self.log("ERROR", message, log_id)

    def log(self, level: str, message: str, log_id: str = None):
        if LOG_LEVELS.get(level, 0) < LOG_LEVELS.get(self.config.log_level.value, 0):
            return
        log_entry = {
            "timestamp": get_current_time(),
            "level": level,
            "log_id": log_id if log_id else "DEFAULT",
            "thread": threading.current_thread().name,
            "process": os.getpid(),
            "message": message
        }
        formatted = format_log(log_entry, self.config.log_format)
        if self.config.multiprocess and self.log_writer is None:
            print(formatted)
        else:
            self.log_writer.enqueue(formatted)
