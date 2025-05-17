import os
import threading
import multiprocessing as mp
from pathlib import Path
from typing import Union
import uuid

from .config import load_config
from .constant import LOG_LEVELS
from .formatter import format_log
from .writer import LogWriter
from .mpwriter import MPLogWriter
from .utils import get_current_time

from ..protocol import OperationProtocol as OPP, IdentityProtocol as IDP


class LogManager(OPP, IDP):
    __instance = None
    __started = False

    @staticmethod
    def get_instance(use_mp: bool = None, log_path: Union[str, Path] = None, to_console: bool = True):
        """
        获取 LogManager 的单例实例。
        如果传入 use_multiprocess 参数，则使用该参数决定日志写入模式，
        否则采用配置文件中的默认设置。
        """
        if LogManager.__instance is None:
            LogManager.__instance = LogManager(use_mp, log_path, to_console)
        return LogManager.__instance

    def __init__(self, use_mp: bool = None, log_path: Union[str, Path] = None, to_console: bool = True):
        if LogManager.__instance is not None:
            raise Exception("LogManager 为单例，请使用 get_instance() 获取实例。")

        self._name = "日志管理器"
        self._uuid: str = str(uuid.uuid4())

        self.config = load_config()
        if log_path is None:
            self.log_path = self.config.get("log_path", "./logs")
        else:
            self.log_path = str(log_path)
            self.config["log_path"] = self.log_path
        os.makedirs(self.log_path, exist_ok=True)

        self.log_level = self.config.get("log_level", "INFO")
        self.log_format = self.config.get("log_format", "TEXT")

        if use_mp is None:
            self.use_mp = self.config.get("use_multiprocess", True)
        else:
            self.use_mp = use_mp

        self.config["to_console"] = to_console

        # 在主进程中初始化 MPLogWriter，子进程中不初始化写入器
        if self.use_mp:
            if mp.current_process().name == "MainProcess":
                self.log_writer = MPLogWriter(self.config)
                self.log("INFO", "多进程日志日志队列已创建，待启动日志管理器")
            else:
                self.log_writer = None
        else:
            self.log_writer = LogWriter(self.config)
            self.log_writer.start()
            self.log("INFO", "单进程日志管理器已启动")
            self.__started = True

    def get_id(self):
        return f"{self._name}:{self._uuid}"

    def get_name(self):
        return self._name

    def start(self):
        if not self.__started:
            # 动态生成未定义的快捷方法，如 self.DEBUG(), self.INFO() 等
            for level in LOG_LEVELS:
                if not hasattr(self, level):
                    setattr(self, level, self._create_level_method(level))
            self.log_writer.start()
            self.__started = True

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

    def _create_level_method(self, level: str):
        def log_method(message: str, log_id: str = None):
            self.log(level, message, log_id)
        return log_method

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
        if LOG_LEVELS.get(level, 0) < LOG_LEVELS.get(self.log_level, 0):
            return
        log_entry = {
            "timestamp": get_current_time(),
            "level": level,
            "log_id": log_id if log_id else "DEFAULT",
            "thread": threading.current_thread().name,
            "process": os.getpid(),
            "message": message
        }
        formatted = format_log(log_entry, self.log_format)
        if self.use_mp and self.log_writer is None:
            print(formatted)
        else:
            self.log_writer.enqueue(formatted)
