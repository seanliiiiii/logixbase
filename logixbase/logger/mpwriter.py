import multiprocessing as mp
import os
import datetime
from datetime import date, datetime, timedelta


from .schema import LoggerConfig


class MPLogWriter:
    def __init__(self, config: LoggerConfig):
        self.config: LoggerConfig = config

        # 使用 multiprocessing.Queue 来传递日志消息
        mgr = mp.Manager()
        self.log_queue = mgr.Queue()
        self.stop_event = mgr.Event()
        # 在主进程中创建日志写入进程
        self.worker_process = None

    def enqueue(self, log_message: str):
        """将日志消息放入队列"""
        self.log_queue.put(log_message)

    def start(self):
        """启动日志写入进程"""
        self.worker_process = mp.Process(target=self._process_queue, daemon=True)
        self.worker_process.start()

    def _process_queue(self):
        """在独立的进程中处理队列中的日志并写入文件"""
        current_date = date.today()
        rotation_index = 0
        current_log_file = self._get_log_filename(current_date, rotation_index)
        log_file = open(current_log_file, "a", encoding="utf-8")

        while True:
            log_message = self.log_queue.get()

            if date.today() != current_date:
                log_file.close()
                current_date = date.today()
                rotation_index = 0
                current_log_file = self._get_log_filename(current_date, rotation_index)
                log_file = open(current_log_file, "a", encoding="utf-8")
                self._cleanup_old_logs()
            else:
                if log_file.tell() >= self.config.rotation_size * 1024 * 1024:
                    log_file.close()
                    rotation_index += 1
                    current_log_file = self._get_log_filename(current_date, rotation_index)
                    log_file = open(current_log_file, "a", encoding="utf-8")
            log_file.write(log_message + "\n")
            log_file.flush()
            if self.config.to_console:
                print(eval(log_message)["timestamp"], eval(log_message)["message"])

            if self.stop_event.is_set() and self.log_queue.empty():
                break

        log_file.close()

    def _get_log_filename(self, log_date: date, rotation_index: int = 0) -> str:
        base_name = f"{log_date.strftime('%Y-%m-%d')}"
        if rotation_index > 0:
            filename = f"{base_name}_{rotation_index}.log"
        else:
            filename = f"{base_name}.log"
        return os.path.join(str(self.config.log_path), filename)

    def _cleanup_old_logs(self):
        threshold: datetime = datetime.now() - timedelta(days=self.config.max_days)
        for filename in os.listdir(self.config.log_path):
            if filename.endswith(".log"):
                file_path = os.path.join(str(self.config.log_path), filename)
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if mtime < threshold:
                        os.remove(file_path)
                except Exception as e:
                    print(f"删除旧日志文件 {file_path} 时出错：{e}")

    def join(self):
        """等待日志写入进程结束"""
        self.worker_process.join()
