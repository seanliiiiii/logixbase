import multiprocessing as mp
import os
import datetime
from datetime import date, datetime, timedelta


class MPLogWriter:
    def __init__(self, config):
        self.config = config
        self.log_path = config.get("log_path", "./logs")
        os.makedirs(self.log_path, exist_ok=True)

        # 使用 multiprocessing.Queue 来传递日志消息
        self.log_queue = mp.Manager().Queue()
        self.stop_event = mp.Manager().Event()
        self.to_console = config.get("to_console", True)

        # 从配置中获取轮转及清理参数
        self.rotation_size_mb = config.get("rotation_size_mb", 10)  # 单文件最大大小（MB）
        self.max_days = config.get("max_days", 7)  # 保留日志天数

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
                if log_file.tell() >= self.rotation_size_mb * 1024 * 1024:
                    log_file.close()
                    rotation_index += 1
                    current_log_file = self._get_log_filename(current_date, rotation_index)
                    log_file = open(current_log_file, "a", encoding="utf-8")
            log_file.write(log_message + "\n")
            log_file.flush()
            if self.to_console:
                print(log_message)

            if self.stop_event.is_set() and self.log_queue.empty():
                break

        log_file.close()

    def _get_log_filename(self, log_date: date, rotation_index: int = 0) -> str:
        base_name = f"{log_date.strftime('%Y-%m-%d')}"
        if rotation_index > 0:
            filename = f"{base_name}_{rotation_index}.log"
        else:
            filename = f"{base_name}.log"
        return os.path.join(self.log_path, filename)

    def _cleanup_old_logs(self):
        threshold: datetime = datetime.now() - timedelta(days=self.max_days)
        for filename in os.listdir(self.log_path):
            if filename.endswith(".log"):
                file_path = os.path.join(self.log_path, filename)
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if mtime < threshold:
                        os.remove(file_path)
                except Exception as e:
                    print(f"删除旧日志文件 {file_path} 时出错：{e}")

    def join(self):
        """等待日志写入进程结束"""
        self.worker_process.join()
