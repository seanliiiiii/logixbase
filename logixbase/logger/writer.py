import threading
import queue
import os
import datetime


class LogWriter(threading.Thread):
    def __init__(self, config):
        super().__init__(daemon=True)
        self.config = config
        self.log_path = config.get("log_path", "./logs")
        os.makedirs(self.log_path, exist_ok=True)
        self.log_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.to_console = config.get("to_console", True)
        # 配置轮转和清理参数
        self.rotation_size_mb = config.get("rotation_size_mb", 10)  # 单文件最大大小（MB）
        self.max_days = config.get("max_days", 7)  # 保留日志文件的最大天数

    def enqueue(self, log_message: str):
        self.log_queue.put(log_message)

    def run(self):
        # 初始化当前日期和轮转序号
        current_date = datetime.date.today()
        rotation_index = 0
        current_log_file = self._get_log_filename(current_date, rotation_index)
        log_file = open(current_log_file, "a", encoding="utf-8")

        while True:
            # 从队列中获取日志消息
            log_message = self.log_queue.get()
            # 检查日期是否发生变化（新的一天）
            now_date = datetime.date.today()
            if now_date != current_date:
                # 日期变化，则关闭当前文件，重置轮转索引及日期，打开新文件
                log_file.close()
                current_date = now_date
                rotation_index = 0
                current_log_file = self._get_log_filename(current_date, rotation_index)
                log_file = open(current_log_file, "a", encoding="utf-8")
                # 调用自动清理，删除超过 max_days 的旧日志
                self._cleanup_old_logs()
            else:
                # 当日志文件大小超过配置阈值时，进行轮转
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
        # 停止时关闭文件
        log_file.close()

    def _get_log_filename(self, log_date: datetime.date, rotation_index: int = 0) -> str:
        """
        根据当前日期和轮转索引生成日志文件名：
          - rotation_index == 0 时，文件名格式：log_YYYY-MM-DD.log
          - rotation_index > 0 时，文件名格式：log_YYYY-MM-DD_1.log、log_YYYY-MM-DD_2.log 等。
        """
        base_name = f"{log_date.strftime('%Y-%m-%d')}"
        if rotation_index > 0:
            filename = f"{base_name}_{rotation_index}.log"
        else:
            filename = f"{base_name}.log"
        return os.path.join(self.log_path, filename)

    def _cleanup_old_logs(self):
        """
        遍历日志存储目录，删除文件修改时间早于当前日期减去 max_days 的日志文件。
        """
        threshold = datetime.datetime.now() - datetime.timedelta(days=self.max_days)
        for f in os.listdir(self.log_path):
            if f.endswith(".log"):
                file_path = os.path.join(self.log_path, f)
                try:
                    mtime = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                    if mtime < threshold:
                        os.remove(file_path)
                except Exception as e:
                    print(f"删除旧日志文件 {file_path} 时出错: {e}")
