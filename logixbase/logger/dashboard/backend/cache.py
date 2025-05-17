# backend/cache.py
import os
import threading
import time
from collections import defaultdict
from typing import List

from .reader import load_logs


class LogCacheManager:
    def __init__(self, log_root: str, line_count: int = 100):
        self.log_root = log_root
        self.line_count = line_count
        self.project_cache: dict[str, List[dict]] = defaultdict(list)
        self.lock = threading.Lock()

    def start_background_polling(self, interval_seconds: int = 60):
        thread = threading.Thread(target=self._poll_loop, args=(interval_seconds,), daemon=True)
        thread.start()

    def _poll_loop(self, interval: int):
        while True:
            try:
                self._reload_latest_logs()
            except Exception as e:
                print(f"[LogCacheManager] 轮询失败: {e}")
            time.sleep(interval)

    def _reload_latest_logs(self):
        with self.lock:
            for project in os.listdir(self.log_root):
                project_path = os.path.join(self.log_root, project)
                if os.path.isdir(project_path):
                    all_logs = load_logs(project_path, dates=None)
                    sorted_logs = sorted(
                        all_logs,
                        key=lambda x: x.get("timestamp", ""),
                        reverse=True
                    )
                    self.project_cache[project] = sorted_logs[:self.line_count]

    def get_project_cache(self, project: str) -> List[dict]:
        with self.lock:
            return self.project_cache.get(project, [])
