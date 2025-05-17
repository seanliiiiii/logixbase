import datetime
import os


def get_current_time() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_today_log_file(log_dir: str) -> str:
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    return os.path.join(log_dir, f"log_{today}.log")
