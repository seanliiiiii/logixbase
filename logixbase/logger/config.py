import json
import os

DEFAULT_CONFIG = {
    "log_path": "D:/logs",
    "log_level": "DEBUG",
    "log_format": "JSON",  # 或 "TEXT"
    "max_days": 7,
    "to_console": True,
    "rotation_size_mb": 10
}


def load_config(config_file="config.json"):
    config = DEFAULT_CONFIG.copy()
    if os.path.exists(config_file):
        with open(config_file, "r", encoding="utf-8") as f:
            file_config = json.load(f)
            config.update(file_config)
    # 环境变量覆盖示例
    if os.getenv("LOG_PATH"):
        config["log_path"] = os.getenv("LOG_PATH")
    if os.getenv("LOG_LEVEL"):
        config["log_level"] = os.getenv("LOG_LEVEL")
    return config
