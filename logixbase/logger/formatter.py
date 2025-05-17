import json


def format_log(log_entry: dict, format_type: str = "json") -> str:
    if format_type.upper() == "JSON":
        return json.dumps(log_entry, ensure_ascii=False)
    else:
        # 文本格式示例：[timestamp] [level] [log_id] [thread] [process] message
        return "[{timestamp}] [{level}] [{log_id}] [{thread}] [{process}] {message}".format(**log_entry)
