import os
import json
from .utils import get_today_log_file


def search_logs(log_dir: str, log_id: str = None, level: str = None,
                start_time: str = None, end_time: str = None,
                keyword: str = None, output_format: str = "TEXT"):
    results = []
    log_file = get_today_log_file(log_dir)
    if not os.path.exists(log_file):
        return results
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            # 尝试解析 JSON 格式日志
            try:
                log_entry = json.loads(line)
            except json.JSONDecodeError:
                # 若非 JSON 则按文本处理，简单封装为 dict
                log_entry = {"raw": line.strip()}
            if log_id and log_entry.get("log_id") != log_id:
                continue
            if level and log_entry.get("level") != level:
                continue
            if keyword and keyword not in line:
                continue
            # 注：时间过滤逻辑可根据实际时间格式解析后比较
            results.append(log_entry)
    if output_format.upper() == "JSON":
        return json.dumps(results, ensure_ascii=False, indent=2)
    else:
        return "\n".join(str(item) for item in results)
