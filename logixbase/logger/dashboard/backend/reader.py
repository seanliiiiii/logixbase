# backend/reader.py
import os
import json
from typing import List, Optional
from datetime import datetime


def list_projects(log_root: str) -> List[str]:
    """获取 /logs 目录下的所有项目子目录名"""
    return [
        name for name in os.listdir(log_root)
        if os.path.isdir(os.path.join(log_root, name))
    ]


def list_log_dates(project_path: str) -> List[str]:
    """返回项目目录下所有日志文件的日期（从文件名中提取）"""
    return [
        f.replace('.log', '')
        for f in os.listdir(project_path)
        if f.endswith('.log')
    ]


def load_logs(
    project_path: str,
    dates: Optional[List[str]] = None,
    levels: Optional[List[str]] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    keyword: Optional[str] = None
) -> List[dict]:
    """按日期+筛选条件读取日志内容"""
    if not start_time or not end_time:
        return []

    st = datetime.strptime(start_time, "%Y-%m-%d")
    ed = datetime.strptime(end_time, "%Y-%m-%d")

    logs = []
    files = os.listdir(project_path)

    for f in files:
        if not f.endswith(".log"):
            continue
        date = f.split(".")[0].split("_")[0]
        if dates and date not in dates:
            continue
        elif date:
            try:
                date = datetime.strptime(date, "%Y-%m-%d")
                if date < st or date > ed:
                    continue
            except Exception as e:
                print(f"[load_logs] 日期检查失败：{e}")
                continue

        filepath = os.path.join(project_path, f)
        try:
            with open(filepath, "r", encoding="utf-8") as file:
                for line in file:
                    try:
                        log = json.loads(line.strip())
                        if levels and log.get("level") not in levels:
                            continue
                        if keyword and keyword not in log.get("message", ""):
                            continue
                        logs.append(log)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"[load_logs] 读取失败: {filepath}, 错误: {e}")
    return logs


def paginate_logs(logs: List[dict], page: int, page_size: int) -> List[dict]:
    """分页逻辑"""
    start = (page - 1) * page_size
    return logs[start:start + page_size]
