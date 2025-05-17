# backend/models.py
from pydantic import BaseModel
from typing import List, Optional


class LogQueryRequest(BaseModel):
    project: str
    dates: Optional[List[str]] = None   # ✅ 改成 Optional
    level: Optional[List[str]] = None
    page: int = 1
    page_size: int = 50
    sort_by: Optional[str] = "timestamp"
    order: Optional[str] = "desc"
    keyword: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None