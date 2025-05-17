"""
仅用于测试，不应在生产环境中使用
"""
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .config import DEFAULT_CONFIG, parse_args, merge_config
from .cache import LogCacheManager
from .reader import list_projects, list_log_dates, load_logs, paginate_logs
from .models import LogQueryRequest

# 默认配置（可被 CLI 覆盖）
config = DEFAULT_CONFIG.copy()
cache = LogCacheManager(
    log_root=config["LOG_ROOT"],
    line_count=config["CACHE_LOG_LINE_COUNT"]
)
cache.start_background_polling(config["POLLING_INTERVAL_SECONDS"])

# FastAPI 应用初始化
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 项目列表
@app.get("/api/projects")
def get_projects():
    return list_projects(config["LOG_ROOT"])

# 日期列表
@app.get("/api/projects/{project}/dates")
def get_dates(project: str):
    proj_path = os.path.join(config["LOG_ROOT"], project)
    return list_log_dates(proj_path)

# 日志查询接口（分页 + 多条件）
@app.post("/api/logs/query")
def query_logs(req: LogQueryRequest):
    project_path = os.path.join(config["LOG_ROOT"], req.project)
    logs = load_logs(
        project_path=project_path,
        dates=req.dates,
        levels=req.level,
        start_time=req.start_time,
        end_time=req.end_time,
        keyword=req.keyword
    )
    reverse = (req.order != "asc")
    logs.sort(key=lambda x: x.get(req.sort_by, ""), reverse=reverse)
    total = len(logs)
    paged = paginate_logs(logs, req.page, req.page_size)
    return {
        "total": total,
        "page": req.page,
        "page_size": req.page_size,
        "logs": paged
    }

# 缓存接口：获取最新日志
@app.get("/api/logs/cache/{project}")
def get_cached_logs(project: str):
    return cache.get_project_cache(project)


# CLI 模式运行支持
if __name__ == "__main__":
    import uvicorn
    args = parse_args()
    config = merge_config(args)

    # 重建缓存配置
    cache = LogCacheManager(
        log_root=config["LOG_ROOT"],
        line_count=config["CACHE_LOG_LINE_COUNT"]
    )
    cache.start_background_polling(config["POLLING_INTERVAL_SECONDS"])

    uvicorn.run("backend.main:app", host=config["HOST"], port=config["PORT"], reload=True)
