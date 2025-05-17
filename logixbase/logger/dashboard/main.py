import os
import uvicorn
import mimetypes
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from backend.config import DEFAULT_CONFIG, parse_args, merge_config
from backend.cache import LogCacheManager
from backend.reader import list_projects, list_log_dates, load_logs, paginate_logs
from backend.models import LogQueryRequest
from fastapi.responses import FileResponse


# 强制添加 MIME 类型映射（有时系统可能不包含默认映射）
mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("text/css", ".css")


class PatchedStaticFiles(StaticFiles):
    def is_not_modified(self, response_headers, request_headers):
        # 禁用缓存检测，确保每次请求都经过 get_response
        return False

    async def get_response(self, path: str, scope):
        print("PatchedStaticFiles -> get_response, path =", path)
        response = await super().get_response(path, scope)
        lower_path = path.lower()
        if lower_path.endswith(".js"):
            response.media_type = "application/javascript"
            response.headers["Content-Type"] = "application/javascript"
            print("Set media_type and header for", path, "to application/javascript")
        elif lower_path.endswith(".css"):
            response.media_type = "text/css"
            response.headers["Content-Type"] = "text/css"
        return response


# ========== 应用初始化 ==========
app = FastAPI()

# 如果你需要跨域访问：
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== 配置加载 ==========
# 注意：如果你想 CLI 传参生效，需要在 __main__ 里 merge_config
# 这里先加载默认的
config = DEFAULT_CONFIG.copy()

# ========== 缓存轮询器启动 ==========
cache = LogCacheManager(
    log_root=config["LOG_ROOT"],
    line_count=config["CACHE_LOG_LINE_COUNT"]
)
cache.start_background_polling(config["POLLING_INTERVAL_SECONDS"])

# ========== 后端日志接口 ==========

@app.get("/api/projects")
def get_projects():
    """列出所有项目子目录"""
    return list_projects(config["LOG_ROOT"])

@app.get("/api/projects/{project}/dates")
def get_dates(project: str):
    """列出指定项目下的所有日志日期（文件名）"""
    proj_path = os.path.join(config["LOG_ROOT"], project)
    return list_log_dates(proj_path)

@app.post("/api/logs/query")
def query_logs(req: LogQueryRequest):
    """按多条件查询日志（日期/等级/关键词/时间区间/分页等）"""
    project_path = os.path.join(config["LOG_ROOT"], req.project)
    logs = load_logs(
        project_path=project_path,
        dates=req.dates,
        levels=req.level,
        start_time=req.start_time,
        end_time=req.end_time,
        keyword=req.keyword
    )
    # 排序
    reverse = (req.order != "asc")
    logs.sort(key=lambda x: x.get(req.sort_by or "timestamp", ""), reverse=reverse)
    # 分页
    total = len(logs)
    paged = paginate_logs(logs, req.page, req.page_size)
    return {
        "total": total,
        "page": req.page,
        "page_size": req.page_size,
        "logs": paged
    }

@app.get("/api/logs/cache/{project}")
def get_cached_logs(project: str):
    """获取缓存中最新的日志数据"""
    return cache.get_project_cache(project)


# 设定前端构建产物路径
BASE_DIR = os.path.dirname(__file__)
DIST_DIR = os.path.join(BASE_DIR, "front", "dist")
INDEX_FILE = os.path.join(DIST_DIR, "index.html")
ASSETS_DIR = os.path.join(DIST_DIR, "assets")

if not os.path.exists(INDEX_FILE):
    print(f"[ERROR] index.html not found in {DIST_DIR}. Please run 'npm run build' in the front directory.")
else:
    # 挂载 /assets 静态文件，使用自定义的 PatchedStaticFiles 保证 MIME 类型正确
    if os.path.exists(ASSETS_DIR):
        app.mount("/assets", PatchedStaticFiles(directory=ASSETS_DIR), name="assets")
        print(f"[INFO] ✅ Mounted /assets from: {ASSETS_DIR}")
    else:
        print(f"[WARNING] Assets directory not found in {DIST_DIR}.")


    # fallback 路由：对于所有非 /api 和非 /assets 的请求，返回 index.html
    @app.get("/{full_path:path}")
    async def spa_fallback(full_path: str):
        # 不拦截 /api 请求，直接返回 404 由 API 路由处理
        if full_path.startswith("api/") or full_path.startswith("assets/"):
            return {"detail": "Not Found"}
        # 返回 index.html 以支持 React Router（子路由刷新）
        return FileResponse(INDEX_FILE)


    print(f"[INFO] ✅ Fallback configured. index.html at: {INDEX_FILE}")
####################################
# 启动入口
####################################
if __name__ == "__main__":
    # 支持命令行参数覆盖默认配置
    args = parse_args()
    config = merge_config(args)
    # 注意：uvicorn 监听地址应为 0.0.0.0，浏览器访问时使用 localhost 或实际 IP
    uvicorn.run(app, host=config["HOST"], port=config["PORT"])