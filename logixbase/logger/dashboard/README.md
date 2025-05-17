# 本项目为基于 基于 FastAPI + React (Vite) 构建的日志查看系统，用于快速浏览、筛选和分析本地日志数据，支持多项目、多日期、多级别的高效查询。


# 项目结构
dashboard  
├── backend/             # 后端 FastAPI 项目  
│   ├── main.py          # FastAPI 应用入口  
│   ├── config.py        # 默认配置 + CLI 覆盖  
│   ├── log_reader.py    # 日志读取与筛选工具  
│   ├── cache_manager.py # 缓存轮询机制  
├── frontend/            # 前端 Vite + React + Tailwind 项目  
│   ├── index.html  
│   ├── vite.config.ts  
│   ├── tailwind.config.js  
│   ├── postcss.config.js  
│   └── src/  
│       ├── main.tsx  
│       ├── pages/  
│       │   └── DashboardPage.tsx  
│       ├── components/（包含 ProjectSelector、LogTable 等）  
│       └── index.css  

# 快速启动（生产模式）
## 1. 安装依赖（只需 Python，无需 Node.js）
确保你已安装 Python 3.9+，然后：  
pip install -r requirements.txt
## 2. 构建前端页面（如已有 front/dist 可跳过）
此步骤只需一次构建，部署时使用静态文件即可，无需 Node 环境运行  
cd front  
npm install  
npm run build  
成功后将生成 front/dist/ 目录，包含 index.html、.js、.css 等文件。
## 3. 修改配置（可选）
在 backend/config.py 中可调整日志目录：
DEFAULT_CONFIG = {
    "LOG_ROOT": "D:/logs",  # ✅ 日志根目录
    "PORT": 8000,
    ...
}  
## 4. 前端端口修改：
### 开发环境下，修改 vite.config.ts 中的 host/port


## 4. 启动服务
python main.py  
或直接运行 runprod.bat  
打开浏览器访问：http://localhost:8000


# 支持功能

✅ 多项目切换: 自动识别日志目录结构 D:/logs/项目名/*.log  
✅ 日期区间筛选: 起始日期选择（自动识别日期型文件名）  
✅ 等级筛选: 支持 INFO / ERROR / DEBUG / WARNING  
✅ 内容关键字搜索: 日志内容全文模糊匹配  
✅ 分页浏览: 支持分页大小选择、排序切换  
✅ 缓存预加载: 服务启动时定时轮询最新日志  


# 开发模式:（前后端分离）
如需开发调试，使用：

## 一键启动开发模式
### rundev.bat

## 后端开发模式
### python backend/main.py

## 前端开发模式（vite）
### cd front
### npm run dev

# 依赖说明
Python 依赖（requirements.txt）
fastapi  
uvicorn  
aiofiles  
Node 依赖（仅开发 / 构建用）  
react  
vite  
tailwindcss  
lucide-react  
react-router-dom  

# .gitignore 建议
## Python
__pycache__/
*.py[cod]
## Node
front/node_modules/
front/dist/
## IDE
.vscode/
.idea/

# LICENSE
### MIT License / 私有项目可删改。