# 后端启动（Windows PowerShell 示例）

## 1. 安装依赖
```powershell
cd C:\Users\IT01\Doubao\chats\2026-10-04\new-chat\backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 2. 启动服务
```powershell
uvicorn app:app --host 0.0.0.0 --port 8000
```

- 本机测试：`http://127.0.0.1:8000/api/news`
- 局域网（鸿蒙真机联调用）：把 `0.0.0.0` 改成 `0.0.0.0` 即可绑定所有网卡，
  手机与电脑同一 Wi-Fi 后访问 `http://<电脑局域网IP>:8000/api/news`

## 3. 接口
- `GET /api/health`    健康检查
- `GET /api/news`      AI 实时新闻（Hacker News + 科技媒体 RSS）
- `GET /api/trending`  GitHub 热门仓库（近7天创建，按星标排序）

返回统一结构：`{"code":0,"message":"ok","data":[...]}`

## 4. 可选配置（环境变量）
- `GITHUB_TOKEN`：GitHub Personal Access Token，可显著提高搜索 API 限额（默认免 token 也可用，10次/分钟）
- `CACHE_TTL`：缓存秒数，默认 600（10分钟）
- `GITHUB_PER_PAGE`：热门仓库数量，默认 30
- `NEWS_LIMIT`：新闻条数上限，默认 40
