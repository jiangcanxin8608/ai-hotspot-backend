# -*- coding: utf-8 -*-
"""
AI热点聚合后端 (AI Hotspot Aggregator Backend)

功能：
  - /api/news      聚合 AI 实时新闻（Hacker News + 多家科技媒体 RSS）
  - /api/trending  聚合 GitHub 热门仓库（GitHub Search API，按近7天创建+星标排序）
  - /api/health    健康检查

特性：
  - 内存缓存 + TTL，避免频繁打第三方接口触发限流
  - 支持 GITHUB_TOKEN 环境变量（提高 GitHub API 限额）
  - 纯标准库 + fastapi/httpx/feedparser

启动：uvicorn app:app --host 0.0.0.0 --port 8000
"""
import asyncio
import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List
from urllib.parse import quote

import feedparser
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# --------------------------------------------------------------------------- #
# 配置
# --------------------------------------------------------------------------- #
CACHE_TTL = int(os.getenv("CACHE_TTL", "600"))          # 缓存秒数，默认10分钟
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")            # 可选，用于提升 GitHub 限额
GITHUB_PER_PAGE = int(os.getenv("GITHUB_PER_PAGE", "30"))
NEWS_LIMIT = int(os.getenv("NEWS_LIMIT", "40"))
HTTP_TIMEOUT = 15

# 实时中文翻译
TRANSLATE_ENABLED = os.getenv("TRANSLATE_ENABLED", "1").lower() in ("1", "true", "yes")
TRANSLATE_LIMIT = int(os.getenv("TRANSLATE_LIMIT", "500"))   # 每轮最多翻译条数，防限流
MYMEMORY_API = "https://api.mymemory.translated.net/get"

# 科技媒体 RSS 源
RSS_SOURCES = [
    {"name": "TechCrunch AI",
     "url": "https://techcrunch.com/category/artificial-intelligence/feed/"},
    {"name": "MIT Technology Review",
     "url": "https://www.technologyreview.com/topic/artificial-intelligence/feed"},
    {"name": "The Verge AI",
     "url": "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml"},
    {"name": "VentureBeat AI",
     "url": "https://venturebeat.com/category/ai/feed/"},
]

# Hacker News 检索关键词（Algolia API）
HN_QUERIES = ["AI", "artificial intelligence", "LLM", "GPT", "OpenAI", "machine learning"]

app = FastAPI(title="AI热点聚合后端", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 简单内存缓存: {"news": (timestamp, data), "trending": (timestamp, data)}
_cache = {}


# --------------------------------------------------------------------------- #
# 工具函数
# --------------------------------------------------------------------------- #
def _cache_get(key: str):
    hit = _cache.get(key)
    if not hit:
        return None
    ts, data = hit
    if time.time() - ts > CACHE_TTL:
        _cache.pop(key, None)
        return None
    return data


def _cache_set(key: str, data):
    _cache[key] = (time.time(), data)


def _make_id(*parts) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def _parse_time_iso(value: str) -> str:
    """把各种时间字符串统一转成 ISO8601，解析失败返回空串。"""
    if not value:
        return ""
    try:
        # 处理 email.utils 风格的 RFC822，如 'Wed, 01 Oct 2025 09:00:00 GMT'
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(value).astimezone(timezone.utc).isoformat()
    except Exception:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
        except Exception:
            return value


async def _fetch_text(client: httpx.AsyncClient, url: str) -> str:
    resp = await client.get(url, timeout=HTTP_TIMEOUT, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


# --------------------------------------------------------------------------- #
# 实时中文翻译（MyMemory，免费无 Key；失败回退为原文）
# --------------------------------------------------------------------------- #
_translate_cache: dict = {}


def _looks_chinese(text: str) -> bool:
    return any('\u4e00' <= ch <= '\u9fff' for ch in text)


async def _translate_one(client: httpx.AsyncClient, text: str) -> str:
    """把英文文本翻译成中文；已是中文/翻译失败/关闭翻译时返回原文。"""
    if not text or not TRANSLATE_ENABLED or _looks_chinese(text):
        return text
    key = text.strip().lower()
    hit = _translate_cache.get(key)
    if hit:
        return hit
    url = f"{MYMEMORY_API}?q={quote(text)}&langpair=en%7Czh-CN"
    try:
        resp = await client.get(url, timeout=10)
        payload = resp.json()
        zh = (payload.get("responseData") or {}).get("translatedText") or ""
        if zh and "MYMEMORY WARNING" not in zh.upper() and "QUERY LENGTH" not in zh.upper():
            _translate_cache[key] = zh
            return zh
    except Exception:
        pass
    return text


async def _translate_batch(items: List[dict], fields: tuple) -> None:
    """原地给每条 item 增加 <field>_zh 字段，值为中文（失败为原文）。"""
    if not TRANSLATE_ENABLED or not items:
        return
    sem = asyncio.Semaphore(6)  # 控制并发，避免触发限流

    async def worker(it: dict, field: str):
        async with sem:
            return await _translate_one(client, it.get(field) or "")

    async with httpx.AsyncClient(headers={"User-Agent": "AIHotspotApp/1.0"}) as client:
        jobs = []
        for it in items[:TRANSLATE_LIMIT]:
            for field in fields:
                jobs.append((it, field, worker(it, field)))
        if not jobs:
            return
        results = await asyncio.gather(*(j[2] for j in jobs))
        for (it, field, _), zh in zip(jobs, results):
            it[field + "_zh"] = zh


# --------------------------------------------------------------------------- #
# 新闻抓取
# --------------------------------------------------------------------------- #
async def _fetch_hn(client: httpx.AsyncClient) -> List[dict]:
    items = []
    async def one(q: str):
        url = "https://hn.algolia.com/api/v1/search_by_date?" + \
              f"query={quote(q)}&tags=story&hitsPerPage=12"
        try:
            txt = await _fetch_text(client, url)
            payload = json.loads(txt)
            for hit in payload.get("hits", []):
                title = (hit.get("title") or "").strip()
                url_ = hit.get("url") or f"https://news.ycombinator.com/item?id={hit.get('objectID')}"
                if not title:
                    continue
                created = hit.get("created_at") or ""
                items.append({
                    "id": _make_id("hn", hit.get("objectID")),
                    "title": title,
                    "url": url_,
                    "source": "Hacker News",
                    "summary": (hit.get("story_text") or "")[:300] or "",
                    "published_at": _parse_time_iso(created),
                })
        except Exception:
            pass
    await asyncio.gather(*(one(q) for q in HN_QUERIES))
    return items


async def _fetch_rss(client: httpx.AsyncClient) -> List[dict]:
    items = []

    async def one(src: dict):
        try:
            txt = await _fetch_text(client, src["url"])
            feed = feedparser.parse(txt)
            for entry in feed.entries[:12]:
                title = (entry.get("title") or "").strip()
                if not title:
                    continue
                url_ = entry.get("link") or ""
                summary = (entry.get("summary") or entry.get("description") or "")
                # 去掉 HTML 标签
                import re
                summary = re.sub(r"<[^>]+>", "", summary).strip()[:300]
                items.append({
                    "id": _make_id("rss", src["name"], title),
                    "title": title,
                    "url": url_,
                    "source": src["name"],
                    "summary": summary,
                    "published_at": _parse_time_iso(entry.get("published") or entry.get("updated") or ""),
                })
        except Exception:
            pass
    await asyncio.gather(*(one(s) for s in RSS_SOURCES))
    return items


async def collect_news() -> List[dict]:
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AIHotspotApp/1.0"},
    ) as client:
        hn, rss = await asyncio.gather(_fetch_hn(client), _fetch_rss(client))
    merged = hn + rss
    # 去重（按 url 归一化）
    seen, result = set(), []
    for item in sorted(merged, key=lambda x: x["published_at"], reverse=True):
        key = item["url"].strip().lower()
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        result.append(item)
        if len(result) >= NEWS_LIMIT:
            break
    await _translate_batch(result, ("title", "summary"))
    return result


# --------------------------------------------------------------------------- #
# GitHub 热门仓库
# --------------------------------------------------------------------------- #
async def collect_trending() -> List[dict]:
    since = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    q = f"created:>{since}"
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "AIHotspotApp/1.0"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    url = (f"https://api.github.com/search/repositories"
           f"?q={quote(q)}&sort=stars&order=desc&per_page={GITHUB_PER_PAGE}")
    async with httpx.AsyncClient(headers=headers, timeout=HTTP_TIMEOUT) as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            payload = resp.json()
        except Exception:
            return []

    repos = []
    for repo in payload.get("items", []):
        repos.append({
            "id": _make_id("gh", repo.get("full_name") or repo.get("id")),
            "name": repo.get("name"),
            "full_name": repo.get("full_name"),
            "url": repo.get("html_url"),
            "description": (repo.get("description") or "")[:500],
            "stars": repo.get("stargazers_count", 0),
            "forks": repo.get("forks_count", 0),
            "language": repo.get("language") or "N/A",
            "topics": (repo.get("topics") or [])[:5],
            "owner_avatar": (repo.get("owner") or {}).get("avatar_url", ""),
            "created_at": repo.get("created_at", ""),
        })
    await _translate_batch(repos, ("description",))
    return repos


# --------------------------------------------------------------------------- #
# 路由
# --------------------------------------------------------------------------- #
@app.get("/api/health")
async def health():
    return {"code": 0, "message": "ok", "data": {"service": "ai-hotspot-backend", "time": datetime.now(timezone.utc).isoformat()}}


@app.get("/api/news")
async def news():
    data = _cache_get("news")
    if data is None:
        data = await collect_news()
        _cache_set("news", data)
    return {"code": 0, "message": "ok", "data": data}


@app.get("/api/trending")
async def trending():
    data = _cache_get("trending")
    if data is None:
        data = await collect_trending()
        _cache_set("trending", data)
    return {"code": 0, "message": "ok", "data": data}
