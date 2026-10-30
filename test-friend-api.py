import os
import re
import sys
import time
import threading
import warnings
import requests
import concurrent.futures
from urllib.parse import urlparse
from datetime import datetime

warnings.filterwarnings("ignore", message="Unverified HTTPS request is being made.*")

# 让 GitHub Actions 里也能实时看到输出
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# 环境变量：
#   BACKEND_URL       新 Go 后端，默认 https://neoend.yaria.top
#   BLOG_SECRET       后端内部令牌（也兼容 SECRET）
#   BLOG_URL          博客站，默认 https://blog.yaria.top
BACKEND_URL = os.getenv("BACKEND_URL", "https://neoend.yaria.top").rstrip("/")
BLOG_URL = os.getenv("BLOG_URL", "https://blog.yaria.top").rstrip("/")
internal_token = os.getenv("BLOG_SECRET") or os.getenv("SECRET")

if not internal_token:
    raise SystemExit("缺少 BLOG_SECRET / SECRET")

# 直连/大陆 ping 都失败时，是否再用 check-host 亚洲 HTTP 节点。
# 打开会慢不少，但能确认“大陆 ping 被禁、HTTP 正常”的站。
USE_CHECKHOST_FALLBACK = False

MAX_WORKERS = 10
DIRECT_TIMEOUT = 8
SKYCLOUDS_TIMEOUT = 8

user_agent = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/58.0.3029.110 Safari/537.36"
)

SKYCLOUDS_PING = "https://api.skyclouds.cc/api/webping"
CHECK_HOST = "https://check-host.net"
CHECK_HOST_NODES = [
    "hk1.node.check-host.net",
    "jp1.node.check-host.net",
    "sg1.node.check-host.net",
]

# 天空云是公益接口，限制同时并发，避免被限流
skyclouds_sem = threading.Semaphore(3)


def check_via_skyclouds(url: str) -> float:
    """大陆节点 ping，返回秒；失败/超时返回 -1。"""
    host = urlparse(url).hostname or url
    try:
        with skyclouds_sem:
            data = requests.get(
                SKYCLOUDS_PING,
                params={"url": host},
                timeout=SKYCLOUDS_TIMEOUT,
            ).json()
            time.sleep(0.1)
        if str(data.get("code")) != "200":
            return -1
        average = str((data.get("data") or {}).get("average", ""))
        match = re.search(r"[\d.]+", average)
        if not match:
            return -1
        return round(float(match.group()) / 1000, 2)
    except Exception as exc:
        print(f"  ❌ 大陆 ping 异常 {url}: {exc}", flush=True)
        return -1


def check_via_checkhost(url: str) -> float:
    """HK/JP/SG 节点 HTTP 请求，返回最快成功的秒数；失败返回 -1。"""
    try:
        params = [("host", url)] + [("node", node) for node in CHECK_HOST_NODES]
        first = requests.get(
            f"{CHECK_HOST}/check-http",
            params=params,
            headers={"Accept": "application/json"},
            timeout=8,
        ).json()

        request_id = first.get("request_id")
        if first.get("ok") != 1 or not request_id:
            return -1

        for _ in range(8):
            time.sleep(0.7)
            result = requests.get(
                f"{CHECK_HOST}/check-result/{request_id}",
                headers={"Accept": "application/json"},
                timeout=8,
            ).json()

            best = None
            pending = False
            for rows in result.values():
                if not rows:
                    pending = True
                    continue
                for row in rows:
                    if not row:
                        pending = True
                        continue
                    success = row[0]
                    elapsed = row[1] if len(row) > 1 else None
                    if success == 1 and isinstance(elapsed, (int, float)) and elapsed > 0:
                        best = elapsed if best is None else min(best, elapsed)

            if best is not None:
                return round(best, 2)
            if not pending and _ >= 1:
                return -1
        return -1
    except Exception as exc:
        print(f"  ❌ check-host 异常 {url}: {exc}", flush=True)
        return -1


def check_link(item):
    link = item["url"]
    link_id = item["id"]

    # 1. GitHub Actions 直接访问（海外）
    try:
        start_time = time.time()
        response = requests.get(
            link,
            headers={"User-Agent": user_agent},
            timeout=DIRECT_TIMEOUT,
            verify=True,
        )
        latency = round(time.time() - start_time, 2)
        if response.status_code == 200:
            print(f"✅ 直接访问 {link} {latency}s", flush=True)
            return {"id": link_id, "latency": latency}
    except requests.RequestException:
        pass

    # 2. 大陆 ping
    latency = check_via_skyclouds(link)
    if latency != -1:
        print(f"✅ 大陆 ping {link} {latency}s", flush=True)
        return {"id": link_id, "latency": latency}

    # 3. 可选：亚洲 HTTP 节点
    if USE_CHECKHOST_FALLBACK:
        latency = check_via_checkhost(link)
        if latency != -1:
            print(f"✅ 亚洲 HTTP {link} {latency}s", flush=True)
            return {"id": link_id, "latency": latency}

    print(f"❌ 无法确认 {link}", flush=True)
    return {"id": link_id, "latency": -1}


# 获取友链
response = requests.get(f"{BACKEND_URL}/links/list", timeout=30)
if response.status_code != 200:
    raise SystemExit(f"❌ 获取友链失败，状态码: {response.status_code}")

data = response.json()
link_list = []
for group in data["data"]:
    link_list += group["links"]

print(f"🔗 待检查友链: {len(link_list)}，并发 {MAX_WORKERS}", flush=True)

# 直连和大陆兜底在同一个线程池里完成，谁先完成谁先输出
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    link_status = list(executor.map(check_link, link_list))

accessible_count = sum(1 for r in link_status if r["latency"] != -1)
failed_count = sum(1 for r in link_status if r["latency"] == -1)
total_count = len(link_status)

print(
    f"📦 检查完成，成功：{accessible_count}，"
    f"失败/无法确认：{failed_count}，总数：{total_count}",
    flush=True,
)

if not link_status:
    print("⚠️ 没有友链可上报，跳过", flush=True)
    raise SystemExit(0)

# 查不到就按 -1 标红
push_data = {"items": link_status}
response = requests.post(
    f"{BACKEND_URL}/links/latency",
    json=push_data,
    headers={"X-Internal-Token": internal_token},
    timeout=30,
)

content_type = response.headers.get("content-type", "")
body = response.json() if content_type.startswith("application/json") else {}

if response.status_code == 200 and body.get("ok"):
    print(
        f"✅ 推送成功，更新 {body.get('data', {}).get('updated', 0)} 条，刷新缓存中…",
        flush=True,
    )
    try:
        requests.get(f"{BLOG_URL}/refreshCache/flinks", timeout=15)
    except requests.RequestException:
        print("⚠️ 博客缓存刷新失败（内容 TTL 会在 10 分钟内自愈）", flush=True)
else:
    raise SystemExit(f"❌ 推送失败: {response.status_code} {response.text[:500]}")
