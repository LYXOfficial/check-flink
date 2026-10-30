import os
import re
import time
import warnings
import requests
import concurrent.futures
from urllib.parse import urlparse
from datetime import datetime
from queue import Queue

warnings.filterwarnings("ignore", message="Unverified HTTPS request is being made.*")

# 环境变量：
#   BACKEND_URL       新 Go 后端，默认 https://neoend.yaria.top
#   BLOG_SECRET       后端内部令牌（也兼容 SECRET）
#   BLOG_URL          博客站，默认 https://blog.yaria.top
BACKEND_URL = os.getenv("BACKEND_URL", "https://neoend.yaria.top").rstrip("/")
BLOG_URL = os.getenv("BLOG_URL", "https://blog.yaria.top").rstrip("/")
internal_token = os.getenv("BLOG_SECRET") or os.getenv("SECRET")

# 海外/大陆都查不到时，是否把友链标成不可达。
# 默认 False：保留旧延迟，避免国内站风控海外 IP 导致误标红。
MARK_UNVERIFIED_DOWN = True

if not internal_token:
    raise SystemExit("缺少 BLOG_SECRET / SECRET")

user_agent = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/58.0.3029.110 Safari/537.36"
)

# 大陆公益 ping 接口（桂ICP，免费、无 key）
SKYCLOUDS_PING = "https://api.skyclouds.cc/api/webping"

# 大陆 ping 失败时的 HTTP 兜底：check-host 亚洲节点，免费、无 key
CHECK_HOST = "https://check-host.net"
CHECK_HOST_NODES = [
    "hk1.node.check-host.net",
    "jp1.node.check-host.net",
    "sg1.node.check-host.net",
]

fallback_queue = Queue()
fallback_results = []


def check_via_skyclouds(url: str) -> float:
    """大陆节点 ping，返回秒；失败/超时返回 -1。"""
    host = urlparse(url).hostname or url
    try:
        resp = requests.get(
            SKYCLOUDS_PING,
            params={"url": host},
            timeout=25,
        )
        data = resp.json()
        if str(data.get("code")) != "200":
            return -1
        average = str((data.get("data") or {}).get("average", ""))
        match = re.search(r"[\d.]+", average)
        if not match:
            return -1
        return round(float(match.group()) / 1000, 2)
    except Exception as exc:
        print(f"❌ 大陆 ping 失败 {url}: {exc}")
        return -1


def check_via_checkhost(url: str) -> float:
    """HK/JP/SG 节点 HTTP 请求，返回最快成功的秒数；失败返回 -1。"""
    try:
        params = [("host", url)] + [("node", node) for node in CHECK_HOST_NODES]
        first = requests.get(
            f"{CHECK_HOST}/check-http",
            params=params,
            headers={"Accept": "application/json"},
            timeout=20,
        ).json()

        request_id = first.get("request_id")
        if first.get("ok") != 1 or not request_id:
            return -1

        for _ in range(15):
            time.sleep(1)
            result = requests.get(
                f"{CHECK_HOST}/check-result/{request_id}",
                headers={"Accept": "application/json"},
                timeout=20,
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
                    # 行格式：[success, time, status, code, ip]
                    success = row[0]
                    elapsed = row[1] if len(row) > 1 else None
                    if success == 1 and isinstance(elapsed, (int, float)) and elapsed > 0:
                        best = elapsed if best is None else min(best, elapsed)

            if best is not None:
                return round(best, 2)
            if not pending and _ >= 2:
                return -1
        return -1
    except Exception as exc:
        print(f"❌ check-host 查询失败 {url}: {exc}")
        return -1


def check_link_accessibility(item):
    headers = {"User-Agent": user_agent}
    link = item["url"]
    link_id = item["id"]

    # 1. GitHub Actions 直接访问（海外 IP）
    try:
        start_time = time.time()
        response = requests.get(link, headers=headers, timeout=15, verify=True)
        latency = round(time.time() - start_time, 2)
        if response.status_code == 200:
            print(f"✅ 直接访问成功 {link}, 延迟: {latency}s")
            return {"id": link_id, "latency": latency}
    except requests.RequestException:
        print(f"❌ 直接访问失败 {link}")

    # 2/3. 交给大陆 ping + 亚洲 HTTP 兜底
    fallback_queue.put({"id": link_id, "url": link})
    return {"id": link_id, "latency": -1}


def handle_fallback_checks():
    while not fallback_queue.empty():
        item = fallback_queue.get()
        link_id = item["id"]
        url = item["url"]

        # 先用大陆节点，解决「只封海外、不封国内」的站
        latency = check_via_skyclouds(url)
        source = "大陆 ping"
        if latency == -1:
            # 大陆 ping 失败时再用亚洲 HTTP 节点确认真实可访问性
            latency = check_via_checkhost(url)
            source = "亚洲 HTTP"

        if latency != -1:
            print(f"✅ {source} 成功 {url}, 延迟: {latency}s")
        else:
            print(f"❌ 大陆/亚洲节点都无法确认 {url}")

        fallback_results.append({"id": link_id, "latency": latency})

        # 公益接口，控制频率
        time.sleep(0.8)


# 获取友链
response = requests.get(f"{BACKEND_URL}/links/list", timeout=30)
if response.status_code != 200:
    raise SystemExit(f"❌ 获取友链失败，状态码: {response.status_code}")

data = response.json()
link_list = []
for group in data["data"]:
    link_list += group["links"]

print(f"🔗 待检查友链: {len(link_list)}")

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    preliminary_results = list(executor.map(check_link_accessibility, link_list))

handle_fallback_checks()

# 同一个 id 以最终结果为准
merged = {}
for result in preliminary_results:
    merged[result["id"]] = result
for result in fallback_results:
    merged[result["id"]] = result
link_status = list(merged.values())

accessible_count = sum(1 for r in link_status if r["latency"] != -1)
unverified_count = sum(1 for r in link_status if r["latency"] == -1)
total_count = len(link_status)

print(
    f"📦 检查完成，成功：{accessible_count}，"
    f"未验证/不可达：{unverified_count}，总数：{total_count}"
)

# 关键：默认不把「查不到」当成「挂了」
reported = [
    r for r in link_status
    if r["latency"] != -1 or MARK_UNVERIFIED_DOWN
]

if not reported:
    print("⚠️ 没有任何可确认的结果，跳过上报")
    raise SystemExit(0)

print(f"📤 实际上报: {len(reported)} 条")

push_data = {"items": reported}
response = requests.post(
    f"{BACKEND_URL}/links/latency",
    json=push_data,
    headers={"X-Internal-Token": internal_token},
    timeout=30,
)

content_type = response.headers.get("content-type", "")
body = response.json() if content_type.startswith("application/json") else {}

if response.status_code == 200 and body.get("ok"):
    print(f"✅ 推送成功，更新 {body.get('data', {}).get('updated', 0)} 条，刷新缓存中…")
    try:
        requests.get(f"{BLOG_URL}/refreshCache/flinks", timeout=15)
    except requests.RequestException:
        print("⚠️ 博客缓存刷新失败（内容 TTL 会在 10 分钟内自愈）")
else:
    raise SystemExit(f"❌ 推送失败: {response.status_code} {response.text[:500]}")
