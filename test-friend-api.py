import os
import time
import json
import warnings
import requests
import concurrent.futures
from datetime import datetime
from queue import Queue

# 忽略 HTTPS 安全警告
warnings.filterwarnings("ignore", message="Unverified HTTPS request is being made.*")

# 环境变量：
#   BACKEND_URL  新 Go 后端，默认 https://neoend.yaria.top
#   BLOG_SECRET  后端内部令牌（也可用 SECRET）
#   LIJIANGAPI_TOKEN  76.al 的 key
BACKEND_URL = os.getenv("BACKEND_URL", "https://neoend.yaria.top").rstrip("/")
BLOG_URL = os.getenv("BLOG_URL", "https://blog.yaria.top").rstrip("/")
internal_token = os.getenv("BLOG_SECRET") or os.getenv("SECRET")
api_key = os.getenv("LIJIANGAPI_TOKEN")

if not internal_token:
    raise SystemExit("缺少 BLOG_SECRET / SECRET")
if not api_key:
    raise SystemExit("缺少 LIJIANGAPI_TOKEN")

user_agent = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/58.0.3029.110 Safari/537.36"
)
api_url_template = "https://api.76.al/api/web/query?key={}&url={}"
proxy_url_template = "https://lius.me/{}"

api_request_queue = Queue()
api_results = []


def check_link_accessibility(item):
    headers = {"User-Agent": user_agent}
    link = item["url"]
    link_id = item["id"]

    # 1. 直接访问
    try:
        start_time = time.time()
        response = requests.get(link, headers=headers, timeout=15, verify=True)
        latency = round(time.time() - start_time, 2)
        if response.status_code == 200:
            print(f"✅ 直接访问成功 {link}, 延迟: {latency}s")
            return {"id": link_id, "latency": latency}
    except requests.RequestException:
        print(f"❌ 直接访问失败 {link}")

    # 2. 代理访问
    try:
        proxy_url = proxy_url_template.format(link)
        start_time = time.time()
        response = requests.get(proxy_url, headers=headers, timeout=15, verify=True)
        latency = round(time.time() - start_time, 2)
        if response.status_code == 200:
            print(f"✅ 代理访问成功 {link}, 延迟: {latency}s")
            return {"id": link_id, "latency": latency}
    except requests.RequestException:
        print(f"❌ 代理访问失败 {link}")

    # 3. 交给 76.al API 兜底
    api_request_queue.put({"id": link_id, "url": link})
    return {"id": link_id, "latency": -1}


def handle_api_requests():
    while not api_request_queue.empty():
        item = api_request_queue.get()
        link_id = item["id"]
        url = item["url"]
        api_url = api_url_template.format(api_key, url)

        try:
            response = requests.get(
                api_url,
                headers={"User-Agent": user_agent},
                timeout=15,
                verify=True,
            )
            response_data = response.json()
            if response_data.get("code") == 200:
                latency = round(response_data["exec_time"], 2)
                print(f"✅ API 成功访问 {url}, 延迟: {latency}s")
                api_results.append({"id": link_id, "latency": latency})
            else:
                print(f"❌ API 错误访问 {url}, code: {response_data.get('code')}")
                api_results.append({"id": link_id, "latency": -1})
        except requests.RequestException:
            print(f"❌ API 请求失败 {url}")
            api_results.append({"id": link_id, "latency": -1})

        time.sleep(0.2)  # 控制 API 速率（最多每秒 5 次）


# 获取友链（新接口）
response = requests.get(f"{BACKEND_URL}/links/list", timeout=30)
if response.status_code != 200:
    raise SystemExit(f"❌ 获取友链失败，状态码: {response.status_code}")

data = response.json()
link_list = []
for group in data["data"]:
    link_list += group["links"]

print(f"🔗 待检查友链: {len(link_list)}")

# 多线程检测
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    preliminary_results = list(executor.map(check_link_accessibility, link_list))

# API 兜底
handle_api_requests()

# 同一个 id 以最终结果为准：API 兜底成功时覆盖前面的 -1
merged = {}
for result in preliminary_results:
    merged[result["id"]] = result
for result in api_results:
    merged[result["id"]] = result
link_status = list(merged.values())

accessible_count = sum(1 for r in link_status if r["latency"] != -1)
inaccessible_count = sum(1 for r in link_status if r["latency"] == -1)
total_count = len(link_status)
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(
    f"📦 检查完成，访问成功：{accessible_count}，"
    f"失败：{inaccessible_count}，总数：{total_count}"
)

# 上报新接口：X-Internal-Token + {"items": [...]}
push_data = {"items": link_status}
response = requests.post(
    f"{BACKEND_URL}/links/latency",
    json=push_data,
    headers={"X-Internal-Token": internal_token},
    timeout=30,
)

body = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
if response.status_code == 200 and body.get("ok"):
    print(f"✅ 推送成功，更新 {body.get('data', {}).get('updated', 0)} 条，刷新缓存中…")
    try:
        requests.get(f"{BLOG_URL}/refreshCache/flinks", timeout=15)
    except requests.RequestException:
        print("⚠️ 博客缓存刷新失败（内容 TTL 会在 10 分钟内自愈）")
else:
    raise SystemExit(f"❌ 推送失败: {response.status_code} {response.text[:500]}")
