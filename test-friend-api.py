import json
import requests
import warnings
import time
import concurrent.futures
from datetime import datetime
from queue import Queue
import os

# 忽略 HTTPS 安全警告
warnings.filterwarnings("ignore", message="Unverified HTTPS request is being made.*")

# 通用请求头
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"

# 变量与模板
api_key = os.getenv("LIJIANGAPI_TOKEN")
blog_secret = os.getenv("BLOG_SECRET")
json_url = 'https://blognext-end.yaria.top/get/flink/flinks'
api_url_template = "https://api.76.al/api/web/query?key={}&url={}"
proxy_url_template = "https://lius.me/{}"
backend_url = "https://blognext-end.yaria.top"

# 队列用于 API 请求
api_request_queue = Queue()
api_results = []  # 用于存储 API 的结果


def check_link_accessibility(item):
    headers = {"User-Agent": user_agent}
    link = item['url']
    id = item['id']
    latency = -1

    # 1. 直接访问
    try:
        start_time = time.time()
        response = requests.get(link, headers=headers, timeout=15, verify=True)
        latency = round(time.time() - start_time, 2)
        if response.status_code == 200:
            print(f"✅ 直接访问成功 {link}, 延迟: {latency}s")
            return {"id": id, "latency": latency}
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
            return {"id": id, "latency": latency}
    except requests.RequestException:
        print(f"❌ 代理访问失败 {link}")

    # 3. 加入 API 请求队列
    api_request_queue.put({"id": id, "url": link})
    return {"id": id, "latency": -1}


def handle_api_requests():
    while not api_request_queue.empty():
        item = api_request_queue.get()
        id = item["id"]
        url = item["url"]
        api_url = api_url_template.format(api_key, url)
        headers = {"User-Agent": user_agent}

        try:
            response = requests.get(api_url, headers=headers, timeout=15, verify=True)
            response_data = response.json()
            if response_data.get("code") == 200:
                latency = round(response_data["exec_time"], 2)
                print(f"✅ API 成功访问 {url}, 延迟: {latency}s")
                api_results.append({"id": id, "latency": latency})
            else:
                print(f"❌ API 错误访问 {url}, code: {response_data.get('code')}")
                api_results.append({"id": id, "latency": -1})
        except requests.RequestException:
            print(f"❌ API 请求失败 {url}")
            api_results.append({"id": id, "latency": -1})

        time.sleep(0.2)  # 控制 API 速率（最多每秒5次）


# 获取链接数据
response = requests.get(json_url)
if response.status_code != 200:
    print(f"❌ 获取链接失败，状态码: {response.status_code}")
    exit(1)

data = response.json()
link_list = []
for item in data["data"]:
    link_list += item["links"]

# 多线程检测可访问性
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    preliminary_results = list(executor.map(check_link_accessibility, link_list))

# API补充处理
handle_api_requests()

# 合并所有结果
link_status = preliminary_results + api_results

# 时间戳与统计信息
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
accessible_count = sum(1 for r in link_status if r["latency"] != -1)
inaccessible_count = sum(1 for r in link_status if r["latency"] == -1)
total_count = len(link_status)

print(f"📦 检查完成，准备推送，访问成功：{accessible_count}，失败：{inaccessible_count}，总数：{total_count}")

# 发送到后端
push_data = {
    'data': {
        'timestamp': current_time,
        'accessibleCount': accessible_count,
        'inaccessibleCount': inaccessible_count,
        'totalCount': total_count,
        'linkStatus': link_status
    },
    'secret': blog_secret
}

response = requests.post(f"{backend_url}/update/flink/pushFlinkStatus", json=push_data)
if response.status_code == 200:
    print("✅ 推送成功，刷新缓存中…")
    requests.get("https://blog.yaria.top/refreshCache/flinks")
else:
    print("❌ 推送失败:", response.status_code, response.text)
    exit(1)
