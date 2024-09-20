import json
import requests
import warnings
import time
import concurrent.futures
from datetime import datetime
from queue import Queue
# from dotenv import load_dotenv
import os

# 忽略警告信息
warnings.filterwarnings("ignore", message="Unverified HTTPS request is being made.*")

# 用户代理字符串，模仿浏览器
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"

# API Key 和 请求URL的模板
# load_dotenv()
api_key = os.getenv("LIJIANGAPI_TOKEN")  # 替换为你的API Key
api_url_template = "https://api.76.al/api/web/query?key={}&url={}"

# 代理链接的模板，代理是通过在代理地址后加目标 URL 来请求
proxy_url_template = "https://lius.me/{}"

# 初始化一个队列来处理API请求
api_request_queue = Queue()

# API 请求处理函数，确保每秒不超过5次请求
def handle_api_requests():
    while not api_request_queue.empty():
        item = api_request_queue.get()
        headers = {"User-Agent": user_agent}
        link = item['url']
        api_url = api_url_template.format(api_key, link)
        # print(f"正在请求API：{api_url}")

        try:
            response = requests.get(api_url, headers=headers, timeout=15, verify=True)
            response_data = response.json()

            # 提取API返回的code和exec_time
            if response_data['code'] == 200:
                latency = round(response_data['exec_time'], 2)
                print(f"成功通过API访问 {link}, 延迟为 {latency} 秒")
                item['latency'] = latency
            else:
                print(f"API返回错误，code: {response_data['code']}，无法访问 {link}")
                item['latency'] = -1
        except requests.RequestException:
            print(f"API请求失败，无法访问 {link}")
            item['latency'] = -1

        time.sleep(0.2)  # 控制API请求速率，确保每秒不超过5次

# 检查链接是否可访问的函数并测量时延
def check_link_accessibility(item):
    headers = {"User-Agent": user_agent}
    link = item['url']
    latency = -1

    # 1. 首先尝试直接访问
    try:
        start_time = time.time()
        response = requests.get(link, headers=headers, timeout=15, verify=True)
        latency = round(time.time() - start_time, 2)
        if response.status_code == 200:
            print(f"成功通过直接访问 {link}, 延迟为 {latency} 秒")
            return [item, latency]
    except requests.RequestException:
        print(f"直接访问失败 {link}")

    # 2. 尝试通过代理访问
    proxy_url = proxy_url_template.format(link)
    try:
        start_time = time.time()
        response = requests.get(proxy_url, headers=headers, timeout=15, verify=True)
        latency = round(time.time() - start_time, 2)
        if response.status_code == 200:
            print(f"成功通过代理访问 {link}, 延迟为 {latency} 秒")
            return [item, latency]
    except requests.RequestException:
        print(f"代理访问失败 {link}")

    # 3. 如果代理也失败，添加到API队列中
    item['latency'] = -1
    api_request_queue.put(item)
    return [item, latency]

# 目标JSON数据的URL
json_url = 'https://blognext-end.yaria.top/get/flink/flinks'

# 发送HTTP GET请求获取JSON数据
response = requests.get(json_url)
if response.status_code == 200:
    data = response.json()  # 解析JSON数据
    link_list = []
    for item in data["data"]:
        link_list += item['links']  # 提取所有的链接项
else:
    print(f"Failed to retrieve data, status code: {response.status_code}")
    exit()

# 使用ThreadPoolExecutor并发检查多个链接
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(check_link_accessibility, link_list))

# 处理API请求
handle_api_requests()

# 添加时延信息到每个链接项
link_status = [{'name': result[0]['name'], 'link': result[0]['url'], 'latency': result[0].get('latency', result[1])} for result in results]

# 获取当前时间
current_time = datetime.no_sw().strftime("%Y-%m-%d %H:%M:%S")

# 统计可访问和不可访问的链接数
accessible_count = len([result for result in results if result[1] != -1])
inaccessible_count = len([result for result in results if result[1] == -1])
total_count = len(results)

backend_url = "https://blognext-end.yaria.top"
blog_secret = os.environ.get("BLOG_SECRET")

print(f"检查完成，推送至 {backend_url}")

# 发送POST请求到后端API
response = requests.post(f"{backend_url}/updata/flink/pushFlinkStatus", json={
    'data': {
        'timestamp': current_time,
        'accessibleCount': accessible_count,
        'inaccessibleCount': inaccessible_count,
        'totalCount': total_count,
        'linkStatus': link_status
    },
    'secret': blog_secret
})

if response.status_code == 200:
    print("推送成功")
else:
    print("推送失败")