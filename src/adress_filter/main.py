import httpx
import asyncio
import os
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt,wait_exponential
import datetime
import pandas as pd
from read_keyword import get_all_keywords  # 新增导入
from tqdm import tqdm  # 新增导入
import aiofiles

load_dotenv()

TOKEN = os.getenv('TOKEN')
ASSISTANT_ID = os.getenv('ASSISTANT_ID')
USER_ID = os.getenv('USER_ID')


@retry(stop=stop_after_attempt(3),wait=wait_exponential(multiplier=1,max=10))
async def get_address_filter(client,keyword:str,semaphore):
    async with semaphore:
        try:
            # 定义 API 的 URL
            url = 'https://open.hunyuan.tencent.com/openapi/v1/agent/chat/completions'

            # 定义请求头
            headers = {
                'X-Source': 'openapi',
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {TOKEN}'
            }

            # 定义请求体
            data = {
                "assistant_id": ASSISTANT_ID,
                "user_id": USER_ID,
                "stream": False,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": keyword
                            }
                        ]
                    }
                ]
            }

            # 将请求体转换为 JSON 格式的字符串
            # json_data = json.dumps(data)

            # 发送 POST 请求
            # response = requests.post(url, headers=headers, json=data)  # 使用 json 参数自动设置正确的 Content-Type
            response = await client.post(url,headers=headers,json=data)
            response.raise_for_status()
            return {"关键词":keyword,"result":response.json()['choices'][0]['message']['content']}
        except (httpx.HTTPError, httpx.TimeoutException) as e:
                print(f"请求失败: {url}, 错误: {str(e)}")
                return None

async def save_results_async(result, temp_file):
    async with aiofiles.open(temp_file, 'w', encoding='utf-8') as f:
        for item in result:
            await f.write(f"{item}\n")
async def main(keyword_list):
    
    # 确保./temp/和./data/目录存在，不存在则创建
    os.makedirs("./temp", exist_ok=True)
    os.makedirs("./data", exist_ok=True)
    
    # 将result写入./temp/temp_时间.txt
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    temp_file = f"./temp/temp_{timestamp}.txt"
    
    # 全局复用 AsyncClient，并设置连接池参数
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
    async with httpx.AsyncClient(timeout=10.0, limits=limits) as client:  # 复用客户端
        # 创建一个信号量，限制并发数量为 10
        semaphore = asyncio.Semaphore(10)
        
        # 创建任务序列
        tasks = [get_address_filter(client,keyword,semaphore) for keyword in keyword_list]
        
        result = []
        with tqdm(total=len(tasks), desc="处理任务") as pbar:
            BATCH_SIZE = 50
            for i in range(0, len(tasks), BATCH_SIZE):
                batch = tasks[i:i+BATCH_SIZE]
                batch_results = await asyncio.gather(*batch, return_exceptions=True)
                result.extend([r for r in batch_results if r is not None])
                await save_results_async(result, temp_file)
                pbar.update(BATCH_SIZE)

    # 解析result的str元素为字典
    parsed_list = []
    for temp in result:
        item = temp['result']
        is_shenzhen = item.split(",")[0]
        province = item.split(",")[1].split(":")[1].strip()
        region = item.split(",")[2].split(":")[1].strip()
        parsed_list.append({
            "关键词": temp['关键词'],
            "是否深圳地区": is_shenzhen,
            "省份": province,
            "地域": region
        })

    # 将解析后的字典列表存储为xlsx文件
    result_file = f"./data/result_{timestamp}.xlsx"
    df = pd.DataFrame(parsed_list)
    df.to_excel(result_file, index=False)

    return result

if __name__ == '__main__':
    keyword_list = get_all_keywords()  # 使用新模块读取的关键词
    result = asyncio.run(main(keyword_list))
    print(result)
