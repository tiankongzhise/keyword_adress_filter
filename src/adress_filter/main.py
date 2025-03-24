import httpx
import asyncio
import json
import os
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt
import datetime
import pandas as pd

load_dotenv()

TOKEN = os.getenv('TOKEN')
ASSISTANT_ID = os.getenv('ASSISTANT_ID')
USER_ID = os.getenv('USER_ID')


@retry(stop=stop_after_attempt(3))
async def get_address_filter(keyword:str,semaphore):
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
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url,headers=headers,json=data)
                response.raise_for_status()
                return {"关键词":keyword,"result":response.json()['choices'][0]['message']['content']}
        except (httpx.HTTPError, httpx.TimeoutException) as e:
                print(f"请求失败: {url}, 错误: {str(e)}")
                return None

async def main(keyword_list):
    # 创建一个信号量，限制并发数量为 10
    semaphore = asyncio.Semaphore(10)
    
    # 创建任务序列
    tasks = [get_address_filter(keyword,semaphore) for keyword in keyword_list]
    
    result = await asyncio.gather(*tasks)
    
    result = [r for r in result if r is not None]

    # 确保./temp/和./data/目录存在，不存在则创建
    os.makedirs("./temp", exist_ok=True)
    os.makedirs("./data", exist_ok=True)

    # 将result写入./temp/temp_时间.txt
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    temp_file = f"./temp/temp_{timestamp}.txt"
    with open(temp_file, 'w', encoding='utf-8') as f:
        for item in result:
            f.write(f"{item}\n")

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
    keyword_list = ['朝阳区运维培训']
    result = asyncio.run(main(keyword_list))
    print(result)
