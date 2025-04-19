import gradio as gr
import requests
import os
import json

# 获取API URL和Ollama URL（从环境变量）
API_URL = os.environ.get("API_URL", "http://backend:8000")
OLLAMA_API = os.environ.get("OLLAMA_HOST", "http://ollama:11434")


def query_flights(query_type, param1="", param2=""):
    """
    查询后端API获取航班信息
    """
    try:
        if query_type == "all_flights":
            response = requests.get(f"{API_URL}/flights")
        elif query_type == "by_airports":
            response = requests.get(f"{API_URL}/flights/airports",
                                    params={"starting": param1, "destination": param2})
        elif query_type == "by_airline":
            response = requests.get(f"{API_URL}/flights/airline",
                                    params={"airline": param1})
        else:
            return "无效的查询类型"

        if response.status_code == 200:
            flights = response.json()
            return format_flights(flights)
        else:
            return f"错误: {response.status_code} - {response.text}"
    except Exception as e:
        return f"连接API时出错: {str(e)}"


def format_flights(flights):
    """格式化航班数据以便显示"""
    if not flights:
        return "未找到航班。"

    result = ""
    for i, flight in enumerate(flights):
        result += f"航班 {i + 1}:\n"
        result += f"  出发机场: {flight.get('startingAirport', 'N/A')}\n"
        result += f"  目的地机场: {flight.get('destinationAirport', 'N/A')}\n"

        # 尝试获取航空公司信息
        airline_info = "N/A"

        # 从segmentDetails获取
        if 'segmentDetails' in flight and flight['segmentDetails']:
            if isinstance(flight['segmentDetails'], list) and len(flight['segmentDetails']) > 0:
                segment = flight['segmentDetails'][0]
                if 'segmentsAirlineName' in segment:
                    airline_info = segment['segmentsAirlineName'].replace('||', ', ')
            elif isinstance(flight['segmentDetails'], dict):
                if 'segmentsAirlineName' in flight['segmentDetails']:
                    airline_info = flight['segmentDetails']['segmentsAirlineName'].replace('||', ', ')

        # 直接从flight对象获取
        elif 'segmentsAirlineName' in flight:
            airline_info = flight['segmentsAirlineName'].replace('||', ', ')

        result += f"  航空公司: {airline_info}\n"
        result += f"  价格: ${flight.get('totalFare', 'N/A')}\n"

        # 获取航班时长
        duration = None
        if 'travelDuration' in flight:
            duration = flight['travelDuration']
        elif 'totalTripDuration' in flight:
            duration = f"{flight['totalTripDuration']} 分钟"

        result += f"  时长: {duration or 'N/A'}\n\n"

    return result


def query_hotels(county="", state=""):
    """
    查询后端API获取酒店信息
    """
    try:
        if county and state:
            response = requests.get(f"{API_URL}/hotels",
                                    params={"county": county, "state": state})
        elif county:
            response = requests.get(f"{API_URL}/hotels/county/{county}")
        elif state:
            response = requests.get(f"{API_URL}/hotels/state/{state}")
        else:
            response = requests.get(f"{API_URL}/hotels")

        if response.status_code == 200:
            hotels = response.json()
            return format_hotels(hotels)
        else:
            return f"错误: {response.status_code} - {response.text}"
    except Exception as e:
        return f"连接API时出错: {str(e)}"


def format_hotels(hotels):
    """格式化酒店数据以便显示"""
    if not hotels:
        return "未找到酒店。"

    result = ""
    for i, hotel in enumerate(hotels):
        result += f"酒店 {i + 1}: {hotel.get('hotel_name', 'N/A')}\n"
        result += f"  县: {hotel.get('county', 'N/A')}\n"
        result += f"  州: {hotel.get('state', 'N/A')}\n"
        result += f"  评分: {hotel.get('rating', 'N/A')}\n"
        result += f"  清洁度: {hotel.get('cleanliness', 'N/A')}\n"
        result += f"  服务: {hotel.get('service', 'N/A')}\n\n"

    return result


def call_ollama(prompt):
    """
    调用Ollama API
    """
    try:
        # 尝试多种可能的API端点格式
        endpoints = [
            "/api/generate",  # 原始尝试
            "/v1/completions",  # 可能的OpenAI兼容端点
            "/v1/chat/completions",  # 可能的OpenAI聊天端点
            ""  # 直接使用基础URL
        ]

        # 构建基本请求数据
        payload = {
            "model": "tinyllama",
            "prompt": prompt,
            "stream": False
        }

        # OpenAI兼容格式
        openai_payload = {
            "model": "tinyllama",
            "messages": [{"role": "user", "content": prompt}],
            "stream": False
        }

        # 尝试各种端点
        for endpoint in endpoints:
            url = f"{OLLAMA_API}{endpoint}"
            print(f"尝试连接到Ollama API: {url}")

            try:
                # 标准Ollama格式
                if "completions" not in endpoint:
                    response = requests.post(url, json=payload, timeout=5)
                # OpenAI兼容格式
                else:
                    response = requests.post(url, json=openai_payload, timeout=5)

                if response.status_code == 200:
                    print(f"成功连接到: {url}")

                    # 解析返回的JSON
                    response_json = response.json()

                    # 处理不同的返回格式
                    if "response" in response_json:
                        # 原生Ollama格式
                        return response_json.get("response")
                    elif "choices" in response_json:
                        # OpenAI格式
                        choices = response_json.get("choices", [])
                        if choices and len(choices) > 0:
                            if "message" in choices[0]:
                                return choices[0]["message"].get("content", "")
                            elif "text" in choices[0]:
                                return choices[0].get("text", "")

                    # 未知格式，但返回了数据
                    return str(response_json)
                else:
                    print(f"尝试 {url} 失败: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"尝试 {url} 出错: {str(e)}")

        # 所有尝试都失败，使用备用处理方法
        print("所有Ollama API端点尝试均失败，将使用备用处理方法")
        return None
    except Exception as e:
        print(f"调用Ollama时发生严重错误: {str(e)}")
        return None


def natural_language_query(query):
    """
    处理自然语言查询
    先尝试简单的模式匹配，如果无法匹配则尝试使用Ollama，
    如果Ollama失败则使用备用的关键词匹配
    """
    # 先检查简单的模式
    query_lower = query.lower()

    # 检查是否是简单的"显示所有航班"查询
    if any(pattern in query_lower for pattern in ["显示所有航班", "所有航班", "查看所有航班",
                                                  "show all flights", "all flights"]):
        print("识别到简单查询：所有航班")
        return query_flights("all_flights")

    # 检查是否是简单的"显示所有酒店"查询
    if any(pattern in query_lower for pattern in ["显示所有酒店", "所有酒店", "查看所有酒店",
                                                  "show all hotels", "all hotels"]):
        print("识别到简单查询：所有酒店")
        return query_hotels()

    # 使用Ollama尝试处理查询
    result = try_ollama_query(query)
    if result:
        return result

    # 如果Ollama失败，使用备用的关键词匹配
    return fallback_natural_language_query(query)


def try_ollama_query(query):
    """
    尝试使用Ollama处理查询
    """
    system_prompt = """
你是一个旅行数据库助手，需要将用户的自然语言查询解析为结构化请求。

确定查询是关于航班还是酒店:
1. 如果是关于航班，确定是:
   a) 所有航班
   b) 特定机场之间的航班（提取出发和目的地机场）
   c) 特定航空公司的航班（提取航空公司名称）

2. 如果是关于酒店，确定是:
   a) 所有酒店
   b) 特定县的酒店（提取县名）
   c) 特定州的酒店（提取州名）
   d) 特定县和州的酒店（提取两者）

以JSON格式返回答案:
航班查询:
{
  "type": "flights",
  "query_type": "all_flights" 或 "by_airports" 或 "by_airline",
  "params": {
    "starting": "出发机场代码（如适用）",
    "destination": "目的地机场代码（如适用）",
    "airline": "航空公司名称（如适用）"
  }
}

酒店查询:
{
  "type": "hotels",
  "query_type": "all_hotels" 或 "by_county" 或 "by_state" 或 "by_county_and_state",
  "params": {
    "county": "县名（如适用）",
    "state": "州名（如适用）"
  }
}

不要提供额外解释，只返回JSON对象。
"""

    full_prompt = f"{system_prompt}\n\n用户查询: {query}"

    try:
        # 调用Ollama API
        response = call_ollama(full_prompt)
        if not response:
            return None

        print(f"Ollama返回: {response[:100]}...")

        # 尝试从响应中提取JSON
        try:
            # 查找JSON开始的位置
            json_start = response.find('{')
            json_end = response.rfind('}') + 1

            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                parsed = json.loads(json_str)

                # 处理解析结果
                return route_query(parsed)
            else:
                print("无法从Ollama响应中提取JSON")
                return None
        except json.JSONDecodeError as e:
            print(f"JSON解析错误: {str(e)}")
            return None
    except Exception as e:
        print(f"处理Ollama查询时出错: {str(e)}")
        return None


def route_query(parsed_query):
    """
    根据解析的查询路由到相应的处理函数
    """
    try:
        print(f"路由查询: {parsed_query}")

        if parsed_query.get("type") == "flights":
            query_type = parsed_query.get("query_type")
            params = parsed_query.get("params", {})

            if query_type == "all_flights":
                return query_flights("all_flights")
            elif query_type == "by_airports":
                return query_flights("by_airports", params.get("starting", ""), params.get("destination", ""))
            elif query_type == "by_airline":
                return query_flights("by_airline", params.get("airline", ""))

        elif parsed_query.get("type") == "hotels":
            query_type = parsed_query.get("query_type")
            params = parsed_query.get("params", {})

            if query_type == "all_hotels":
                return query_hotels()
            elif query_type == "by_county":
                return query_hotels(county=params.get("county", ""))
            elif query_type == "by_state":
                return query_hotels(state=params.get("state", ""))
            elif query_type == "by_county_and_state":
                return query_hotels(county=params.get("county", ""), state=params.get("state", ""))

        return None
    except Exception as e:
        print(f"路由查询时出错: {str(e)}")
        return None


def fallback_natural_language_query(query):
    """
    当Ollama不可用时的后备自然语言处理（基于关键词匹配）
    """
    print("使用备用关键词匹配处理查询")

    # 简单的关键词匹配
    query_lower = query.lower()

    # 检查是否是航班查询
    if any(kw in query_lower for kw in ["航班", "飞机", "flight", "plane", "air", "飞行"]):
        # 检查是否包含机场代码或名称
        airport_patterns = [
            (r'从\s*([A-Za-z]{3})\s*到\s*([A-Za-z]{3})', '从机场代码到机场代码'),
            (r'从\s*(\w+)\s*到\s*(\w+)', '从城市到城市'),
            (r'([A-Za-z]{3})\s*到\s*([A-Za-z]{3})', '机场代码到机场代码'),
            (r'from\s*([A-Za-z]{3})\s*to\s*([A-Za-z]{3})', '从到'),
            (r'between\s*([A-Za-z]{3})\s*and\s*([A-Za-z]{3})', '之间')
        ]

        for pattern, desc in airport_patterns:
            import re
            match = re.search(pattern, query_lower)
            if match:
                starting = match.group(1).upper()
                destination = match.group(2).upper()

                # 如果提取到了看起来像机场代码的内容
                if starting and destination:
                    # 确保是3个字符的机场代码
                    starting = starting[:3]
                    destination = destination[:3]

                    print(f"识别到机场查询: 从{starting}到{destination}")
                    return query_flights("by_airports", starting, destination)

        # 检查航空公司
        for kw in ["航空公司", "航空", "airline", "carrier"]:
            if kw in query_lower:
                parts = query_lower.split(kw)
                if len(parts) > 1:
                    airline = parts[1].strip()
                    if airline:
                        # 提取第一个单词作为航空公司名称
                        airline_name = airline.split()[0]
                        print(f"识别到航空公司查询: {airline_name}")
                        return query_flights("by_airline", airline_name)

        # 默认返回所有航班
        print("默认返回所有航班")
        return query_flights("all_flights")

    # 检查是否是酒店查询
    elif any(kw in query_lower for kw in ["酒店", "宾馆", "hotel", "motel", "inn", "住宿"]):
        county = None
        state = None

        # 检查是否包含县名
        for kw in ["县", "county"]:
            if kw in query_lower:
                parts = query_lower.split(kw)
                if len(parts) > 0:
                    county = parts[0].strip()

        # 检查是否包含州名
        for kw in ["州", "state"]:
            if kw in query_lower:
                parts = query_lower.split(kw)
                if len(parts) > 0:
                    state = parts[0].strip()

        # 根据提取的信息查询
        if county and state:
            return query_hotels(county, state)
        elif county:
            return query_hotels(county=county)
        elif state:
            return query_hotels(state=state)
        else:
            return query_hotels()

    # 无法确定查询类型
    return "无法理解您的查询。请尝试更清晰的表述，例如'显示所有航班'或'查询加利福尼亚州的酒店'。"


# 创建Gradio界面
with gr.Blocks(title="旅行数据库查询") as demo:
    gr.Markdown("# 旅行数据库查询")
    gr.Markdown("查询航班和酒店信息")

    with gr.Tab("自然语言查询"):
        with gr.Row():
            nl_input = gr.Textbox(label="您的问题", placeholder="例如，显示从LAX到JFK的航班")
            nl_button = gr.Button("搜索")
        nl_output = gr.Textbox(label="结果", lines=10)
        nl_button.click(natural_language_query, inputs=[nl_input], outputs=[nl_output])

    with gr.Tab("航班搜索"):
        with gr.Row():
            query_type = gr.Radio(
                ["所有航班", "按机场搜索", "按航空公司搜索"],
                label="查询类型",
                value="所有航班"
            )

        with gr.Row():
            starting_airport = gr.Textbox(label="出发机场代码", visible=False)
            destination_airport = gr.Textbox(label="目的地机场代码", visible=False)
            airline_name = gr.Textbox(label="航空公司名称", visible=False)


        def update_visibility(query_type):
            if query_type == "所有航班":
                return gr.update(visible=False), gr.update(visible=False), gr.update(visible=False)
            elif query_type == "按机场搜索":
                return gr.update(visible=True), gr.update(visible=True), gr.update(visible=False)
            else:  # 按航空公司搜索
                return gr.update(visible=False), gr.update(visible=False), gr.update(visible=True)


        query_type.change(
            update_visibility,
            inputs=[query_type],
            outputs=[starting_airport, destination_airport, airline_name]
        )

        flight_button = gr.Button("搜索航班")
        flight_results = gr.Textbox(label="结果", lines=10)


        def process_flight_query(query_type, starting, destination, airline):
            if query_type == "所有航班":
                return query_flights("all_flights")
            elif query_type == "按机场搜索":
                return query_flights("by_airports", starting, destination)
            else:  # 按航空公司搜索
                return query_flights("by_airline", airline)


        flight_button.click(
            process_flight_query,
            inputs=[query_type, starting_airport, destination_airport, airline_name],
            outputs=[flight_results]
        )

    with gr.Tab("酒店搜索"):
        with gr.Row():
            county = gr.Textbox(label="县")
            state = gr.Textbox(label="州")

        hotel_button = gr.Button("搜索酒店")
        hotel_results = gr.Textbox(label="结果", lines=10)

        hotel_button.click(
            query_hotels,
            inputs=[county, state],
            outputs=[hotel_results]
        )

# 启动应用
if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)