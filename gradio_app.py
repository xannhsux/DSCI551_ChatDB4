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
        result += f"航班 {i+1}:\n"
        result += f"  出发机场: {flight.get('startingAirport', 'N/A')}\n"
        result += f"  目的地机场: {flight.get('destinationAirport', 'N/A')}\n"
        result += f"  航空公司: {flight.get('segmentsAirlineName', 'N/A')}\n"
        result += f"  价格: ${flight.get('totalFare', 'N/A')}\n"
        result += f"  时长: {flight.get('totalTripDuration', 'N/A')} 分钟\n\n"
    
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
        result += f"酒店 {i+1}: {hotel.get('hotel_name', 'N/A')}\n"
        result += f"  县: {hotel.get('county', 'N/A')}\n"
        result += f"  州: {hotel.get('state', 'N/A')}\n"
        result += f"  评分: {hotel.get('rating', 'N/A')}\n"
        result += f"  清洁度: {hotel.get('cleanliness', 'N/A')}\n"
        result += f"  服务: {hotel.get('service', 'N/A')}\n\n"
    
    return result

def call_ollama(prompt):
    """
    直接调用Ollama API
    """
    try:
        payload = {
            "model": "tinyllama",
            "prompt": prompt,
            "stream": False
        }
        
        response = requests.post(f"{OLLAMA_API}/api/generate", json=payload)
        
        if response.status_code == 200:
            return response.json().get("response", "无响应")
        else:
            return f"Ollama API错误: {response.status_code} - {response.text}"
    except Exception as e:
        return f"调用Ollama时出错: {str(e)}"

def natural_language_query(query):
    """
    使用Ollama处理自然语言查询
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

        # 尝试从响应中提取JSON
        try:
            # 查找JSON开始的位置
            json_start = response.find('{')
            json_end = response.rfind('}') + 1

            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                parsed = json.loads(json_str)
            else:
                # 如果没有找到JSON格式，退回到关键词匹配
                return fallback_natural_language_query(query)

            # 根据解析结果路由到相应的函数
            if parsed.get("type") == "flights":
                query_type = parsed.get("query_type")
                params = parsed.get("params", {})

                if query_type == "all_flights":
                    return query_flights("all_flights")
                elif query_type == "by_airports":
                    return query_flights("by_airports", params.get("starting", ""), params.get("destination", ""))
                elif query_type == "by_airline":
                    return query_flights("by_airline", params.get("airline", ""))

            elif parsed.get("type") == "hotels":
                query_type = parsed.get("query_type")
                params = parsed.get("params", {})

                if query_type == "all_hotels":
                    return query_hotels()
                elif query_type == "by_county":
                    return query_hotels(county=params.get("county", ""))
                elif query_type == "by_state":
                    return query_hotels(state=params.get("state", ""))
                elif query_type == "by_county_and_state":
                    return query_hotels(county=params.get("county", ""), state=params.get("state", ""))

            return "无法理解查询。请尝试更具体的表述。"
        except json.JSONDecodeError:
            # 如果JSON解析失败，退回到关键词匹配
            return fallback_natural_language_query(query)
    except Exception as e:
        # 如果Ollama调用失败，退回到关键词匹配
        return f"处理查询时出错: {str(e)}\n\n尝试使用关键词匹配...\n\n{fallback_natural_language_query(query)}"

def fallback_natural_language_query(query):
    """
    当Ollama不可用时的后备自然语言处理（基于关键词匹配）
    """
    # 简单的关键词匹配
    query_lower = query.lower()
    
    # 检查是否是航班查询
    if "航班" in query_lower or "飞机" in query_lower or "flight" in query_lower:
        # 检查是否包含机场代码
        airport_keywords = ["从", "到", "between", "from", "to"]
        
        # 检查是否有特定航空公司
        airline_keywords = ["航空公司", "航空", "airline"]
        
        # 优先检查机场
        for keyword in airport_keywords:
            if keyword in query_lower:
                # 简单解析，假设格式为"从LAX到JFK"或"LAX到JFK"
                parts = query_lower.split(keyword)
                if len(parts) > 1:
                    # 非常简化的处理，实际应用需要更复杂的逻辑
                    starting = parts[0].strip().upper()
                    destination = parts[1].strip().upper()
                    if starting and destination:
                        return query_flights("by_airports", starting, destination)
        
        # 检查航空公司
        for keyword in airline_keywords:
            if keyword in query_lower:
                parts = query_lower.split(keyword)
                if len(parts) > 1:
                    airline = parts[1].strip()
                    if airline:
                        return query_flights("by_airline", airline)
        
        # 默认返回所有航班
        return query_flights("all_flights")
    
    # 检查是否是酒店查询
    elif "酒店" in query_lower or "宾馆" in query_lower or "hotel" in query_lower:
        county = None
        state = None
        
        # 检查是否包含县名
        county_keywords = ["县", "county"]
        for keyword in county_keywords:
            if keyword in query_lower:
                parts = query_lower.split(keyword)
                if len(parts) > 0:
                    county = parts[0].strip()
        
        # 检查是否包含州名
        state_keywords = ["州", "state"]
        for keyword in state_keywords:
            if keyword in query_lower:
                parts = query_lower.split(keyword)
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
    else:
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
