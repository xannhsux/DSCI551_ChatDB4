from langchain_community.llms.ollama import Ollama
from sqlalchemy import text
from app.db_config import sql_engine

llm = Ollama(model="llama3")

def answer_sql_question(question: str) -> str:
    """
    使用 LLaMA 模型将自然语言问题转换为 SQL 查询，然后执行该查询
    """
    prompt = f"""
你是 SQL 查询助手。将用户的问题转换成 SQL 查询，不要包含任何解释：
问题：{question}
"""
    # 获取 SQL 查询
    sql_query = llm.invoke(prompt).strip()
    
    try:
        # 执行 SQL 查询
        with sql_engine.connect() as conn:
            result = conn.execute(text(sql_query))
            rows = result.fetchall()
            
            # 构建结果
            if not rows:
                return "查询未返回任何结果。"
            
            # 获取列名
            columns = result.keys()
            
            # 格式化结果
            formatted_result = "查询结果:\n\n"
            formatted_result += " | ".join(columns) + "\n"
            formatted_result += "-" * (sum(len(col) for col in columns) + 3 * (len(columns) - 1)) + "\n"
            
            for row in rows:
                formatted_result += " | ".join(str(cell) for cell in row) + "\n"
            
            return formatted_result
    except Exception as e:
        return f"执行查询时出错: {str(e)}\n\n尝试执行的查询: {sql_query}"
