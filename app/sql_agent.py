import sqlite3
import os
import logging
from sqlalchemy import create_engine, text

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLite数据库路径
SQLITE_DB_DIR = os.environ.get("SQLITE_DB_DIR", os.path.join(os.getcwd(), "data"))
LOCATION_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_location.db")
RATE_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_rate.db")

# 创建SQLAlchemy引擎
location_engine = create_engine(f"sqlite:///{LOCATION_DB_PATH}")
rate_engine = create_engine(f"sqlite:///{RATE_DB_PATH}")

def get_connection(db_path):
    """
    返回到指定SQLite数据库的连接
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # 返回类似字典的对象
        return conn
    except Exception as e:
        logger.error(f"连接SQLite数据库时出错 {db_path}: {e}")
        raise

def execute_sql_query(db_path, query, params=(), fetch_all=True):
    """
    执行SQL查询并返回结果
    
    参数:
        db_path: 数据库文件路径
        query: SQL查询字符串
        params: 查询参数
        fetch_all: 是否获取所有结果，False只获取一行
        
    返回:
        查询结果列表
    """
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        if fetch_all:
            results = cursor.fetchall()
        else:
            results = cursor.fetchone()
            
        conn.commit()
        return results
    except Exception as e:
        logger.error(f"执行查询时出错 {db_path}: {e}")
        logger.error(f"查询: {query}")
        logger.error(f"参数: {params}")
        raise
    finally:
        if conn:
            conn.close()

def create_views():
    """
    创建连接所有表的视图
    """
    try:
        # 1. 创建酒店视图 - 连接所有酒店名称表和位置表
        # 检查视图是否已存在
        hotel_view_check_query = "SELECT name FROM sqlite_master WHERE type='view' AND name='hotel_complete_view'"
        hotel_view_exists = execute_sql_query(LOCATION_DB_PATH, hotel_view_check_query)
        
        if not hotel_view_exists:
            # 创建酒店视图
            create_hotel_view_query = """
            CREATE VIEW IF NOT EXISTS hotel_complete_view AS
            -- 从hotel_name1获取
            SELECT 
                h1.ID, 
                h1.hotel_name, 
                l.county, 
                l.state
            FROM hotel_name1 h1
            JOIN location l ON h1.ID = l.ID
            
            UNION ALL
            
            -- 从hotel_name2获取 (注意列名不同)
            SELECT 
                h2.hotel AS ID, 
                h2.hotel_name, 
                l.county, 
                l.state
            FROM hotel_name2 h2
            JOIN location l ON h2.hotel = l.ID
            
            UNION ALL
            
            -- 从hotel_name3获取
            SELECT 
                h3.ID, 
                h3.hotel_name, 
                l.county, 
                l.state
            FROM hotel_name3 h3
            JOIN location l ON h3.ID = l.ID
            """
            execute_sql_query(LOCATION_DB_PATH, create_hotel_view_query)
            logger.info("成功创建hotel_complete_view视图")
        else:
            logger.info("hotel_complete_view视图已存在")
        
        # 2. 创建评分视图 - 在rate数据库中
        # 检查视图是否已存在
        rate_view_check_query = "SELECT name FROM sqlite_master WHERE type='view' AND name='rate_complete_view'"
        rate_view_exists = execute_sql_query(RATE_DB_PATH, rate_view_check_query)
        
        if not rate_view_exists:
            # 创建评分视图
            create_rate_view_query = """
            CREATE VIEW IF NOT EXISTS rate_complete_view AS
            SELECT 
                r.ID,
                r.rating,
                r.sleepquality,
                r.service,
                r.rooms,
                r.cleanliness
            FROM rate r
            """
            execute_sql_query(RATE_DB_PATH, create_rate_view_query)
            logger.info("成功创建rate_complete_view视图")
        else:
            logger.info("rate_complete_view视图已存在")
            
        return True
    except Exception as e:
        logger.error(f"创建视图时出错: {e}")
        return False

# 应用启动时创建视图
create_views_result = create_views()
if not create_views_result:
    logger.warning("无法创建视图，将使用备用查询方法")

def get_all_hotels():
    """
    获取所有酒店信息（不包含评分）
    """
    try:
        query = "SELECT DISTINCT ID, hotel_name, county, state FROM hotel_complete_view"
        return execute_sql_query(LOCATION_DB_PATH, query)
    except Exception as e:
        logger.error(f"获取所有酒店时出错: {e}")
        # 备用方法
        result = []
        try:
            loc_query = "SELECT ID, county, state FROM location"
            locations = execute_sql_query(LOCATION_DB_PATH, loc_query)
            
            for loc in locations:
                hotel_name = get_hotel_name_fallback(loc['ID'])
                result.append({
                    'ID': loc['ID'],
                    'hotel_name': hotel_name,
                    'county': loc['county'],
                    'state': loc['state']
                })
            return result
        except Exception as e2:
            logger.error(f"备用方法也失败: {e2}")
            raise

def get_hotel_name_fallback(hotel_id):
    """
    备用方法：从酒店名称表中获取酒店名称
    """
    hotel_name_tables = ['hotel_name1', 'hotel_name2', 'hotel_name3']
    
    for table in hotel_name_tables:
        try:
            query = f"SELECT hotel_name FROM {table} WHERE "
            if table == 'hotel_name2':
                query += "hotel = ?"
            else:
                query += "ID = ?"
                
            result = execute_sql_query(LOCATION_DB_PATH, query, (hotel_id,), fetch_all=False)
            if result:
                return result['hotel_name']
        except Exception:
            continue
    
    return "Unknown Hotel"

def get_all_reviews():
    """
    获取所有酒店评论并包含名称 - 使用视图连接
    """
    try:
        query = """
        SELECT 
            r.rating, 
            r.sleepquality, 
            r.service, 
            r.rooms, 
            r.cleanliness,
            h.hotel_name,
            h.county,
            h.state
        FROM rate_complete_view r
        JOIN hotel_complete_view h ON r.ID = h.ID
        """
        
        # 尝试使用连接查询视图表
        try:
            # 注意：跨数据库查询在SQLite中不直接支持，我们需要在代码层面实现
            hotel_data = execute_sql_query(LOCATION_DB_PATH, "SELECT DISTINCT ID, hotel_name, county, state FROM hotel_complete_view")
            rate_data = execute_sql_query(RATE_DB_PATH, "SELECT * FROM rate_complete_view")
            
            # 创建ID到hotel信息的映射
            hotel_map = {hotel['ID']: hotel for hotel in hotel_data}
            
            result = []
            for rate in rate_data:
                hotel_id = rate['ID']
                if hotel_id in hotel_map:
                    hotel = hotel_map[hotel_id]
                    result.append((
                        rate['rating'],
                        rate['sleepquality'],
                        rate['service'],
                        rate['rooms'],
                        rate['cleanliness'],
                        0,  # 替代 value 列
                        hotel['hotel_name'],
                        hotel['county'],
                        hotel['state']
                    ))
            
            return result
        except Exception as e:
            logger.warning(f"视图连接查询失败，使用备用方法: {e}")
            
            # 从视图中获取所有酒店
            hotels = get_all_hotels()
            
            result = []
            for hotel in hotels:
                hotel_id = hotel['ID']
                
                # 获取评分信息
                rate_query = """
                SELECT 
                    rating, 
                    sleepquality, 
                    service, 
                    rooms, 
                    cleanliness
                FROM rate_complete_view
                WHERE ID = ?
                """
                rate = execute_sql_query(RATE_DB_PATH, rate_query, (hotel_id,), fetch_all=False)
                
                if rate:
                    result.append((
                        rate['rating'],
                        rate['sleepquality'],
                        rate['service'],
                        rate['rooms'],
                        rate['cleanliness'],
                        0,  # 替代 value 列
                        hotel['hotel_name'],
                        hotel['county'],
                        hotel['state']
                    ))
            
            return result
    except Exception as e:
        logger.error(f"获取所有评论时出错: {e}")
        raise

def get_reviews_by_county(county):
    """
    获取特定县的酒店评论 - 使用视图表
    """
    try:
        # 从视图中获取指定县的所有酒店
        county_query = "SELECT ID, hotel_name, county, state FROM hotel_complete_view WHERE county = ?"
        hotels = execute_sql_query(LOCATION_DB_PATH, county_query, (county,))
        
        # 获取所有酒店ID
        hotel_ids = [hotel['ID'] for hotel in hotels]
        
        if not hotel_ids:
            return []  # 如果没有找到酒店，返回空列表
        
        # 创建ID到hotel信息的映射
        hotel_map = {hotel['ID']: hotel for hotel in hotels}
        
        # 获取这些酒店的评分信息
        # 由于SQLite不支持数组参数，我们需要构建参数列表
        placeholders = ','.join(['?' for _ in hotel_ids])
        rate_query = f"""
        SELECT * 
        FROM rate_complete_view
        WHERE ID IN ({placeholders})
        """
        rates = execute_sql_query(RATE_DB_PATH, rate_query, hotel_ids)
        
        result = []
        for rate in rates:
            hotel_id = rate['ID']
            hotel = hotel_map.get(hotel_id)
            
            if hotel:
                result.append((
                    rate['rating'],
                    rate['sleepquality'],
                    rate['service'],
                    rate['rooms'],
                    rate['cleanliness'],
                    0,  # 替代 value 列
                    hotel['hotel_name'],
                    hotel['county'],
                    hotel['state']
                ))
        
        return result
    except Exception as e:
        logger.error(f"获取县评论时出错 {county}: {e}")
        raise

def get_reviews_by_state(state):
    """
    获取特定州的酒店评论 - 使用视图表
    """
    try:
        # 从视图中获取指定州的所有酒店
        state_query = "SELECT ID, hotel_name, county, state FROM hotel_complete_view WHERE state = ?"
        hotels = execute_sql_query(LOCATION_DB_PATH, state_query, (state,))
        
        # 获取所有酒店ID
        hotel_ids = [hotel['ID'] for hotel in hotels]
        
        if not hotel_ids:
            return []  # 如果没有找到酒店，返回空列表
        
        # 创建ID到hotel信息的映射
        hotel_map = {hotel['ID']: hotel for hotel in hotels}
        
        # 获取这些酒店的评分信息
        # 由于SQLite不支持数组参数，我们需要构建参数列表
        placeholders = ','.join(['?' for _ in hotel_ids])
        rate_query = f"""
        SELECT * 
        FROM rate_complete_view
        WHERE ID IN ({placeholders})
        """
        rates = execute_sql_query(RATE_DB_PATH, rate_query, hotel_ids)
        
        result = []
        for rate in rates:
            hotel_id = rate['ID']
            hotel = hotel_map.get(hotel_id)
            
            if hotel:
                result.append((
                    rate['rating'],
                    rate['sleepquality'],
                    rate['service'],
                    rate['rooms'],
                    rate['cleanliness'],
                    0,  # 替代 value 列
                    hotel['hotel_name'],
                    hotel['county'],
                    hotel['state']
                ))
        
        return result
    except Exception as e:
        logger.error(f"获取州评论时出错 {state}: {e}")
        raise

def get_average_ratings_by_state():
    """
    获取每个州酒店的平均评分
    """
    try:
        # 从视图获取所有酒店
        hotels_query = "SELECT DISTINCT ID, state FROM hotel_complete_view ORDER BY state"
        hotels = execute_sql_query(LOCATION_DB_PATH, hotels_query)
        
        # 按州分组
        state_ratings = {}
        for hotel in hotels:
            hotel_id = hotel['ID']
            state = hotel['state']
            
            # 获取评分信息
            rate_query = "SELECT rating FROM rate WHERE ID = ?"
            rate = execute_sql_query(RATE_DB_PATH, rate_query, (hotel_id,), fetch_all=False)
            
            if rate:
                if state not in state_ratings:
                    state_ratings[state] = {'sum': 0, 'count': 0}
                
                state_ratings[state]['sum'] += rate['rating']
                state_ratings[state]['count'] += 1
        
        # 计算平均值
        result = []
        for state, data in state_ratings.items():
            avg_rating = data['sum'] / data['count'] if data['count'] > 0 else 0
            result.append({'state': state, 'avg_rating': avg_rating, 'hotel_count': data['count']})
        
        # 按平均评分排序（最高在前）
        result.sort(key=lambda x: x['avg_rating'], reverse=True)
        
        return result
    except Exception as e:
        logger.error(f"获取州平均评分时出错: {e}")
        raise

def count_hotels_by_state():
    """
    统计每个州的酒店数量
    """
    try:
        query = "SELECT state, COUNT(DISTINCT ID) as count FROM hotel_complete_view GROUP BY state ORDER BY count DESC"
        return execute_sql_query(LOCATION_DB_PATH, query)
    except Exception as e:
        logger.error(f"按州统计酒店时出错: {e}")
        raise

def find_hotels_with_min_rating(min_rating):
    """
    查找评分至少为指定值的酒店 - 使用视图表
    """
    try:
        # 首先获取评分高于指定值的酒店ID和评分信息
        rate_query = f"SELECT * FROM rate_complete_view WHERE rating >= {min_rating}"
        rate_matches = execute_sql_query(RATE_DB_PATH, rate_query)
        
        # 获取所有符合条件的酒店ID
        hotel_ids = [rate['ID'] for rate in rate_matches]
        
        if not hotel_ids:
            return []  # 如果没有找到酒店，返回空列表
        
        # 创建ID到rate信息的映射
        rate_map = {rate['ID']: rate for rate in rate_matches}
        
        # 获取这些酒店的信息
        # 由于SQLite不支持数组参数，我们需要构建参数列表
        placeholders = ','.join(['?' for _ in hotel_ids])
        hotel_query = f"""
        SELECT * 
        FROM hotel_complete_view
        WHERE ID IN ({placeholders})
        """
        hotels = execute_sql_query(LOCATION_DB_PATH, hotel_query, hotel_ids)
        
        result = []
        for hotel in hotels:
            hotel_id = hotel['ID']
            rate = rate_map.get(hotel_id)
            
            if rate:
                result.append((
                    rate['rating'],
                    rate['sleepquality'],
                    rate['service'],
                    rate['rooms'],
                    rate['cleanliness'],
                    0,  # 替代 value 列
                    hotel['hotel_name'],
                    hotel['county'],
                    hotel['state']
                ))
        
        return result
    except Exception as e:
        logger.error(f"查找最低评分酒店时出错 {min_rating}: {e}")
        raise

def execute_custom_query(query, is_location_db=True):
    """
    执行自定义SQL查询
    """
    db_path = LOCATION_DB_PATH if is_location_db else RATE_DB_PATH
    return execute_sql_query(db_path, query)
