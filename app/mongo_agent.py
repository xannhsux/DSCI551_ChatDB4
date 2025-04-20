from pymongo import MongoClient
import os
import json
from bson import json_util, ObjectId

# 使用环境变量获取URI，默认使用硬编码的URI
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://flightsdata:dsci551@flightsdata.y57hp.mongodb.net/?retryWrites=true&w=majority")

# 本地MongoDB连接字符串（作为备选）
MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
MONGO_PORT = os.environ.get("MONGO_PORT", "27017")
LOCAL_MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"

def get_client():
    """
    获取MongoDB客户端连接，优先使用云端，失败则使用本地
    """
    try:
        # 先尝试连接云端MongoDB
        client = MongoClient(MONGO_URI)
        # 测试连接
        client.server_info()
        print("连接到云端MongoDB成功")
        return client
    except Exception as e:
        print(f"连接到云端MongoDB失败: {e}")
        try:
            # 尝试使用本地MongoDB作为后备
            client = MongoClient(LOCAL_MONGO_URI)
            client.server_info()
            print("连接到本地MongoDB成功")
            return client
        except Exception as e:
            print(f"连接到本地MongoDB失败: {e}")
            # 重新抛出异常
            raise


# 获取客户端连接
client = get_client()
db = client["flights"]  # 连接到flights数据库


# 用于处理ObjectId的函数
def convert_objectid_to_str(document):
    """
    将文档中的ObjectId转换为字符串
    """
    if isinstance(document, list):
        return [convert_objectid_to_str(item) for item in document]
    elif isinstance(document, dict):
        result = {}
        for key, value in document.items():
            if isinstance(value, ObjectId):
                result[key] = str(value)
            elif isinstance(value, (dict, list)):
                result[key] = convert_objectid_to_str(value)
            else:
                result[key] = value
        return result
    else:
        return document


# Schema Exploration 功能
def get_collections():
    """
    获取数据库中的所有集合
    """
    return db.list_collection_names()


def get_sample_documents(collection_name, limit=5):
    """
    获取指定集合的样本文档
    """
    collection = db[collection_name]
    docs = list(collection.find().limit(limit))
    return convert_objectid_to_str(docs)


# 基本查询功能
def find_with_projection(collection_name, query={}, projection=None, limit=100):
    """
    使用投影执行find查询
    """
    collection = db[collection_name]
    docs = list(collection.find(query, projection).limit(limit))
    return convert_objectid_to_str(docs)


def aggregate(collection_name, pipeline):
    """
    执行聚合管道查询
    """
    collection = db[collection_name]
    docs = list(collection.aggregate(pipeline))
    return convert_objectid_to_str(docs)


# 航班查询功能，更新为使用新的集合名称和字段
def get_all_flights(collection_name="flights_basic", limit=100):
    """
    获取所有航班（带限制）
    """
    print(f"获取所有航班，使用集合: {collection_name}")
    collection = db[collection_name]
    docs = list(collection.find({}).limit(limit))
    return convert_objectid_to_str(docs)


def get_flights_by_airports(starting, destination, collection_name="flights_basic"):
    """
    按起始和目的地机场查询航班
    """
    print(f"查询航班: 从 {starting} 到 {destination}, 使用集合: {collection_name}")
    collection = db[collection_name]

    # 检查数据库中是否有数据
    total_flights = collection.count_documents({})
    print(f"数据库中总航班数: {total_flights}")

    # 获取几个样本记录了解数据结构
    sample = list(collection.find({}).limit(2))
    for doc in sample:
        print(f"样本记录字段: {list(doc.keys())}")

    # 执行精确查询
    results = list(collection.find({
        "startingAirport": starting,
        "destinationAirport": destination
    }))

    # 如果没有结果，尝试模糊查询
    if not results:
        print("精确查询无结果，尝试模糊查询...")
        results = list(collection.find({
            "startingAirport": {"$regex": starting, "$options": "i"},
            "destinationAirport": {"$regex": destination, "$options": "i"}
        }))

    print(f"查询结果数量: {len(results)}")
    return convert_objectid_to_str(results)


def get_flights_by_airline(airline_name, collection_name="flights_segments"):
    """
    按航空公司名称查询航班
    注意：根据新结构，航空公司信息在flights_segments集合中
    """
    print(f"按航空公司查询: {airline_name}, 使用集合: {collection_name}")
    collection = db[collection_name]

    # 使用新字段segmentsAirlineName查询
    # 因为segmentsAirlineName字段可能包含多个航空公司（格式为"airline1||airline2"），使用正则表达式匹配
    query = {"segmentsAirlineName": {"$regex": airline_name, "$options": "i"}}
    segments = list(collection.find(query, {"originalId": 1}))

    if not segments:
        print("未找到匹配的航班段")
        return []

    # 获取匹配航班段的originalId列表
    original_ids = [segment["originalId"] for segment in segments]

    # 在flights_basic集合中查找对应的航班详情
    basic_collection = db["flights_basic"]
    results = list(basic_collection.find({"originalId": {"$in": original_ids}}))

    print(f"查询结果数量: {len(results)}")
    return convert_objectid_to_str(results)


# 高级查询功能
def search_flights(query_params, limit=100):
    """
    使用多种条件搜索航班

    query_params可包含:
    - starting: 出发机场
    - destination: 目的地机场
    - airline: 航空公司
    - max_price: 最高价格
    - min_price: 最低价格
    - sort_by: 排序字段
    - sort_order: 排序顺序 (1 升序, -1 降序)
    - skip: 跳过的结果数
    - limit: 返回的最大结果数
    """
    print(f"高级搜索航班，参数: {query_params}")

    # 基本查询条件（用于flights_basic集合）
    basic_query = {}
    if "starting" in query_params and query_params["starting"]:
        basic_query["startingAirport"] = {"$regex": query_params["starting"], "$options": "i"}
    if "destination" in query_params and query_params["destination"]:
        basic_query["destinationAirport"] = {"$regex": query_params["destination"], "$options": "i"}

    # 价格范围条件
    price_condition = {}
    if "max_price" in query_params and query_params["max_price"]:
        price_condition["$lte"] = float(query_params["max_price"])
    if "min_price" in query_params and query_params["min_price"]:
        price_condition["$gte"] = float(query_params["min_price"])
    if price_condition:
        basic_query["totalFare"] = price_condition

    # 航空公司查询条件（如果有的话，需要在flights_segments集合中查询）
    has_airline_filter = "airline" in query_params and query_params["airline"]

    # 获取排序参数
    sort_field = query_params.get("sort_by", "totalFare")
    sort_order = int(query_params.get("sort_order", 1))  # 1升序, -1降序

    # 获取分页参数
    skip = int(query_params.get("skip", 0))
    limit = int(query_params.get("limit", limit))

    # 如果没有航空公司筛选，直接在flights_basic集合中查询
    if not has_airline_filter:
        basic_collection = db["flights_basic"]
        print(f"构建的查询条件 (flights_basic): {basic_query}")
        results = list(basic_collection.find(basic_query)
                       .sort(sort_field, sort_order)
                       .skip(skip)
                       .limit(limit))
        return convert_objectid_to_str(results)
    else:
        # 如果有航空公司筛选，先在flights_segments中查询匹配的originalId
        segments_collection = db["flights_segments"]
        segments_query = {"segmentsAirlineName": {"$regex": query_params["airline"], "$options": "i"}}
        print(f"构建的查询条件 (flights_segments): {segments_query}")

        matching_segments = list(segments_collection.find(segments_query, {"originalId": 1}))
        original_ids = [segment["originalId"] for segment in matching_segments]

        if not original_ids:
            print("未找到匹配航空公司的航班")
            return []

        # 在flights_basic中查询这些originalId，并应用其他筛选条件
        basic_query["originalId"] = {"$in": original_ids}
        basic_collection = db["flights_basic"]
        print(f"构建的查询条件 (flights_basic with originalIds): {basic_query}")

        results = list(basic_collection.find(basic_query)
                       .sort(sort_field, sort_order)
                       .skip(skip)
                       .limit(limit))
        return convert_objectid_to_str(results)


# 聚合查询示例
def get_average_fare_by_airline():
    """
    获取各航空公司的平均票价
    """
    # 第一步：从flights_segments获取所有航空公司和对应的originalId
    segments_collection = db["flights_segments"]

    # 对segmentsAirlineName进行处理（分割多航空公司的情况）
    pipeline = [
        # 展开segmentsAirlineName字段（处理多航空公司情况，如"UA||DL"）
        {"$addFields": {
            "airlines": {"$split": ["$segmentsAirlineName", "||"]}
        }},
        # 展开airlines数组，每个航空公司生成一个文档
        {"$unwind": "$airlines"},
        # 按航空公司和originalId分组
        {"$group": {
            "_id": {
                "airline": "$airlines",
                "originalId": "$originalId"
            }
        }},
        # 仅保留必要字段
        {"$project": {
            "_id": 0,
            "airline": "$_id.airline",
            "originalId": "$_id.originalId"
        }}
    ]

    airline_flights = list(segments_collection.aggregate(pipeline))

    # 第二步：从flights_basic获取对应航班的价格
    basic_collection = db["flights_basic"]

    # 按航空公司分组计算平均价格
    result = {}

    # 对每个航空公司分别计算
    airlines = set(item["airline"] for item in airline_flights)

    for airline in airlines:
        # 获取该航空公司的所有originalId
        original_ids = [item["originalId"] for item in airline_flights if item["airline"] == airline]

        # 查询这些originalId对应的航班价格
        flights = list(basic_collection.find({"originalId": {"$in": original_ids}}, {"totalFare": 1}))

        if flights:
            # 计算平均价格
            total_fare = sum(flight["totalFare"] for flight in flights)
            avg_fare = total_fare / len(flights)

            result[airline] = {
                "averageFare": avg_fare,
                "flightCount": len(flights)
            }

    # 转换为列表格式返回
    formatted_result = []
    for airline, data in result.items():
        formatted_result.append({
            "airline": airline,
            "averageFare": data["averageFare"],
            "flightCount": data["flightCount"]
        })

    # 按平均价格排序
    formatted_result.sort(key=lambda x: x["averageFare"])

    return formatted_result


def get_popular_routes(limit=10):
    """
    获取最受欢迎的航线（以航班数量计算）
    """
    basic_collection = db["flights_basic"]

    pipeline = [
        {"$group": {
            "_id": {
                "from": "$startingAirport",
                "to": "$destinationAirport"
            },
            "count": {"$sum": 1},
            "avgFare": {"$avg": "$totalFare"}
        }},
        {"$sort": {"count": -1}},
        {"$limit": limit},
        {"$project": {
            "route": {
                "from": "$_id.from",
                "to": "$_id.to"
            },
            "flightCount": "$count",
            "averageFare": "$avgFare",
            "_id": 0
        }}
    ]

    results = list(basic_collection.aggregate(pipeline))
    return convert_objectid_to_str(results)


# 支持跨集合查询
def join_flight_data(limit=100):
    """
    连接flights_basic和flights_segments集合的数据
    """
    print(f"连接 flights_basic 和 flights_segments 集合的数据")
    basic_collection = db["flights_basic"]

    # 使用originalId字段进行连接
    pipeline = [
        {"$lookup": {
            "from": "flights_segments",
            "localField": "originalId",
            "foreignField": "originalId",
            "as": "segmentDetails"
        }},
        {"$limit": limit}
    ]

    results = list(basic_collection.aggregate(pipeline))
    return convert_objectid_to_str(results)


# 数据修改操作
def insert_one(collection_name, document):
    """
    插入单个文档
    """
    collection = db[collection_name]
    result = collection.insert_one(document)
    return {
        "acknowledged": result.acknowledged,
        "inserted_id": str(result.inserted_id)
    }


def insert_many(collection_name, documents):
    """
    插入多个文档
    """
    collection = db[collection_name]
    result = collection.insert_many(documents)
    return {
        "acknowledged": result.acknowledged,
        "inserted_ids": [str(id) for id in result.inserted_ids],
        "inserted_count": len(result.inserted_ids)
    }


def update_one(collection_name, filter_query, update_query):
    """
    更新单个文档
    """
    collection = db[collection_name]
    result = collection.update_one(filter_query, update_query)
    return {
        "acknowledged": result.acknowledged,
        "matched_count": result.matched_count,
        "modified_count": result.modified_count
    }


def delete_one(collection_name, filter_query):
    """
    删除单个文档
    """
    collection = db[collection_name]
    result = collection.delete_one(filter_query)
    return {
        "acknowledged": result.acknowledged,
        "deleted_count": result.deleted_count
    }