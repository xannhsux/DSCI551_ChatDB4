from flask import Flask, jsonify, request
from mongo_agent import get_all_flights, get_flights_by_airports, get_flights_by_airline
from sql_agent import get_all_reviews, get_reviews_by_county, insert_review

app = Flask(__name__)

# === MongoDB 接口部分 (保持不变) ===
@app.route("/flights", methods=["GET"])
def all_flights():
    return jsonify(get_all_flights())

@app.route("/flights/route", methods=["GET"])
def flights_by_route():
    starting = request.args.get("from")
    destination = request.args.get("to")
    return jsonify(get_flights_by_airports(starting, destination))

@app.route("/flights/airline/<airline>", methods=["GET"])
def flights_by_airline(airline):
    return jsonify(get_flights_by_airline(airline))

# === SQLite 接口部分 (hotel_reviews 表) ===
# 获取所有酒店评价记录
@app.route("/hotelreviews", methods=["GET"])
def all_hotel_reviews():
    reviews = get_all_reviews()
    return jsonify(reviews)

# 根据 county 查询酒店评价记录
@app.route("/hotelreviews/county", methods=["GET"])
def hotel_reviews_by_county():
    county = request.args.get("county")
    reviews = get_reviews_by_county(county)
    return jsonify(reviews)

# 插入新的酒店评价记录（POST）
@app.route("/hotelreviews", methods=["POST"])
def add_hotel_review():
    data = request.get_json()
    # 根据你的 sql_agent.py 函数定义，这里需要传入 9 个字段：
    rating = data.get("rating")
    sleepquality = data.get("sleepquality")
    service = data.get("service")
    rooms = data.get("rooms")
    cleanliness = data.get("cleanliness")
    value = data.get("value")
    hotel_name = data.get("hotel_name")
    county = data.get("county")
    state = data.get("state")
    new_id = insert_review(rating, sleepquality, service, rooms, cleanliness, value, hotel_name, county, state)
    return jsonify({"new_id": new_id})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8100)
