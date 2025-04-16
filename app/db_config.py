import os

MONGO_URI = "mongodb+srv://flightsdata:dsci551@flightsdata.y57hp.mongodb.net/?retryWrites=true&w=majority"

MONGO_DB = "flights"    # 例如：如果在 Compass 显示是 “DSCI551_Project”
MONGO_COLLECTION = "DSCI551_Project"    # 如果 MongoDB 中集合名称是 flights

# SQLite 数据库路径，这里使用容器内部的路径（和 Dockerfile 中复制的路径保持一致）
SQLITE_DB_PATH = os.path.join(os.getcwd(), "hotel.db")
