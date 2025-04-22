import os
from sqlalchemy import create_engine

# MongoDB Configuration
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://flightsdata:dsci551@flightsdata.y57hp.mongodb.net/?retryWrites=true&w=majority",
)
MONGO_DB = "flightsdata"  # 修改为实际的数据库名称
MONGO_COLLECTIONS = {
    "flights": "flights_basic",  # Update to match what's used in mongo_agent.py
    "segments": "flights_segments",
}

# MongoDB local connection (as fallback)
MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
MONGO_PORT = os.environ.get("MONGO_PORT", "27017")
LOCAL_MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"

# SQLite Configuration
SQLITE_DB_DIR = os.environ.get("SQLITE_DB_DIR", os.path.join(os.getcwd(), "data"))
LOCATION_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_location.db")
RATE_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_rate.db")

# Create SQLAlchemy engines for Gradio app
location_engine = create_engine(f"sqlite:///{LOCATION_DB_PATH}")
rate_engine = create_engine(f"sqlite:///{RATE_DB_PATH}")
