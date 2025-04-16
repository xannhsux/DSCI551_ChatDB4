import os
from sqlalchemy import create_engine

# MongoDB Configuration
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://flightsdata:dsci551@flightsdata.y57hp.mongodb.net/?retryWrites=true&w=majority")
MONGO_DB = "flights"
MONGO_COLLECTION = "DSCI551_Project"

# MongoDB local connection (as fallback)
MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
MONGO_PORT = os.environ.get("MONGO_PORT", "27017")
LOCAL_MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"

# SQLite Configuration
SQLITE_DB_PATH = os.environ.get("SQLITE_DB_PATH", os.path.join(os.getcwd(), "hotel.db"))

# Create SQLAlchemy engine for Gradio app
sql_engine = create_engine(f"sqlite:///{SQLITE_DB_PATH}")