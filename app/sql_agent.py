import sqlite3
import os
import logging
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Path to SQLite databases - use environment variable if available
SQLITE_DB_DIR = os.environ.get("SQLITE_DB_DIR", os.path.join(os.getcwd(), "data"))
LOCATION_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_location.sql")
RATE_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_rate.sql")

# Create SQLAlchemy engines for Gradio app usage
location_engine = create_engine(f"sqlite:///{LOCATION_DB_PATH}")
rate_engine = create_engine(f"sqlite:///{RATE_DB_PATH}")

def get_connection(db_path):
    """
    Return a connection to the specified SQLite database
    """
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to SQLite database at {db_path}: {e}")
        raise

def execute_sql_query(db_path, query, params=()):
    """
    Execute a SQL query and return the results

    Args:
        db_path: Path to the SQLite database
        query: SQL query string
        params: Parameters for the query

    Returns:
        List of query results
    """
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.commit()
        return results
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise
    finally:
        if conn:
            conn.close()

def get_all_reviews():
    """
    Get all hotel reviews by joining the location and rate data
    """
    try:
        # Get hotel location data
        locations = execute_sql_query(LOCATION_DB_PATH,
                                     "SELECT hotel_id, hotel_name, county, state FROM hotel_locations;")

        # Get hotel rate data
        rates = execute_sql_query(RATE_DB_PATH,
                                 "SELECT hotel_id, rating, sleepquality, service, rooms, cleanliness, value FROM hotel_rates;")

        # Create a dictionary to map hotel_id to rates
        rate_dict = {r[0]: r[1:] for r in rates}

        # Join the data
        result = []
        for loc in locations:
            hotel_id = loc[0]
            if hotel_id in rate_dict:
                # Combine location and rate information
                # Format: rating, sleepquality, service, rooms, cleanliness, value, hotel_name, county, state
                result.append(rate_dict[hotel_id] + loc[1:])

        return result
    except Exception as e:
        logger.error(f"Error joining hotel data: {e}")
        raise

def get_reviews_by_county(county):
    """
    Get hotel reviews by county
    """
    try:
        # Get hotel location data filtered by county
        locations = execute_sql_query(LOCATION_DB_PATH,
                                     "SELECT hotel_id, hotel_name, county, state FROM hotel_locations WHERE county = ?;",
                                     (county,))

        # Get hotel rate data
        rates = execute_sql_query(RATE_DB_PATH,
                                 "SELECT hotel_id, rating, sleepquality, service, rooms, cleanliness, value FROM hotel_rates;")

        # Create a dictionary to map hotel_id to rates
        rate_dict = {r[0]: r[1:] for r in rates}

        # Join the data
        result = []
        for loc in locations:
            hotel_id = loc[0]
            if hotel_id in rate_dict:
                # Combine location and rate information
                result.append(rate_dict[hotel_id] + loc[1:])

        return result
    except Exception as e:
        logger.error(f"Error getting reviews by county: {e}")
        raise

