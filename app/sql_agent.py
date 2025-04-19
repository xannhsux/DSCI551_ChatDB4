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

def get_reviews_by_state(state):
    """
    Get hotel reviews by state
    """
    try:
        # Get hotel location data filtered by state
        locations = execute_sql_query(LOCATION_DB_PATH, 
                                     "SELECT hotel_id, hotel_name, county, state FROM hotel_locations WHERE state = ?;",
                                     (state,))
        
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
        logger.error(f"Error getting reviews by state: {e}")
        raise

def insert_review(rating, sleepquality, service, rooms, cleanliness, value, hotel_name, county, state):
    """
    Insert a new hotel review (this will require inserting into both tables)
    """
    try:
        # First, insert the location data and get the new hotel_id
        conn_loc = get_connection(LOCATION_DB_PATH)
        cursor_loc = conn_loc.cursor()
        cursor_loc.execute(
            "INSERT INTO hotel_locations (hotel_name, county, state) VALUES (?, ?, ?);",
            (hotel_name, county, state)
        )
        hotel_id = cursor_loc.lastrowid
        conn_loc.commit()
        conn_loc.close()
        
        # Then, insert the rate data
        conn_rate = get_connection(RATE_DB_PATH)
        cursor_rate = conn_rate.cursor()
        cursor_rate.execute(
            "INSERT INTO hotel_rates (hotel_id, rating, sleepquality, service, rooms, cleanliness, value) VALUES (?, ?, ?, ?, ?, ?, ?);",
            (hotel_id, rating, sleepquality, service, rooms, cleanliness, value)
        )
        conn_rate.commit()
        conn_rate.close()
        
        return hotel_id
    except Exception as e:
        logger.error(f"Error inserting review: {e}")
        raise

def update_review_rating(hotel_id, new_rating):
    """
    Update a review's rating
    """
    query = "UPDATE hotel_rates SET rating = ? WHERE hotel_id = ?;"
    return execute_sql_query(RATE_DB_PATH, query, (new_rating, hotel_id))

def delete_review(hotel_id):
    """
    Delete a review (needs to delete from both tables)
    """
    try:
        # First delete from rates table
        execute_sql_query(RATE_DB_PATH, "DELETE FROM hotel_rates WHERE hotel_id = ?;", (hotel_id,))
        
        # Then delete from locations table
        execute_sql_query(LOCATION_DB_PATH, "DELETE FROM hotel_locations WHERE hotel_id = ?;", (hotel_id,))
        
        return True
    except Exception as e:
        logger.error(f"Error deleting review: {e}")
        raise