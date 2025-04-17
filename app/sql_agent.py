import sqlite3
import os
import logging
from sqlalchemy import create_engine

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Path to SQLite database - use environment variable if available
SQLITE_DB_PATH = os.environ.get("SQLITE_DB_PATH", os.path.join(os.getcwd(), "hotel.db"))

# Create SQLAlchemy engine for Gradio app usage
sql_engine = create_engine(f"sqlite:///{SQLITE_DB_PATH}")

def get_connection():
    """
    Return a connection to the SQLite database
    """
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to SQLite database: {e}")
        raise

def execute_sql_query(query, params=()):
    """
    Execute a SQL query and return the results
    
    Args:
        query: SQL query string
        params: Parameters for the query
        
    Returns:
        List of query results
    """
    try:
        conn = get_connection()
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
    Get all hotel reviews
    """
    query = "SELECT * FROM hotel_reviews;"
    return execute_sql_query(query)

def get_reviews_by_county(county):
    """
    Get hotel reviews by county
    """
    query = "SELECT * FROM hotel_reviews WHERE county = ?;"
    return execute_sql_query(query, (county,))

def get_reviews_by_state(state):
    """
    Get hotel reviews by state
    """
    query = "SELECT * FROM hotel_reviews WHERE state = ?;"
    return execute_sql_query(query, (state,))

def insert_review(rating, sleepquality, service, rooms, cleanliness, value, hotel_name, county, state):
    """
    Insert a new hotel review
    """
    query = """
    INSERT INTO hotel_reviews
    (rating, sleepquality, service, rooms, cleanliness, value, hotel_name, county, state)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query, (rating, sleepquality, service, rooms, cleanliness, value, hotel_name, county, state))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        logger.error(f"Error inserting review: {e}")
        raise
    finally:
        if conn:
            conn.close()

def update_review_rating(row_id, new_rating):
    """
    Update a review's rating
    """
    query = "UPDATE hotel_reviews SET rating = ? WHERE rowid = ?;"
    return execute_sql_query(query, (new_rating, row_id))

def delete_review(row_id):
    """
    Delete a review
    """
    query = "DELETE FROM hotel_reviews WHERE rowid = ?;"
    return execute_sql_query(query, (row_id,))