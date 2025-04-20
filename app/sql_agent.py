import sqlite3
import os
import logging
from sqlalchemy import create_engine, text
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#  Path to SQLite databases - use environment variable if available
SQLITE_DB_DIR = os.environ.get("SQLITE_DB_DIR", os.path.join(os.getcwd(), "data"))
LOCATION_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_location.db")
RATE_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_rate.db")

# Create SQLAlchemy engines for Gradio app usage
location_engine = create_engine(f"sqlite:///{LOCATION_DB_PATH}")
rate_engine = create_engine(f"sqlite:///{RATE_DB_PATH}")

def get_connection(db_path):
    """
    Return a connection to the specified SQLite database with row_factory
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # This makes it return dictionary-like objects
        return conn
    except Exception as e:
        logger.error(f"Error connecting to SQLite database at {db_path}: {e}")
        raise

def execute_sql_query(db_path, query, params=(), fetch_all=True):
    """
    Execute SQL query and return results
    
    Args:
        db_path: Path to the database file
        query: SQL query string
        params: Query parameters
        fetch_all: Whether to fetch all results, False fetches only one row
        
    Returns:
        Query results list
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
        logger.error(f"Error executing query on {db_path}: {e}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        raise
    finally:
        if conn:
            conn.close()

def get_hotel_name(hotel_id):
    """
    Get hotel name from hotel name tables
    """
    hotel_name_tables = ['hotel_name1', 'hotel_name2', 'hotel_name3']
    
    for table in hotel_name_tables:
        try:
            query = f"SELECT hotel_name FROM {table} WHERE hotel = ?"
            result = execute_sql_query(LOCATION_DB_PATH, query, (hotel_id,), fetch_all=False)
            if result:
                return result[0]
        except Exception:
            continue
    
    return "Unknown Hotel"

def get_all_reviews():
    """
    Get all hotel reviews with names
    """
    try:
        query = """
        SELECT 
            r.ID, 
            r.rating, 
            r.sleepquality, 
            r.service, 
            r.rooms, 
            r.cleanliness, 
            r.value
        FROM rate r
        """
        rates = execute_sql_query(RATE_DB_PATH, query)
        
        result = []
        for rate in rates:
            hotel_name = get_hotel_name(rate['ID'])
            
            # Find location
            loc_query = """
            SELECT county, state 
            FROM location 
            WHERE ID = ?
            """
            location = execute_sql_query(LOCATION_DB_PATH, loc_query, (rate['ID'],), fetch_all=False)
            
            county = location['county'] if location else 'Unknown'
            state = location['state'] if location else 'Unknown'
            
            result.append((
                rate['rating'],
                rate['sleepquality'],
                rate['service'],
                rate['rooms'],
                rate['cleanliness'],
                rate['value'],
                hotel_name,
                county,
                state
            ))
        
        return result
    except Exception as e:
        logger.error(f"Error getting all reviews: {e}")
        raise

def get_reviews_by_county(county):
    """
    Get hotels by county
    """
    try:
        query = """
        SELECT ID, county, state 
        FROM location 
        WHERE county = ?
        """
        locations = execute_sql_query(LOCATION_DB_PATH, query, (county,))
        
        result = []
        for loc in locations:
            # Get rate information
            rate_query = """
            SELECT 
                rating, 
                sleepquality, 
                service, 
                rooms, 
                cleanliness, 
                value
            FROM rate
            WHERE ID = ?
            """
            rate = execute_sql_query(RATE_DB_PATH, rate_query, (loc['ID'],), fetch_all=False)
            
            if rate:
                hotel_name = get_hotel_name(loc['ID'])
                
                result.append((
                    rate['rating'],
                    rate['sleepquality'],
                    rate['service'],
                    rate['rooms'],
                    rate['cleanliness'],
                    rate['value'],
                    hotel_name,
                    loc['county'],
                    loc['state']
                ))
        
        return result
    except Exception as e:
        logger.error(f"Error getting reviews by county {county}: {e}")
        raise

def get_reviews_by_state(state):
    """
    Get hotels by state
    """
    try:
        query = """
        SELECT ID, county, state 
        FROM location 
        WHERE state = ?
        """
        locations = execute_sql_query(LOCATION_DB_PATH, query, (state,))
        
        result = []
        for loc in locations:
            # Get rate information
            rate_query = """
            SELECT 
                rating, 
                sleepquality, 
                service, 
                rooms, 
                cleanliness
            FROM rate
            WHERE ID = ?
            """
            rate = execute_sql_query(RATE_DB_PATH, rate_query, (loc['ID'],), fetch_all=False)
            
            if rate:
                hotel_name = get_hotel_name(loc['ID'])
                
                result.append((
                    rate['rating'],
                    rate['sleepquality'],
                    rate['service'],
                    rate['rooms'],
                    rate['cleanliness'],
                    0,  # Placeholder for removed 'value' column
                    hotel_name,
                    loc['county'],
                    loc['state']
                ))
        
        return result
    except Exception as e:
        logger.error(f"Error getting reviews by state {state}: {e}")
        raise

def get_average_ratings_by_state():
    """
    Get average ratings for hotels in each state
    """
    try:
        query = """
        SELECT 
            l.state, 
            AVG(r.rating) as avg_rating, 
            COUNT(*) as hotel_count
        FROM location l
        JOIN rate r ON l.ID = r.ID
        GROUP BY l.state
        ORDER BY avg_rating DESC
        """
        return execute_sql_query(LOCATION_DB_PATH, query)
    except Exception as e:
        logger.error(f"Error getting average ratings by state: {e}")
        raise

def count_hotels_by_state():
    """
    Count number of hotels per state
    """
    try:
        query = """
        SELECT state, COUNT(*) as count 
        FROM location 
        GROUP BY state 
        ORDER BY count DESC
        """
        return execute_sql_query(LOCATION_DB_PATH, query)
    except Exception as e:
        logger.error(f"Error counting hotels by state: {e}")
        raise

def find_hotels_with_min_rating(min_rating):
    """
    Find hotels with rating at least the specified value
    """
    try:
        query = """
        SELECT ID 
        FROM rate 
        WHERE rating >= ?
        """
        rate_matches = execute_sql_query(RATE_DB_PATH, query, (min_rating,))
        
        result = []
        for rate in rate_matches:
            # Get location info
            loc_query = """
            SELECT county, state 
            FROM location 
            WHERE ID = ?
            """
            loc = execute_sql_query(LOCATION_DB_PATH, loc_query, (rate['ID'],), fetch_all=False)
            
            # Get hotel name
            hotel_name = get_hotel_name(rate['ID'])
            
            # Get full rate details
            full_rate_query = """
            SELECT 
                rating, 
                sleepquality, 
                service, 
                rooms, 
                cleanliness, 
                value
            FROM rate
            WHERE ID = ?
            """
            full_rate = execute_sql_query(RATE_DB_PATH, full_rate_query, (rate['ID'],), fetch_all=False)
            
            result.append((
                full_rate['rating'],
                full_rate['sleepquality'],
                full_rate['service'],
                full_rate['rooms'],
                full_rate['cleanliness'],
                full_rate['value'],
                hotel_name,
                loc['county'],
                loc['state']
            ))
        
        return result
    except Exception as e:
        logger.error(f"Error finding hotels with min rating {min_rating}: {e}")
        raise

# 其他函数保持不变，如有需要可以进一步调整
# 如 insert_hotel, update_hotel_rating, delete_hotel 等
# 需要注意处理多个 hotel_name 表的逻辑

def execute_custom_query(query, is_location_db=True):
    """
    Execute custom SQL query
    """
    db_path = LOCATION_DB_PATH if is_location_db else RATE_DB_PATH
    return execute_sql_query(db_path, query)