import sqlite3
import os
import logging
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLite database paths
SQLITE_DB_DIR = os.environ.get("SQLITE_DB_DIR", os.path.join(os.getcwd(), "data"))
LOCATION_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_location.db")
RATE_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_rate.db")

# Create SQLAlchemy engines
location_engine = create_engine(f"sqlite:///{LOCATION_DB_PATH}")
rate_engine = create_engine(f"sqlite:///{RATE_DB_PATH}")

def get_connection(db_path):
    """
    Return a connection to the specified SQLite database
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Returns dictionary-like objects
        return conn
    except Exception as e:
        logger.error(f"Error connecting to SQLite database {db_path}: {e}")
        raise

def execute_sql_query(db_path, query, params=(), fetch_all=True):
    """
    Execute SQL query and return results
    
    Args:
        db_path: Database file path
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

def create_views():
    """
    Create views connecting all tables
    """
    try:
        # 1. Create hotel view - connect all hotel name tables and location table
        # Check if view already exists
        hotel_view_check_query = "SELECT name FROM sqlite_master WHERE type='view' AND name='hotel_complete_view'"
        hotel_view_exists = execute_sql_query(LOCATION_DB_PATH, hotel_view_check_query)
        
        if not hotel_view_exists:
            # Create hotel view
            create_hotel_view_query = """
            CREATE VIEW IF NOT EXISTS hotel_complete_view AS
            -- From hotel_name1
            SELECT 
                h1.ID, 
                h1.hotel_name, 
                l.county, 
                l.state
            FROM hotel_name1 h1
            JOIN location l ON h1.ID = l.ID
            
            UNION ALL
            
            -- From hotel_name2 (note different column name)
            SELECT 
                h2.hotel AS ID, 
                h2.hotel_name, 
                l.county, 
                l.state
            FROM hotel_name2 h2
            JOIN location l ON h2.hotel = l.ID
            
            UNION ALL
            
            -- From hotel_name3
            SELECT 
                h3.ID, 
                h3.hotel_name, 
                l.county, 
                l.state
            FROM hotel_name3 h3
            JOIN location l ON h3.ID = l.ID
            """
            execute_sql_query(LOCATION_DB_PATH, create_hotel_view_query)
            logger.info("Successfully created hotel_complete_view")
        else:
            logger.info("hotel_complete_view already exists")
        
        # 2. Create rating view - in rate database
        # Check if view already exists
        rate_view_check_query = "SELECT name FROM sqlite_master WHERE type='view' AND name='rate_complete_view'"
        rate_view_exists = execute_sql_query(RATE_DB_PATH, rate_view_check_query)
        
        if not rate_view_exists:
            # Create rating view
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
            logger.info("Successfully created rate_complete_view")
        else:
            logger.info("rate_complete_view already exists")
            
        return True
    except Exception as e:
        logger.error(f"Error creating views: {e}")
        return False

# Create views when application starts
create_views_result = create_views()
if not create_views_result:
    logger.warning("Unable to create views, will use fallback query methods")

def get_all_hotels():
    """
    Get all hotel information (without ratings)
    """
    try:
        query = "SELECT DISTINCT ID, hotel_name, county, state FROM hotel_complete_view"
        return execute_sql_query(LOCATION_DB_PATH, query)
    except Exception as e:
        logger.error(f"Error getting all hotels: {e}")
        # Fallback method
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
            logger.error(f"Fallback method also failed: {e2}")
            raise

def get_hotel_name_fallback(hotel_id):
    """
    Fallback method: Get hotel name from hotel name tables
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
    Get all hotel reviews with names - using view connections
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
        
        # Try using joined query on view tables
        try:
            # Note: Cross-database queries aren't directly supported in SQLite, we need to implement at code level
            hotel_data = execute_sql_query(LOCATION_DB_PATH, "SELECT DISTINCT ID, hotel_name, county, state FROM hotel_complete_view")
            rate_data = execute_sql_query(RATE_DB_PATH, "SELECT * FROM rate_complete_view")
            
            # Create mapping from ID to hotel information
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
                        0,  # Placeholder for value column
                        hotel['hotel_name'],
                        hotel['county'],
                        hotel['state']
                    ))
            
            return result
        except Exception as e:
            logger.warning(f"View join query failed, using fallback method: {e}")
            
            # Get all hotels from view
            hotels = get_all_hotels()
            
            result = []
            for hotel in hotels:
                hotel_id = hotel['ID']
                
                # Get rating information
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
                        0,  # Placeholder for value column
                        hotel['hotel_name'],
                        hotel['county'],
                        hotel['state']
                    ))
            
            return result
    except Exception as e:
        logger.error(f"Error getting all reviews: {e}")
        raise

def get_reviews_by_county(county):
    """
    Get hotel reviews for a specific county - using view tables
    """
    try:
        # Get all hotels in the specified county from view
        county_query = "SELECT ID, hotel_name, county, state FROM hotel_complete_view WHERE county = ?"
        hotels = execute_sql_query(LOCATION_DB_PATH, county_query, (county,))
        
        # Get all hotel IDs
        hotel_ids = [hotel['ID'] for hotel in hotels]
        
        if not hotel_ids:
            return []  # If no hotels found, return empty list
        
        # Create mapping from ID to hotel information
        hotel_map = {hotel['ID']: hotel for hotel in hotels}
        
        # Get ratings for these hotels
        # Since SQLite doesn't support array parameters, we need to build a parameter list
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
                    0,  # Placeholder for value column
                    hotel['hotel_name'],
                    hotel['county'],
                    hotel['state']
                ))
        
        return result
    except Exception as e:
        logger.error(f"Error getting reviews by county {county}: {e}")
        raise

def get_reviews_by_state(state):
    """
    Get hotel reviews for a specific state - using view tables
    """
    try:
        # Get all hotels in the specified state from view
        state_query = "SELECT ID, hotel_name, county, state FROM hotel_complete_view WHERE state = ?"
        hotels = execute_sql_query(LOCATION_DB_PATH, state_query, (state,))
        
        # Get all hotel IDs
        hotel_ids = [hotel['ID'] for hotel in hotels]
        
        if not hotel_ids:
            return []  # If no hotels found, return empty list
        
        # Create mapping from ID to hotel information
        hotel_map = {hotel['ID']: hotel for hotel in hotels}
        
        # Get ratings for these hotels
        # Since SQLite doesn't support array parameters, we need to build a parameter list
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
                    0,  # Placeholder for value column
                    hotel['hotel_name'],
                    hotel['county'],
                    hotel['state']
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
        # Get all hotels from view
        hotels_query = "SELECT DISTINCT ID, state FROM hotel_complete_view ORDER BY state"
        hotels = execute_sql_query(LOCATION_DB_PATH, hotels_query)
        
        # Group by state
        state_ratings = {}
        for hotel in hotels:
            hotel_id = hotel['ID']
            state = hotel['state']
            
            # Get rating information
            rate_query = "SELECT rating FROM rate WHERE ID = ?"
            rate = execute_sql_query(RATE_DB_PATH, rate_query, (hotel_id,), fetch_all=False)
            
            if rate:
                if state not in state_ratings:
                    state_ratings[state] = {'sum': 0, 'count': 0}
                
                state_ratings[state]['sum'] += rate['rating']
                state_ratings[state]['count'] += 1
        
        # Calculate averages
        result = []
        for state, data in state_ratings.items():
            avg_rating = data['sum'] / data['count'] if data['count'] > 0 else 0
            result.append({'state': state, 'avg_rating': avg_rating, 'hotel_count': data['count']})
        
        # Sort by average rating (highest first)
        result.sort(key=lambda x: x['avg_rating'], reverse=True)
        
        return result
    except Exception as e:
        logger.error(f"Error getting average ratings by state: {e}")
        raise

def count_hotels_by_state():
    """
    Count number of hotels per state
    """
    try:
        query = "SELECT state, COUNT(DISTINCT ID) as count FROM hotel_complete_view GROUP BY state ORDER BY count DESC"
        return execute_sql_query(LOCATION_DB_PATH, query)
    except Exception as e:
        logger.error(f"Error counting hotels by state: {e}")
        raise

def find_hotels_with_min_rating(min_rating):
    """
    Find hotels with rating at least the specified value - using view tables
    """
    try:
        # First get hotel IDs and rating information with ratings above the specified value
        rate_query = f"SELECT * FROM rate_complete_view WHERE rating >= {min_rating}"
        rate_matches = execute_sql_query(RATE_DB_PATH, rate_query)
        
        # Get all qualifying hotel IDs
        hotel_ids = [rate['ID'] for rate in rate_matches]
        
        if not hotel_ids:
            return []  # If no hotels found, return empty list
        
        # Create mapping from ID to rate information
        rate_map = {rate['ID']: rate for rate in rate_matches}
        
        # Get information for these hotels
        # Since SQLite doesn't support array parameters, we need to build a parameter list
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
                    0,  # Placeholder for value column
                    hotel['hotel_name'],
                    hotel['county'],
                    hotel['state']
                ))
        
        return result
    except Exception as e:
        logger.error(f"Error finding hotels with minimum rating {min_rating}: {e}")
        raise

def execute_custom_query(query, is_location_db=True):
    """
    Execute custom SQL query
    """
    db_path = LOCATION_DB_PATH if is_location_db else RATE_DB_PATH
    return execute_sql_query(db_path, query)
