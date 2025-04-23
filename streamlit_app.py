import streamlit as st
import pandas as pd
import os
import sqlite3
import re
import requests
import json
from schema_display import get_sql_schema, get_mongo_schema, get_complete_schema

# Set page config
st.set_page_config(page_title="Travel Database Query", page_icon="✈️", layout="wide")
st.title("Travel Information System")

# Database path constants
SQLITE_DB_DIR = "./data"
LOCATION_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_location.db")
RATE_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_rate.db")
API_URL = os.environ.get("API_URL", "http://backend:8000")
OLLAMA_API = os.environ.get("OLLAMA_HOST", "http://ollama:11434")

# Initialize session state variables if they don't exist
if 'query' not in st.session_state:
    st.session_state.query = ""
if 'query_type' not in st.session_state:
    st.session_state.query_type = "sql"

def format_flights_as_df(flights):
    """
    Format flight data as pandas DataFrame for display
    
    Args:
        flights: List of flight dictionaries
        
    Returns:
        Pandas DataFrame with formatted flight data
    """
    if not flights:
        return pd.DataFrame()
    
    # Create a list of dictionaries with standardized fields
    formatted_flights = []
    
    # Don't try to fetch individual segment data since it's causing 404 errors
    # Instead, use the airline information if available directly in the flight data
    
    for flight in flights:
        # Get airline information from segmentsAirlineName if available
        airline = flight.get("segmentsAirlineName", "N/A")
        
        # If we have multiple airlines separated by "||", format them nicely
        if airline != "N/A" and "||" in airline:
            airline = airline.replace("||", " / ")
        
        # Format price with dollar sign
        price = flight.get("totalFare", "N/A")
        if price != "N/A":
            price = f"${price}"
        
        # Handle duration format
        duration = flight.get("totalTripDuration", flight.get("travelDuration", "N/A"))
        
        formatted_flights.append({
            "Departure Airport": flight.get("startingAirport", "N/A"),
            "Destination Airport": flight.get("destinationAirport", "N/A"),
            "Airline": airline,
            "Price": price,
            "Duration (min)": duration
        })
    
    return pd.DataFrame(formatted_flights)


# Function to process natural language queries for flights
def process_flight_nl_query(nl_query):
    """Process natural language query for flights and return MongoDB query, query type, and parameters"""
    query_lower = nl_query.lower()
    
    # Default values
    mongo_query = "db.flights.find({}).limit(10)"
    query_type = "all_flights"
    params = {}
    
    # Check for origin-destination pattern
    if ("from" in query_lower and "to" in query_lower) or ("between" in query_lower and "and" in query_lower):
        # Extract airport codes - this is a simplified implementation
        # In production, you'd want more sophisticated NLP
        words = query_lower.split()
        for i, word in enumerate(words):
            if word == "from" and i+1 < len(words):
                params["starting"] = words[i+1].upper()
            if word == "to" and i+1 < len(words):
                params["destination"] = words[i+1].upper()
        
        if "starting" in params and "destination" in params:
            mongo_query = f"""db.flights.find({{
                "startingAirport": "{params['starting']}", 
                "destinationAirport": "{params['destination']}"
            }}).sort({{ "totalFare": 1 }})"""
            query_type = "by_airports"
    
    # Check for airline pattern
    elif any(airline in query_lower for airline in ["delta", "american", "united", "southwest", "airlines", "airways"]):
        # Extract airline name - simplified implementation
        airline_keywords = ["delta", "american", "united", "southwest", "jetblue", "frontier"]
        for keyword in airline_keywords:
            if keyword in query_lower:
                params["airline"] = keyword
                break
        
        if "airline" in params:
            mongo_query = f"""db.flights_segments.find({{
                "segmentsAirlineName": {{ "$regex": "{params['airline']}", "$options": "i" }}
            }})"""
            query_type = "by_airline"
    
    return mongo_query, query_type, params

# Function to process natural language queries for hotels
def process_hotel_nl_query(nl_query):
    """Process natural language query for hotels and return SQL query and parameters"""
    query_lower = nl_query.lower()
    params = {}
    
    # Extract county information
    county_match = re.search(r'(in|from) ([a-z]+) county', query_lower)
    if county_match:
        params["county"] = county_match.group(2).title()
    
    # Extract state information
    state_match = re.search(r'(in|from) ([a-z]+)(,| state)', query_lower)
    if state_match:
        params["state"] = state_match.group(2).title()
    
    # Extract rating information
    rating_match = re.search(r'(rating|rated) (above|over|higher than) (\d+\.?\d*)', query_lower)
    if rating_match:
        params["min_rating"] = float(rating_match.group(3))
    
    # Generate appropriate SQL query based on extracted parameters
    if "county" in params and "state" in params and "min_rating" in params:
        sql_query = f"""
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.county = '{params["county"]}' AND h.state = '{params["state"]}' AND r.rating >= {params["min_rating"]}
        ORDER BY r.rating DESC
        """
    elif "county" in params and "state" in params:
        sql_query = f"""
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.county = '{params["county"]}' AND h.state = '{params["state"]}'
        ORDER BY r.rating DESC
        """
    elif "county" in params:
        sql_query = f"""
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.county = '{params["county"]}'
        ORDER BY r.rating DESC
        """
    elif "state" in params:
        sql_query = f"""
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.state = '{params["state"]}'
        ORDER BY r.rating DESC
        """
    elif "min_rating" in params:
        sql_query = f"""
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE r.rating >= {params["min_rating"]}
        ORDER BY r.rating DESC
        """
    else:
        sql_query = """
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        ORDER BY r.rating DESC
        LIMIT 10
        """
    
    return sql_query, params

# Generate SQL query function (simplified version)
def generate_sql_query(natural_query):
    query_lower = natural_query.lower()

    # Check for schema requests
    if "select * from" in query_lower or "schema" in query_lower or "show tables" in query_lower:
        if "hotel_location" in query_lower or "location" in query_lower:
            return """
            SELECT * FROM hotel_complete_view LIMIT 50
            """
        elif "hotel_rate" in query_lower or "rate" in query_lower:
            return """
            SELECT * FROM rate_complete_view LIMIT 50
            """
        else:
            return """
            SELECT h.ID, h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
            FROM hotel_complete_view h
            JOIN rate_complete_view r ON h.ID = r.ID
            LIMIT 50
            """

    # Check for specific queries
    if "orange county" in query_lower:
        return """
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.county = 'Orange'
        ORDER BY r.rating DESC
        """
    elif "california" in query_lower:
        return """
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.state = 'CA'
        ORDER BY r.rating DESC
        """
    elif "best rating" in query_lower or "highest rating" in query_lower:
        return """
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        ORDER BY r.rating DESC
        LIMIT 20
        """
    elif "cleanliness" in query_lower:
        return """
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        ORDER BY r.cleanliness DESC
        LIMIT 20
        """

    # Default query
    return """
    SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r.rooms
    FROM hotel_complete_view h
    JOIN rate_complete_view r ON h.ID = r.ID
    LIMIT 10
    """

# Generate MongoDB query function
# Function to call Ollama API for generating MongoDB queries from natural language
def generate_mongo_query(natural_language_query):
    """
    Generate MongoDB query from natural language using Ollama API

    Args:
        natural_language_query (str): The natural language query from the user

    Returns:
        str: The generated MongoDB query
    """
    try:
        # Create a more effective prompt with examples
        prompt = f"""
        You are a MongoDB query generator. Your task is to convert natural language queries into MongoDB queries for a flight database.

        Natural Language Query: "{natural_language_query}"

        Database Structure:
        - flights_basic collection: contains fields startingAirport, destinationAirport, totalFare, travelDuration
        - flights_segments collection: contains fields originalId, segmentsAirlineName

        Example Conversions:
        - "Find flights from SFO" → db.flights_basic.find({{"startingAirport": "SFO"}}).limit(20)
        - "Show Delta Airlines flights" → db.flights_segments.find({{"segmentsAirlineName": {{"$regex": "Delta", "$options": "i"}}}}).limit(20)
        - "Find flights from LAX to JFK" → db.flights_basic.find({{"startingAirport": "LAX", "destinationAirport": "JFK"}}).limit(20)
        - "What are the cheapest flights?" → db.flights_basic.find({{}}).sort({{"totalFare": 1}}).limit(20)

        Return ONLY the valid MongoDB query, no explanation.
        """

        # Call Ollama API with optimized parameters
        response = requests.post(
            f"{OLLAMA_API}/api/generate",
            json={
                "model": "llama3",  # Use llama3 as shown in the logs
                "prompt": prompt,
                "stream": False,
                "temperature": 0.1,  # Low temperature for more predictable results
                "top_p": 0.9,  # Focus on more likely tokens
                "stop": ["\n", ";"]  # Stop generation at newlines or semicolons
            }
        )

        if response.status_code == 200:
            result = response.json()
            generated_query = result.get("response", "").strip()

            # Clean up the generated query
            # Remove any markdown code block formatting
            generated_query = generated_query.replace("```javascript", "").replace("```", "").strip()

            # Basic validation of the generated query
            if not generated_query.startswith("db."):
                # If no valid query was generated, create a basic query
                st.warning("The model didn't generate a valid MongoDB query. Using a default query.")
                generated_query = "db.flights_basic.find({}).limit(20)"

            # Make sure it has a limit
            if "limit" not in generated_query:
                # Add limit before the closing parenthesis if not present
                if generated_query.endswith(")"):
                    generated_query = generated_query[:-1] + ".limit(20))"
                else:
                    generated_query = generated_query + ".limit(20)"

            return generated_query
        else:
            st.error(f"Error calling Ollama API: {response.status_code} - {response.text}")
            return "db.flights_basic.find({}).limit(20)"  # Default fallback query

    except Exception as e:
        st.error(f"Error generating MongoDB query: {str(e)}")
        return "db.flights_basic.find({}).limit(20)"  # Default fallback query


# Function to parse the MongoDB query into components for API call
def parse_mongo_query(query_string):
    """
    Parse a MongoDB query string into components needed for API call

    Args:
        query_string (str): The MongoDB query string

    Returns:
        tuple: (query_type, params, mongo_query)
    """
    # Default values
    query_type = "mongo_query"  # Use mongo_query as type for direct execution
    params = {"limit": 20, "mongo_query": query_string}  # Include original query string

    try:
        # Extract collection name
        collection_match = re.search(r'db\.(\w+)\.', query_string)
        collection = collection_match.group(1) if collection_match else "flights_basic"

        # Map collection names to match backend expectations
        if collection == "flights_basic":
            # Update the query to use "flights" instead
            query_string = query_string.replace("db.flights_basic", "db.flights")
            params["mongo_query"] = query_string
            collection = "flights"
        elif collection == "flights_segments":
            # Update the query to use "segments" instead
            query_string = query_string.replace("db.flights_segments", "db.segments")
            params["mongo_query"] = query_string
            collection = "segments"

        # Extract query parameters
        query_params_match = re.search(r'find\(\s*(\{.*?\})\s*\)', query_string)
        query_params = {}

        if query_params_match:
            # Try to parse the query parameters
            params_str = query_params_match.group(1)

            # Handle advanced regex patterns in the query
            # Replace any single quotes with double quotes for valid JSON
            params_str = params_str.replace("'", '"')

            try:
                query_params = json.loads(params_str)
            except json.JSONDecodeError:
                # If we can't parse the JSON, try to extract keys and values manually
                st.warning(f"Could not parse query parameters as JSON: {params_str}")
                query_params = {}

        # Extract limit
        limit_match = re.search(r'\.limit\((\d+)\)', query_string)
        limit = int(limit_match.group(1)) if limit_match else 20
        params["limit"] = limit

        # Determine query type based on parameters - this helps our API routing
        if "startingAirport" in query_params and "destinationAirport" in query_params:
            query_type = "by_airports"
            params["starting"] = query_params["startingAirport"]
            params["destination"] = query_params["destinationAirport"]
        elif "segmentsAirlineName" in query_params or collection == "segments":
            query_type = "by_airline"
            # Try to extract airline name from regex pattern if present
            if isinstance(query_params.get("segmentsAirlineName"), dict):
                regex = query_params["segmentsAirlineName"].get("$regex", "")
                params["airline"] = regex
            else:
                params["airline"] = query_params.get("segmentsAirlineName", "")

        # Add collection name to params
        params["collection"] = collection

        return query_type, params, query_string

    except Exception as e:
        st.error(f"Error parsing MongoDB query: {str(e)}")
        return "mongo_query", {"limit": 20, "mongo_query": query_string, "collection": "flights"}, query_string


def execute_mongo_query(query_string):
    """
    Execute a MongoDB query directly against the flights database

    Args:
        query_string (str): MongoDB query string (e.g., "db.flights.find({}).limit(10)")

    Returns:
        list: Results of the query
    """
    # Extract collection name from the query
    import re
    from pymongo import MongoClient

    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['flights']  # Connect to flights database

    # Extract collection name from query
    collection_match = re.search(r'db\.(\w+)\.', query_string)
    collection_name = collection_match.group(1) if collection_match else "flights"

    # Map collection names to actual collections in database
    if collection_name == "flights_basic":
        collection = db['flights']
    elif collection_name == "flights_segments":
        collection = db['segments']
    else:
        collection = db[collection_name]

    # Extract query parameters using regex
    find_pattern = r'find\(\s*(\{.*?\})\s*\)'
    find_match = re.search(find_pattern, query_string)

    if find_match:
        query_json = find_match.group(1)
        import json
        query_params = json.loads(query_json)
    else:
        query_params = {}

    # Extract limit if present
    limit_pattern = r'\.limit\((\d+)\)'
    limit_match = re.search(limit_pattern, query_string)
    limit = int(limit_match.group(1)) if limit_match else 100

    # Extract sort if present
    sort_pattern = r'\.sort\(\s*(\{.*?\})\s*\)'
    sort_match = re.search(sort_pattern, query_string)

    if sort_match:
        sort_json = sort_match.group(1)
        sort_params = json.loads(sort_json)

        # Get the field name and sort order
        sort_field = list(sort_params.keys())[0]
        sort_order = sort_params[sort_field]

        # Execute find with sort and limit
        results = list(collection.find(query_params).sort(sort_field, sort_order).limit(limit))
    else:
        # Execute find with just limit
        results = list(collection.find(query_params).limit(limit))

    # Close connection
    client.close()

    # Convert ObjectIds to strings for JSON serialization
    from bson import json_util
    return json.loads(json_util.dumps(results))


def get_flights_by_direct_api(query_type, params=None):
    """
    Directly query backend API endpoints instead of using MongoDB query execution
    """
    if params is None:
        params = {}

    try:
        # Handle direct MongoDB query execution
        if query_type == "mongo_query" and "mongo_query" in params:
            # Execute the MongoDB query directly through our new endpoint
            payload = {
                "collection": params.get("collection", "flights"),
                "query": params["mongo_query"]
            }
            response = requests.post(
                f"{API_URL}/execute_mongo_query",
                json=payload
            )
            if st.sidebar.checkbox("Show Debug Info", key="debug_info_mongo"):
                st.write(f"Debug: Using /execute_mongo_query endpoint with query: {params['mongo_query']}")

            if response.status_code == 200:
                flights = response.json()
                return flights
            else:
                st.error(f"API Error: {response.status_code} - {response.text}")
                return []

        # Select the appropriate endpoint based on query type
        if query_type == "by_airports" and "starting" in params and "destination" in params:
            # Add a limit parameter to avoid performance issues
            limit = params.get("limit", 20)

            # Query for flights between specific airports
            response = requests.get(
                f"{API_URL}/flights/airports",
                params={
                    "starting": params["starting"],
                    "destination": params["destination"],
                    "limit": limit  # Add limit to query parameters
                }
            )
            if st.sidebar.checkbox("Show Debug Info", key="debug_info_airports"):
                st.write(f"Debug: Using /flights/airports endpoint with params: {{'starting': '{params['starting']}', 'destination': '{params['destination']}', 'limit': {limit}}}")

        elif query_type == "by_airline" and "airline" in params:
            # Add a limit parameter to avoid performance issues
            limit = params.get("limit", 20)

            # Query for flights by airline
            response = requests.get(
                f"{API_URL}/flights/airline",
                params={"airline": params["airline"], "limit": limit}
            )
            if st.sidebar.checkbox("Show Debug Info", key="debug_info_airline"):
                st.write(f"Debug: Using /flights/airline endpoint with params: {{'airline': '{params['airline']}', 'limit': {limit}}}")

        else:
            # Default to getting all flights with limit
            limit = params.get("limit", 20)
            response = requests.get(f"{API_URL}/flights", params={"limit": limit})
            if st.sidebar.checkbox("Show Debug Info", key="debug_info_all"):
                st.write(f"Debug: Using /flights endpoint with limit: {limit}")

        if response.status_code == 200:
            flights = response.json()
            result_count = len(flights)
            if st.sidebar.checkbox("Show Debug Info", key="debug_info_results"):
                st.write(f"Debug: Received {result_count} flights from API")

            # If we got too many results, limit them to avoid performance issues
            if result_count > 100:
                st.warning(f"Found {result_count} matching flights. Showing first 100 results.")
                flights = flights[:100]  # Limit to first 100 flights

            return flights
        else:
            st.error(f"API Error: {response.status_code} - {response.text}")
            return []

    except Exception as e:
        st.error(f"Error querying API: {str(e)}")
        return []

# Handle HTTP errors with custom message and status code
def handle_api_error(response, operation="query"):
    """Handle API error responses with appropriate error messages"""
    try:
        error_message = response.json().get("detail", response.text)
    except:
        error_message = response.text if response.text else f"Error: {response.status_code}"
    
    operation_messages = {
        "query": "executing query",
        "add": "adding record",
        "update": "updating record",
        "delete": "deleting record",
        "search": "searching records"
    }
    
    operation_text = operation_messages.get(operation, "performing operation")
    return f"Error {operation_text}: {error_message}"

# Create tabs
tab1, tab2, tab3 = st.tabs(["Natural Language Query", "Database Schema", "Data Modification"])

# Natural Language Query tab
with tab1:
    st.header("Natural Language Query Interface")

    # Example queries
    st.markdown("### Example Queries")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Hotel Query Examples")
        hotel_examples = [
            "Show me hotels in Orange County with ratings above 4.5",
            "Find hotels with the best cleanliness ratings",
            "What are the top rated hotels in California?"
        ]

        for ex in hotel_examples:
            if st.button(ex, key=f"hotel_{ex}"):
                st.session_state.query = ex
                st.session_state.query_type = "sql"

    with col2:
        st.subheader("Flight Query Examples")
        flight_examples = [
            "Find flights from LAX to JFK",
            "Show me all Delta Airlines flights",
            "What are the cheapest flights?",
            "Find flights departing from SFO"
        ]

        for ex in flight_examples:
            if st.button(ex, key=f"flight_{ex}"):
                st.session_state.query = ex
                st.session_state.query_type = "mongo"

    with col3:
        st.subheader("Schema Queries")
        schema_examples = [
            "SELECT * FROM hotel_location",
            "SELECT * FROM hotel_rate",
            "SELECT * FROM flights",
            "Show hotel schema",
            "Show flight schema"
        ]

        for ex in schema_examples:
            if st.button(ex, key=f"schema_{ex}"):
                st.session_state.query = ex
                if "flight" in ex.lower():
                    st.session_state.query_type = "mongo"
                else:
                    st.session_state.query_type = "sql"

    # Natural language query input
    nl_query = st.text_area("Enter your query in natural language:",
                            height=100,
                            key="nl_query",
                            value=st.session_state.get('query', ''))

    col1, col2 = st.columns(2)
    with col1:
        query_type = st.radio("Select Database Type:",
                              ["SQL (Hotel Database)", "MongoDB (Flight Database)"],
                              index=0 if st.session_state.get('query_type', 'sql') == 'sql' else 1,
                              key="query_type_radio")
        # Update session state when radio button changes
        st.session_state.query_type = "sql" if query_type == "SQL (Hotel Database)" else "mongo"

    with col2:
        execute_button = st.button("Execute Query", key="execute_nl_query")

    if execute_button and nl_query:
        with st.spinner("Processing query..."):
            if query_type == "SQL (Hotel Database)":
                # Process SQL query for hotels
                sql_query = generate_sql_query(nl_query)

                st.subheader("Generated SQL Query:")
                st.code(sql_query, language="sql")

                # Extract key parameters
                county = None
                state = None
                min_rating = None

                # Check for county in query
                county_match = re.search(r'WHERE\s+h\.county\s*=\s*\'([^\']+)\'', sql_query)
                if county_match:
                    county = county_match.group(1)

                # Check for state in query
                state_match = re.search(r'WHERE\s+h\.state\s*=\s*\'([^\']+)\'', sql_query)
                if not state_match:
                    state_match = re.search(r'AND\s+h\.state\s*=\s*\'([^\']+)\'', sql_query)
                if state_match:
                    state = state_match.group(1)

                # Check for rating in query
                rating_match = re.search(r'WHERE\s+r\.rating\s*>=\s*(\d+\.?\d*)', sql_query)
                if not rating_match:
                    rating_match = re.search(r'AND\s+r\.rating\s*>=\s*(\d+\.?\d*)', sql_query)
                if rating_match:
                    min_rating = float(rating_match.group(1))

                # Query hotels with extracted parameters
                with st.spinner("Querying hotels..."):
                    status, hotels = process_hotel_nl_query(county, state, min_rating)

                    if "Success" in status:
                        st.subheader("Query Results:")
                        #hotels_df = format_hotels_as_df(hotels)
                        #st.dataframe(hotels_df)
                        st.success(f"Query executed successfully. Found {len(hotels)} results.")

                        # Option to show raw data
                        if st.checkbox("Show raw hotel data", key="hotel_raw_data"):
                            st.json(hotels)
                    else:
                        st.error(status)



            # In your streamlit_app.py file - modify the MongoDB query execution part

            # MongoDB (Flight Database) section

            else:  # MongoDB (Flight Database)

                # Generate MongoDB query using Ollama

                with st.spinner("Generating MongoDB query using Ollama..."):

                    mongo_query = generate_mongo_query(nl_query)

                    st.subheader("Generated MongoDB Query:")

                    st.code(mongo_query, language="javascript")

                    # Get flights directly from API using the Ollama-generated query

                    with st.spinner("Executing MongoDB query..."):

                        try:

                            # Create appropriate API request payload

                            payload = {

                                "query": mongo_query

                            }

                            # Debug the payload being sent

                            st.write("Debug - Sending payload:", payload)

                            # Set a timeout for the request

                            response = requests.post(

                                f"{API_URL}/execute_mongo_query",

                                json=payload,

                                timeout=30  # Add a timeout in seconds

                            )

                            # Debug the response

                            st.write(f"Debug - Response status: {response.status_code}")

                            if response.status_code == 200:

                                # Successfully got results

                                flights = response.json()

                                # Display results

                                if flights:

                                    st.subheader("Query Results:")

                                    formatted_results = format_flights_as_df(flights)

                                    st.dataframe(formatted_results)

                                    st.success(f"Query executed successfully. Found {len(flights)} results.")

                                    # Allow viewing raw data

                                    if st.checkbox("Show raw flight data", key="flight_raw_data"):
                                        st.json(flights)

                                else:

                                    st.info("No results found for this query.")

                            else:

                                st.error(f"API Error: {response.status_code} - {response.text}")

                        except requests.exceptions.Timeout:

                            st.error("The request timed out. The query might be too complex or the server is busy.")

                        except Exception as e:

                            st.error(f"Error executing query: {str(e)}")

                            st.info("Check the backend logs for more details.")

    else:
        if execute_button:
            st.warning("Please enter a query.")

# Database Schema tab
with tab2:
    st.header("Database Schema")

    schema_tab1, schema_tab2 = st.tabs(["Hotel Database (SQL)", "Flight Database (MongoDB)"])

    with schema_tab1:
        # Display formatted SQL schema
        try:
            sql_schema = get_sql_schema()
            st.markdown(sql_schema)
        except Exception as e:
            st.error(f"Error loading SQL schema: {str(e)}")
            st.markdown("""
            ## Hotel Database Schema (Fallback)
            
            ### hotel_complete_view
            - ID (INTEGER): Primary key
            - hotel_name (TEXT): Name of hotel
            - county (TEXT): County location
            - state (TEXT): State code (e.g., CA, NY)
            
            ### rate_complete_view
            - ID (INTEGER): Primary key (matches hotel_complete_view.ID)
            - rating (REAL): Overall rating (1-5)
            - cleanliness (REAL): Cleanliness rating (1-5)
            - service (REAL): Service rating (1-5)
            - rooms (REAL): Room quality rating (1-5)
            """)

        # Add example SQL query
        st.subheader("Example SQL Query")
        st.code("""
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness 
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.county = 'Orange' AND r.rating >= 4.0
        ORDER BY r.rating DESC
        """)

    with schema_tab2:
        # Display formatted MongoDB schema
        try:
            mongo_schema = get_mongo_schema()
            st.markdown(mongo_schema)
        except Exception as e:
            st.error(f"Error loading MongoDB schema: {str(e)}")
            st.markdown("""
            ## Flight Database Schema (Fallback)
            
            ### flights
            - _id (ObjectId): MongoDB document ID
            - originalId (String): Unique flight identifier
            - startingAirport (String): Departure airport code
            - destinationAirport (String): Arrival airport code
            - totalFare (Number): Flight price in USD
            - totalTripDuration (Number): Trip duration in minutes
            
            ### flights_segments
            - _id (ObjectId): MongoDB document ID
            - originalId (String): Matches flights.originalId
            - segmentsAirlineName (String): Airline name(s), multiple airlines separated by "||"
            """)

        # Add example MongoDB query
        st.subheader("Example MongoDB Query")
        st.code("""
        db.flights.find({
            "startingAirport": "LAX",
            "destinationAirport": "JFK"
        }).sort({ "totalFare": 1 })
        """)

    # Add a sample data section
    st.header("Sample Data")

    sample_tab1, sample_tab2 = st.tabs(["Hotel Sample Data", "Flight Sample Data"])

    with sample_tab1:
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Show Hotel Location Sample"):
                try:
                    conn = sqlite3.connect(LOCATION_DB_PATH)
                    df = pd.read_sql_query("SELECT * FROM hotel_complete_view LIMIT 10", conn)
                    conn.close()
                    st.dataframe(df)
                except Exception as e:
                    st.error(f"Error loading sample data: {str(e)}")
        
        with col2:
            if st.button("Show Hotel Rating Sample"):
                try:
                    conn = sqlite3.connect(RATE_DB_PATH)
                    df = pd.read_sql_query("SELECT * FROM rate_complete_view LIMIT 10", conn)
                    conn.close()
                    st.dataframe(df)
                except Exception as e:
                    st.error(f"Error loading sample data: {str(e)}")

    with sample_tab2:
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Show Flight Sample"):
                try:
                    response = requests.get(f"{API_URL}/flights", params={"limit": 10})
                    if response.status_code == 200:
                        flights_data = response.json()
                        if flights_data:
                            st.dataframe(pd.DataFrame(flights_data))
                        else:
                            st.info("No sample data available")
                    else:
                        st.error(handle_api_error(response, "query"))
                except Exception as e:
                    st.error(f"Error loading sample data: {str(e)}")
        
        with col2:
            if st.button("Show Flight Segments Sample"):
                try:
                    response = requests.get(f"{API_URL}/segments", params={"limit": 10})
                    if response.status_code == 200:
                        segments_data = response.json()
                        if segments_data:
                            st.dataframe(pd.DataFrame(segments_data))
                        else:
                            st.info("No sample data available")
                    else:
                        st.error(handle_api_error(response, "query"))
                except Exception as e:
                    st.error(f"Error loading sample data: {str(e)}")

# Data Modification tab
with tab3:
    st.header("Data Modification")

    # Create subtabs for different databases
    mod_tab1, mod_tab2 = st.tabs(["Hotel Database Modification", "Flight Database Modification"])

    # Hotel Database Modification
    with mod_tab1:
        st.subheader("Modify Hotel Data")

        hotel_operation = st.radio(
            "Select Operation",
            ["Add New Hotel", "Update Hotel", "Delete Hotel"],
            horizontal=True
        )

        if hotel_operation == "Add New Hotel":
            with st.form("add_hotel_form"):
                col1, col2 = st.columns(2)
                with col1:
                    hotel_name = st.text_input("Hotel Name", key="add_hotel_name")
                    county = st.text_input("County", key="add_county")
                    state = st.text_input("State (2-letter code)", key="add_state", max_chars=2)
                with col2:
                    rating = st.slider("Rating", min_value=1.0, max_value=5.0, value=3.0, step=0.5, key="add_rating")
                    cleanliness = st.slider("Cleanliness", min_value=1.0, max_value=5.0, value=3.0, step=0.5)
                    service = st.slider("Service", min_value=1.0, max_value=5.0, value=3.0, step=0.5)

                rooms = st.slider("Rooms", min_value=1.0, max_value=5.0, value=3.0, step=0.5)

                submit_button = st.form_submit_button("Add Hotel")

                if submit_button:
                    if not hotel_name or not county or not state:
                        st.error("Please fill in all required fields (Hotel Name, County, State)")
                    else:
                        hotel_data = {
                            "hotel_name": hotel_name,
                            "county": county,
                            "state": state.upper(),
                            "rating": rating,
                            "cleanliness": cleanliness,
                            "service": service,
                            "rooms": rooms
                        }

                        try:
                            response = requests.post(f"{API_URL}/hotels", json=hotel_data)
                            if response.status_code == 200 or response.status_code == 201:
                                st.success(f"Successfully added hotel: {hotel_name}")

                                # Display the SQL queries that would be executed
                                st.subheader("Equivalent SQL Queries:")

                                # Get the hotel_id from the response if available
                                hotel_id = "new_id"
                                try:
                                    response_data = response.json()
                                    if "id" in response_data:
                                        hotel_id = response_data["id"]
                                except:
                                    pass

                                # Display the insert queries
                                hotel_name_insert = f"""
                                -- Insert into hotel_name1
                                INSERT INTO hotel_name1 (hotel_name) VALUES ('{hotel_name}');
                                """
                                st.code(hotel_name_insert, language="sql")

                                location_insert = f"""
                                -- Insert into location
                                INSERT INTO location (ID, county, state) 
                                VALUES ({hotel_id}, '{county}', '{state.upper()}');
                                """
                                st.code(location_insert, language="sql")

                                rating_insert = f"""
                                -- Insert into rate
                                INSERT INTO rate (ID, rating, service, rooms, cleanliness)
                                VALUES ({hotel_id}, {rating}, {service}, {rooms}, {cleanliness});
                                """
                                st.code(rating_insert, language="sql")

                                st.balloons()
                            else:
                                st.error(handle_api_error(response, "add"))
                        except Exception as e:
                            st.error(f"Error connecting to API: {str(e)}")




        elif hotel_operation == "Update Hotel":

            st.subheader("Update Hotel Information")

            # Let user enter a hotel ID directly

            hotel_id = st.number_input("Enter Hotel ID to update:", min_value=1, step=1)

            if hotel_id:

                # Attempt to fetch the specific hotel by ID

                try:

                    fetch_button = st.button("Fetch Hotel Details")

                    if fetch_button:

                        with st.spinner("Fetching hotel details..."):

                            response = requests.get(f"{API_URL}/hotels/{hotel_id}")

                            if response.status_code == 200:

                                hotel_data = response.json()

                                if isinstance(hotel_data, list) and len(hotel_data) > 0:
                                    hotel_data = hotel_data[0]  # Take the first item if it's a list

                                st.success(f"Found hotel: {hotel_data.get('hotel_name', 'Unknown')}")

                                st.session_state.current_hotel = hotel_data

                            else:

                                st.error(f"Error fetching hotel: {response.text}")

                    # If we have hotel data stored in session state, show the update form

                    if hasattr(st.session_state, 'current_hotel') and st.session_state.current_hotel:

                        hotel_data = st.session_state.current_hotel

                        with st.form("update_hotel_form"):

                            col1, col2 = st.columns(2)

                            with col1:

                                hotel_name = st.text_input("Hotel Name",

                                                           value=hotel_data.get('hotel_name', ''))

                                county = st.text_input("County",

                                                       value=hotel_data.get('county', ''))

                                state = st.text_input("State (2-letter code)",

                                                      value=hotel_data.get('state', ''),

                                                      max_chars=2)

                            with col2:

                                # Convert to float with safe defaults

                                try:

                                    rating_val = float(hotel_data.get('rating', 3.0))

                                except (ValueError, TypeError):

                                    rating_val = 3.0

                                try:

                                    cleanliness_val = float(hotel_data.get('cleanliness', 3.0))

                                except (ValueError, TypeError):

                                    cleanliness_val = 3.0

                                try:

                                    service_val = float(hotel_data.get('service', 3.0))

                                except (ValueError, TypeError):

                                    service_val = 3.0

                                rating = st.slider("Rating", min_value=1.0, max_value=5.0,

                                                   value=rating_val, step=0.5)

                                cleanliness = st.slider("Cleanliness", min_value=1.0, max_value=5.0,

                                                        value=cleanliness_val, step=0.5)

                                service = st.slider("Service", min_value=1.0, max_value=5.0,

                                                    value=service_val, step=0.5)

                            try:

                                rooms_val = float(hotel_data.get('rooms', 3.0))

                            except (ValueError, TypeError):

                                rooms_val = 3.0

                            rooms = st.slider("Rooms", min_value=1.0, max_value=5.0,

                                              value=rooms_val, step=0.5)

                            # Display the hotel ID (read-only)

                            st.info(f"Hotel ID: {hotel_id}")

                            update_button = st.form_submit_button("Update Hotel")

                            if update_button:

                                updated_data = {

                                    "hotel_name": hotel_name,

                                    "county": county,

                                    "state": state.upper(),

                                    "rating": rating,

                                    "cleanliness": cleanliness,

                                    "service": service,

                                    "rooms": rooms

                                }

                                try:

                                    # Make the update request

                                    with st.spinner("Updating hotel information..."):

                                        update_response = requests.put(f"{API_URL}/hotels/{hotel_id}",
                                                                       json=updated_data)

                                        if update_response.status_code == 200:

                                            st.success(f"Successfully updated hotel: {hotel_name}")

                                            # Display the SQL queries that would be executed

                                            st.subheader("Equivalent SQL Queries:")

                                            hotel_name_update = f"""

                                            -- Update hotel_name1

                                            UPDATE hotel_name1 SET hotel_name = '{hotel_name}' 

                                            WHERE ID = {hotel_id};

                                            """

                                            st.code(hotel_name_update, language="sql")

                                            location_update = f"""

                                            -- Update location

                                            UPDATE location SET county = '{county}', state = '{state.upper()}' 

                                            WHERE ID = {hotel_id};

                                            """

                                            st.code(location_update, language="sql")

                                            rating_update = f"""

                                            -- Update rate

                                            UPDATE rate 

                                            SET rating = {rating}, 

                                                service = {service}, 

                                                rooms = {rooms}, 

                                                cleanliness = {cleanliness}

                                            WHERE ID = {hotel_id};

                                            """

                                            st.code(rating_update, language="sql")

                                            st.balloons()

                                            # Clear the current hotel data after update

                                            if 'current_hotel' in st.session_state:
                                                del st.session_state.current_hotel

                                        else:

                                            st.error(f"Error updating hotel: {update_response.text}")

                                except Exception as e:

                                    st.error(f"Error connecting to API: {str(e)}")

                except Exception as e:

                    st.error(f"Error: {str(e)}")

        elif hotel_operation == "Delete Hotel":

            st.subheader("Delete Hotel by ID")

            # Direct delete by ID input

            hotel_id = st.number_input("Enter Hotel ID to delete", min_value=1, step=1)

            if st.button("Delete Hotel", key="delete_hotel_btn"):

                # Confirm deletion without additional search

                if hotel_id:

                    try:

                        # Delete the hotel

                        response = requests.delete(f"{API_URL}/hotels/{hotel_id}")

                        if response.status_code == 200:

                            st.success(f"Successfully deleted hotel with ID: {hotel_id}")

                            # Display the SQL queries that would be executed

                            st.subheader("Equivalent SQL Queries:")

                            # Display the delete queries

                            hotel_name_delete = f"""

                            -- Delete from hotel_name1

                            DELETE FROM hotel_name1 WHERE ID = {hotel_id};

                            """

                            st.code(hotel_name_delete, language="sql")

                            location_delete = f"""

                            -- Delete from location

                            DELETE FROM location WHERE ID = {hotel_id};

                            """

                            st.code(location_delete, language="sql")

                            rating_delete = f"""

                            -- Delete from rate

                            DELETE FROM rate WHERE ID = {hotel_id};

                            """

                            st.code(rating_delete, language="sql")

                        else:

                            st.error(handle_api_error(response, "delete"))

                    except Exception as e:

                        st.error(f"Error connecting to API: {str(e)}")

                else:

                    st.warning("Please enter a valid hotel ID")

        # Flight Database Modification
        with mod_tab2:
            st.subheader("Modify Flight Data")

            flight_operation = st.radio(
                "Select Operation",
                ["Add New Flight", "Update Flight", "Delete Flight"],
                horizontal=True
            )

            if flight_operation == "Add New Flight":
                # Choose between single and multiple flight addition
                add_mode = st.radio(
                    "Select Add Mode",
                    ["Add Single Flight", "Add Multiple Flights"],
                    horizontal=True
                )

                if add_mode == "Add Single Flight":
                    # Keep the original single flight addition form
                    with st.form("add_flight_form"):
                        col1, col2 = st.columns(2)
                        with col1:
                            original_id = st.text_input("Original ID (unique identifier)", key="add_flight_id")
                            starting_airport = st.text_input("Starting Airport Code", key="add_starting")
                            destination_airport = st.text_input("Destination Airport Code", key="add_destination")
                        with col2:
                            airline_name = st.text_input("Airline Name", key="add_airline")
                            total_fare = st.number_input("Total Fare ($)", min_value=0.0, value=100.0, format="%.2f",
                                                         key="add_fare")
                            trip_duration = st.number_input("Trip Duration (minutes)", min_value=0, value=180,
                                                            key="add_duration")

                        submit_button = st.form_submit_button("Add Flight")

                        if submit_button:
                            if not original_id or not starting_airport or not destination_airport:
                                st.error(
                                    "Please fill in all required fields (Original ID, Starting Airport, Destination Airport)")
                            else:
                                # Need to add both flight and segment
                                flight_data = {
                                    "originalId": original_id,
                                    "startingAirport": starting_airport.upper(),
                                    "destinationAirport": destination_airport.upper(),
                                    "totalFare": total_fare,
                                    "totalTripDuration": trip_duration
                                }

                                segment_data = {
                                    "originalId": original_id,
                                    "segmentsAirlineName": airline_name
                                }

                                try:
                                    # First add flight
                                    flight_response = requests.post(f"{API_URL}/flights", json=flight_data)
                                    if flight_response.status_code == 200 or flight_response.status_code == 201:
                                        # Then add segment
                                        segment_response = requests.post(f"{API_URL}/segments", json=segment_data)
                                        if segment_response.status_code == 200 or segment_response.status_code == 201:
                                            st.success(
                                                f"Successfully added flight from {starting_airport} to {destination_airport}")

                                            # Display the MongoDB queries instead of raw data
                                            st.subheader("MongoDB Insert Queries:")

                                            # Flight insert query
                                            flight_insert_query = f"""
                                                    db.flights.insertOne({{
                                                        originalId: "{original_id}",
                                                        startingAirport: "{starting_airport.upper()}",
                                                        destinationAirport: "{destination_airport.upper()}",
                                                        totalFare: {total_fare},
                                                        totalTripDuration: {trip_duration}
                                                    }})
                                                    """
                                            st.code(flight_insert_query, language="javascript")

                                            # Segment insert query
                                            segment_insert_query = f"""
                                                    db.flights_segments.insertOne({{
                                                        originalId: "{original_id}",
                                                        segmentsAirlineName: "{airline_name}"
                                                    }})
                                                    """
                                            st.code(segment_insert_query, language="javascript")

                                            st.balloons()
                                        else:
                                            st.error(f"Error adding segment: {segment_response.text}")
                                    else:
                                        st.error(f"Error adding flight: {flight_response.text}")
                                except Exception as e:
                                    st.error(f"Error connecting to API: {str(e)}")

                else:  # Add multiple flights
                    st.markdown("### Add Multiple Flights")

                    # Input the number of flights to add
                    num_flights = st.number_input("Number of flights to add", min_value=2, max_value=10, value=3)

                    # Create form
                    with st.form("add_multiple_flights_form"):
                        # List to store all flight data
                        all_flights_data = []

                        # Create input fields for each flight
                        for i in range(num_flights):
                            st.markdown(f"### Flight #{i + 1}")

                            col1, col2 = st.columns(2)
                            with col1:
                                original_id = st.text_input(f"Original ID #{i + 1}", key=f"id_{i}")
                                starting_airport = st.text_input(f"Starting Airport Code #{i + 1}", key=f"start_{i}")
                                destination_airport = st.text_input(f"Destination Airport Code #{i + 1}",
                                                                    key=f"dest_{i}")
                            with col2:
                                airline_name = st.text_input(f"Airline Name #{i + 1}", key=f"airline_{i}")
                                total_fare = st.number_input(f"Total Fare ($) #{i + 1}", min_value=0.0, value=100.0,
                                                             format="%.2f", key=f"fare_{i}")
                                trip_duration = st.number_input(f"Trip Duration (minutes) #{i + 1}", min_value=0,
                                                                value=180, key=f"duration_{i}")

                            # Add flight data to the list
                            all_flights_data.append({
                                "original_id": original_id,
                                "starting_airport": starting_airport,
                                "destination_airport": destination_airport,
                                "airline_name": airline_name,
                                "total_fare": total_fare,
                                "trip_duration": trip_duration
                            })

                            # Add separator (except after the last flight)
                            if i < num_flights - 1:
                                st.markdown("---")

                        # Submit button
                        submit_button = st.form_submit_button("Add All Flights")

                        if submit_button:
                            # Validate data for each flight
                            valid_flights = []
                            invalid_indices = []

                            for i, flight in enumerate(all_flights_data):
                                # Check required fields
                                if flight["original_id"] and flight["starting_airport"] and flight[
                                    "destination_airport"]:
                                    valid_flights.append(flight)
                                else:
                                    invalid_indices.append(i + 1)

                            if invalid_indices:
                                # Show flights with missing required fields
                                st.error(
                                    f"Flights #{', #'.join(map(str, invalid_indices))} are missing required fields")

                            if valid_flights:
                                # Display MongoDB insertMany queries that would be executed
                                st.subheader("Equivalent MongoDB Queries:")

                                # Prepare the insertMany data arrays
                                mongo_flights_data = []
                                mongo_segments_data = []

                                for flight in valid_flights:
                                    mongo_flights_data.append({
                                        "originalId": flight["original_id"],
                                        "startingAirport": flight["starting_airport"].upper(),
                                        "destinationAirport": flight["destination_airport"].upper(),
                                        "totalFare": flight["total_fare"],
                                        "totalTripDuration": flight["trip_duration"]
                                    })

                                    mongo_segments_data.append({
                                        "originalId": flight["original_id"],
                                        "segmentsAirlineName": flight["airline_name"]
                                    })

                                # Format the insertMany queries
                                flights_insert_query = "db.flights.insertMany(" + json.dumps(mongo_flights_data,
                                                                                             indent=2) + ")"
                                segments_insert_query = "db.flights_segments.insertMany(" + json.dumps(
                                    mongo_segments_data, indent=2) + ")"

                                st.code(flights_insert_query, language="javascript")
                                st.code(segments_insert_query, language="javascript")

                                # Show progress bar
                                progress_bar = st.progress(0)
                                status_placeholder = st.empty()

                                # Track addition results
                                success_count = 0
                                failed_flights = []

                                # Add flights one by one
                                for i, flight in enumerate(valid_flights):
                                    try:
                                        # Update progress bar and status text
                                        progress = (i + 1) / len(valid_flights)
                                        progress_bar.progress(progress)
                                        status_placeholder.text(
                                            f"Adding flight {i + 1} of {len(valid_flights)}: {flight['starting_airport']} to {flight['destination_airport']}")

                                        # Prepare flight data
                                        flight_data = {
                                            "originalId": flight["original_id"],
                                            "startingAirport": flight["starting_airport"].upper(),
                                            "destinationAirport": flight["destination_airport"].upper(),
                                            "totalFare": flight["total_fare"],
                                            "totalTripDuration": flight["trip_duration"]
                                        }

                                        segment_data = {
                                            "originalId": flight["original_id"],
                                            "segmentsAirlineName": flight["airline_name"]
                                        }

                                        # First add flight
                                        flight_response = requests.post(f"{API_URL}/flights", json=flight_data)

                                        if flight_response.status_code == 200 or flight_response.status_code == 201:
                                            # Then add segment
                                            segment_response = requests.post(f"{API_URL}/segments", json=segment_data)

                                            if segment_response.status_code == 200 or segment_response.status_code == 201:
                                                success_count += 1
                                            else:
                                                failed_flights.append({
                                                    "route": f"{flight['starting_airport']} to {flight['destination_airport']}",
                                                    "error": f"Error adding segment: {segment_response.text}"
                                                })
                                        else:
                                            failed_flights.append({
                                                "route": f"{flight['starting_airport']} to {flight['destination_airport']}",
                                                "error": f"Error adding flight: {flight_response.text}"
                                            })

                                    except Exception as e:
                                        failed_flights.append({
                                            "route": f"{flight['starting_airport']} to {flight['destination_airport']}",
                                            "error": str(e)
                                        })

                                # Clear status text
                                status_placeholder.empty()

                                # Show addition results
                                if success_count > 0:
                                    st.success(f"Successfully added {success_count} of {len(valid_flights)} flights")

                                    # Show balloons effect if all flights were added successfully
                                    if success_count == len(valid_flights):
                                        st.balloons()

                                # Show failed flights
                                if failed_flights:
                                    st.error(f"Failed to add {len(failed_flights)} flights")

                                    with st.expander("Show failed flights"):
                                        for failed_flight in failed_flights:
                                            st.markdown(f"**{failed_flight['route']}**: {failed_flight['error']}")


            elif flight_operation == "Update Flight":
                # Search for flight by ID
                search_col1, search_col2 = st.columns([3, 1])
                with search_col1:
                    original_id = st.text_input("Enter Flight Original ID", key="flight_search_id")
                with search_col2:
                    search_button = st.button("Search", key="flight_search_btn")

                if search_button and original_id:
                    try:
                        # Search for flight
                        flight_response = requests.get(f"{API_URL}/flights/id/{original_id}")
                        # Search for segment
                        segment_response = requests.get(f"{API_URL}/segments/id/{original_id}")

                        if flight_response.status_code == 200:
                            flight = flight_response.json()
                            if flight:
                                st.session_state.found_flight = flight[0] if isinstance(flight, list) else flight
                                st.success(f"Found flight with ID: {original_id}")

                                # Display the found flight
                                st.subheader("Found Flight:")
                                st.json(st.session_state.found_flight)

                                # Store segment if found
                                if segment_response.status_code == 200:
                                    segment = segment_response.json()
                                    if segment:
                                        st.session_state.found_segment = segment[0] if isinstance(segment,
                                                                                                  list) else segment

                                        # Display the found segment
                                        st.subheader("Found Segment:")
                                        st.json(st.session_state.found_segment)
                            else:
                                st.warning(f"No flight found with ID: {original_id}")
                        else:
                            st.error(f"Error searching for flight: {flight_response.text}")
                    except Exception as e:
                        st.error(f"Error connecting to API: {str(e)}")

                # If flight was found, show update form
                if hasattr(st.session_state, 'found_flight'):
                    flight = st.session_state.found_flight
                    segment = getattr(st.session_state, 'found_segment', None)

                    with st.form("update_flight_form"):
                        col1, col2 = st.columns(2)
                        with col1:
                            starting_airport = st.text_input("Starting Airport Code",
                                                             value=flight.get('startingAirport', ''))
                            destination_airport = st.text_input("Destination Airport Code",
                                                                value=flight.get('destinationAirport', ''))
                        with col2:
                            airline_name = st.text_input("Airline Name", value=segment.get('segmentsAirlineName',
                                                                                           '') if segment else '')
                            total_fare = st.number_input("Total Fare ($)", min_value=0.0,
                                                         value=float(flight.get('totalFare', 100.0)), format="%.2f")

                        trip_duration = st.number_input("Trip Duration (minutes)", min_value=0,
                                                        value=int(flight.get('totalTripDuration', 180)))

                        update_button = st.form_submit_button("Update Flight")

                        if update_button:
                            # Update both flight and segment
                            updated_flight = {
                                "startingAirport": starting_airport.upper(),
                                "destinationAirport": destination_airport.upper(),
                                "totalFare": total_fare,
                                "totalTripDuration": trip_duration
                            }

                            updated_segment = {
                                "segmentsAirlineName": airline_name
                            }

                            try:
                                # Update flight
                                flight_response = requests.put(f"{API_URL}/flights/id/{original_id}",
                                                               json=updated_flight)
                                success_flight = flight_response.status_code == 200

                                # Update segment if it exists
                                success_segment = True
                                if segment:
                                    segment_response = requests.put(f"{API_URL}/segments/id/{original_id}",
                                                                    json=updated_segment)
                                    success_segment = segment_response.status_code == 200

                                if success_flight and success_segment:
                                    st.success(
                                        f"Successfully updated flight from {starting_airport} to {destination_airport}")

                                    # Display the MongoDB update queries
                                    st.subheader("MongoDB Update Queries:")

                                    # Flight update query
                                    flight_update_query = f"""
                                                db.flights.updateOne(
                                                    {{ originalId: "{original_id}" }},
                                                    {{ $set: {{
                                                        startingAirport: "{starting_airport.upper()}",
                                                        destinationAirport: "{destination_airport.upper()}",
                                                        totalFare: {total_fare},
                                                        totalTripDuration: {trip_duration}
                                                    }} }}
                                                )
                                                """
                                    st.code(flight_update_query, language="javascript")

                                    # Segment update query (if exists)
                                    if segment:
                                        segment_update_query = f"""
                                                    db.flights_segments.updateOne(
                                                        {{ originalId: "{original_id}" }},
                                                        {{ $set: {{
                                                            segmentsAirlineName: "{airline_name}"
                                                        }} }}
                                                    )
                                                    """
                                        st.code(segment_update_query, language="javascript")

                                    # Clear stored data
                                    if 'found_flight' in st.session_state:
                                        del st.session_state.found_flight
                                    if 'found_segment' in st.session_state:
                                        del st.session_state.found_segment
                                else:
                                    if not success_flight:
                                        st.error(f"Error updating flight: {flight_response.text}")
                                    if not success_segment:
                                        st.error(f"Error updating segment: {segment_response.text}")
                            except Exception as e:
                                st.error(f"Error connecting to API: {str(e)}")

            elif flight_operation == "Delete Flight":
                st.markdown("### Delete Flight by ID")

                # Single input and delete button
                original_id = st.text_input("Enter Flight ID to delete", key="flight_delete_id")

                if st.button("🗑️ Delete Flight", key="delete_flight_btn"):
                    try:
                        # Immediate deletion without search step
                        with st.spinner('Deleting flight...'):
                            # Delete both flight and segment
                            flight_response = requests.delete(f"{API_URL}/flights/id/{original_id}")
                            segment_response = requests.delete(f"{API_URL}/segments/id/{original_id}")

                            success_flight = flight_response.status_code == 200
                            success_segment = segment_response.status_code == 200

                            if success_flight or success_segment:
                                st.success("✅ Flight successfully deleted")

                                # Display the executed MongoDB queries
                                st.subheader("Executed MongoDB Queries:")

                                col1, col2 = st.columns(2)
                                with col1:
                                    st.markdown("**Flight Deletion:**")
                                    st.code(
                                        f"db.flights.deleteOne({{ originalId: \"{original_id}\" }})",
                                        language="javascript"
                                    )

                                with col2:
                                    st.markdown("**Segment Deletion:**")
                                    st.code(
                                        f"db.flights_segments.deleteOne({{ originalId: \"{original_id}\" }})",
                                        language="javascript"
                                    )

                                st.balloons()
                            else:
                                if not success_flight:
                                    st.error(f"Failed to delete flight: {flight_response.text}")
                                if not success_segment:
                                    st.warning(f"Flight deleted but segment deletion failed: {segment_response.text}")
                    except Exception as e:
                        st.error(f"Deletion error: {str(e)}")

    # Add a section at the bottom for database schema reference
    st.sidebar.markdown("## Database Schema Reference")
    schema_expander = st.sidebar.expander("Click to view schema details")
    with schema_expander:
        st.markdown("### Hotel Database Schema:")
        st.markdown("- **hotel_complete_view**(ID, hotel_name, county, state)")
        st.markdown("- **rate_complete_view**(ID, rating, service, rooms, cleanliness)")

        st.markdown("### Flight Database Schema:")
        st.markdown(
            "- **flights**(_id, originalId, startingAirport, destinationAirport, totalFare, totalTripDuration, ...)")
        st.markdown("- **flights_segments**(_id, originalId, segmentsAirlineName, ...)")
