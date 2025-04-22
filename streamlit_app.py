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

# Initialize session state variables if they don't exist
if 'query' not in st.session_state:
    st.session_state.query = ""
if 'query_type' not in st.session_state:
    st.session_state.query_type = "sql"

# Function to format flight data for display
def format_flights_as_df(flights):
    """Format flight data as pandas DataFrame for display"""
    if not flights:
        return pd.DataFrame()

    # Create a list of dictionaries with standardized fields
    formatted_flights = []
    for flight in flights:
        # Extract airline name - check multiple possible sources
        airline_name = "N/A"

        # Check if segmentDetails is available as an array
        if "segmentDetails" in flight and flight["segmentDetails"] and len(flight["segmentDetails"]) > 0:
            for segment in flight["segmentDetails"]:
                if isinstance(segment, dict) and "segmentsAirlineName" in segment:
                    airline_name = segment["segmentsAirlineName"]
                    break

        # Check if segmentsAirlineName is directly in the flight object
        elif "segmentsAirlineName" in flight:
            airline_name = flight["segmentsAirlineName"]

        # Check if airline_name is directly in the flight object (might be set during update)
        elif "airline_name" in flight:
            airline_name = flight["airline_name"]

        formatted_flights.append({
            "Departure Airport": flight.get("startingAirport", "N/A"),
            "Destination Airport": flight.get("destinationAirport", "N/A"),
            "Airline": airline_name,
            "Price": f"${flight.get('totalFare', 'N/A')}",
            "Duration (min)": flight.get("totalTripDuration", "N/A")
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
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.county = '{params["county"]}' AND h.state = '{params["state"]}' AND r.rating >= {params["min_rating"]}
        ORDER BY r.rating DESC
        """
    elif "county" in params and "state" in params:
        sql_query = f"""
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.county = '{params["county"]}' AND h.state = '{params["state"]}'
        ORDER BY r.rating DESC
        """
    elif "county" in params:
        sql_query = f"""
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.county = '{params["county"]}'
        ORDER BY r.rating DESC
        """
    elif "state" in params:
        sql_query = f"""
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.state = '{params["state"]}'
        ORDER BY r.rating DESC
        """
    elif "min_rating" in params:
        sql_query = f"""
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE r.rating >= {params["min_rating"]}
        ORDER BY r.rating DESC
        """
    else:
        sql_query = """
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
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
            SELECT h.ID, h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
            FROM hotel_complete_view h
            JOIN rate_complete_view r ON h.ID = r.ID
            LIMIT 50
            """

    # Check for specific queries
    if "orange county" in query_lower:
        return """
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.county = 'Orange'
        ORDER BY r.rating DESC
        """
    elif "california" in query_lower:
        return """
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        WHERE h.state = 'CA'
        ORDER BY r.rating DESC
        """
    elif "best rating" in query_lower or "highest rating" in query_lower:
        return """
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        ORDER BY r.rating DESC
        LIMIT 20
        """
    elif "cleanliness" in query_lower:
        return """
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        ORDER BY r.cleanliness DESC
        LIMIT 20
        """
    elif "sleep quality" in query_lower:
        return """
        SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
        FROM hotel_complete_view h
        JOIN rate_complete_view r ON h.ID = r.ID
        ORDER BY r."sleep quality" DESC
        LIMIT 20
        """

    # Default query
    return """
    SELECT h.hotel_name, h.county, h.state, r.rating, r.cleanliness, r.service, r."sleep quality", r.rooms
    FROM hotel_complete_view h
    JOIN rate_complete_view r ON h.ID = r.ID
    LIMIT 10
    """

# Generate MongoDB query function (simplified version)
def generate_mongo_query(natural_query):
    query_lower = natural_query.lower()

    # Check for schema requests
    if "select * from" in query_lower or "schema" in query_lower or "show collection" in query_lower:
        if "segments" in query_lower:
            return """db.flights_segments.find({}).limit(50)"""
        else:
            return """db.flights.find({}).limit(50)"""

    # Check for specific queries
    if "lax" in query_lower and "jfk" in query_lower:
        return """db.flights.find({
            "startingAirport": "LAX", 
            "destinationAirport": "JFK" 
        }).sort({ "totalFare": 1 })"""
    elif "delta" in query_lower:
        return """db.flights_segments.find({
            "segmentsAirlineName": { "$regex": "Delta", "$options": "i" }
        })"""
    elif "cheapest" in query_lower:
        return """db.flights.find({}).sort({ "totalFare": 1 }).limit(20)"""
    elif "sfo" in query_lower:
        return """db.flights.find({
            "startingAirport": "SFO"
        })"""

    # Default query
    return """db.flights.find({}).limit(10)"""

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
            "What are the top rated hotels in California?",
            "Show me hotels with good sleep quality"
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
                # Generate SQL query
                sql_query = generate_sql_query(nl_query)

                st.subheader("Generated SQL Query:")
                st.code(sql_query, language="sql")

                try:
                    # Connect to SQLite and execute query
                    conn = sqlite3.connect(LOCATION_DB_PATH)
                    result_df = pd.read_sql_query(sql_query, conn)
                    conn.close()

                    # Display results
                    st.subheader("Query Results:")
                    st.dataframe(result_df)
                    st.success(f"Query executed successfully. Found {len(result_df)} results.")
                except Exception as e:
                    st.error(f"Error executing SQL query: {str(e)}")
            else:
                # Generate MongoDB query
                mongo_query = generate_mongo_query(nl_query)

                st.subheader("Generated MongoDB Query:")
                st.code(mongo_query, language="javascript")

                try:
                    # Extract collection name and query part
                    if "flights_segments" in mongo_query:
                        collection = "segments"
                    else:
                        collection = "flights"

                    # Call API to execute MongoDB query
                    response = requests.post(f"{API_URL}/execute_mongo_query",
                                             json={"collection": collection, "query": mongo_query})

                    if response.status_code == 200:
                        results = response.json()

                        # Display results
                        st.subheader("Query Results:")
                        if results:
                            formatted_results = format_flights_as_df(results)
                            st.dataframe(formatted_results)
                            st.success(f"Query executed successfully. Found {len(results)} results.")

                            # Option to show raw data
                            if st.checkbox("Show raw data"):
                                st.json(results)
                        else:
                            st.info("No results found for this query.")
                    else:
                        st.error(handle_api_error(response, "query"))
                except Exception as e:
                    st.error(f"Error executing MongoDB query: {str(e)}")

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
            - sleep quality (REAL): Sleep quality rating (1-5)
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
                sleep_quality = st.slider("Sleep Quality", min_value=1.0, max_value=5.0, value=3.0, step=0.5)

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
                            "rooms": rooms,
                            "sleep quality": sleep_quality
                        }

                        try:
                            response = requests.post(f"{API_URL}/hotels", json=hotel_data)
                            if response.status_code == 200 or response.status_code == 201:
                                st.success(f"Successfully added hotel: {hotel_name}")
                                
                                # Display the SQL query that would be executed
                                st.subheader("Equivalent SQL Query:")
                                sql_insert = f"""
                                -- Insert into hotel_complete_view
                                INSERT INTO hotel_complete_view (hotel_name, county, state)
                                VALUES ('{hotel_name}', '{county}', '{state.upper()}');
                                
                                -- Insert into rate_complete_view (assuming last_insert_rowid() for ID)
                                INSERT INTO rate_complete_view (ID, rating, cleanliness, service, rooms, "sleep quality")
                                VALUES (last_insert_rowid(), {rating}, {cleanliness}, {service}, {rooms}, {sleep_quality});
                                """
                                st.code(sql_insert, language="sql")
                                
                                st.balloons()
                            else:
                                st.error(handle_api_error(response, "add"))
                        except Exception as e:
                            st.error(f"Error connecting to API: {str(e)}")

        elif hotel_operation == "Update Hotel":
            # First, let user search for a hotel
            search_col1, search_col2 = st.columns([3, 1])
            with search_col1:
                search_term = st.text_input("Search for hotel by name", key="hotel_search")
            with search_col2:
                search_button = st.button("Search", key="hotel_search_btn")

            if search_button and search_term:
                try:
                    response = requests.get(f"{API_URL}/hotels/search", params={"name": search_term})
                    if response.status_code == 200:
                        hotels = response.json()
                        if hotels:
                            st.session_state.found_hotels = hotels
                            hotel_options = [f"{h['hotel_name']} (ID: {h['ID']})" for h in hotels]
                            st.session_state.hotel_options = hotel_options
                            st.success(f"Found {len(hotels)} hotels matching '{search_term}'")

                            # Display the found hotels in a table
                            st.subheader("Found Hotels:")
                            st.dataframe(pd.DataFrame(hotels))
                        else:
                            st.warning(f"No hotels found matching '{search_term}'")
                    else:
                        st.error(handle_api_error(response, "search"))
                except Exception as e:
                    st.error(f"Error connecting to API: {str(e)}")

            # If hotels were found in the search, show selection dropdown
            if hasattr(st.session_state, 'hotel_options') and st.session_state.hotel_options:
                selected_hotel = st.selectbox("Select hotel to update", st.session_state.hotel_options)
                if selected_hotel:
                    hotel_id = int(re.search(r'ID: (\d+)', selected_hotel).group(1))
                    selected_hotel_data = next((h for h in st.session_state.found_hotels if h['ID'] == hotel_id), None)

                    if selected_hotel_data:
                        with st.form("update_hotel_form"):
                            col1, col2 = st.columns(2)
                            with col1:
                                hotel_name = st.text_input("Hotel Name",
                                                           value=selected_hotel_data.get('hotel_name', ''))
                                county = st.text_input("County", value=selected_hotel_data.get('county', ''))
                                state = st.text_input("State", value=selected_hotel_data.get('state', ''), max_chars=2)
                            with col2:
                                # Convert to float with safe defaults
                                try:
                                    rating_val = float(selected_hotel_data.get('rating', 3.0))
                                except (ValueError, TypeError):
                                    rating_val = 3.0
                                    
                                try:
                                    cleanliness_val = float(selected_hotel_data.get('cleanliness', 3.0))
                                except (ValueError, TypeError):
                                    cleanliness_val = 3.0
                                    
                                try:
                                    service_val = float(selected_hotel_data.get('service', 3.0))
                                except (ValueError, TypeError):
                                    service_val = 3.0
                                
                                rating = st.slider("Rating", min_value=1.0, max_value=5.0,
                                                   value=rating_val, step=0.5)
                                cleanliness = st.slider("Cleanliness", min_value=1.0, max_value=5.0,
                                                        value=cleanliness_val, step=0.5)
                                service = st.slider("Service", min_value=1.0, max_value=5.0,
                                                    value=service_val, step=0.5)
                            
                            try:
                                rooms_val = float(selected_hotel_data.get('rooms', 3.0))
                            except (ValueError, TypeError):
                                rooms_val = 3.0
                                
                            try:
                                sleep_val = float(selected_hotel_data.get('sleep quality', 3.0))
                            except (ValueError, TypeError):
                                sleep_val = 3.0
                                
                            rooms = st.slider("Rooms", min_value=1.0, max_value=5.0,
                                              value=rooms_val, step=0.5)
                            sleep_quality = st.slider("Sleep Quality", min_value=1.0, max_value=5.0,
                                                      value=sleep_val, step=0.5)
                            update_button = st.form_submit_button("Update Hotel")

                            if update_button:
                                updated_data = {
                                    "hotel_name": hotel_name,
                                    "county": county,
                                    "state": state.upper(),
                                    "rating": rating,
                                    "cleanliness": cleanliness,
                                    "service": service,
                                    "rooms": rooms,
                                    "sleep quality": sleep_quality
                                }

                                try:
                                    response = requests.put(f"{API_URL}/hotels/{hotel_id}", json=updated_data)
                                    if response.status_code == 200:
                                        st.success(f"Successfully updated hotel: {hotel_name}")

                                        # Display the updated data
                                        st.subheader("Updated Hotel Data:")
                                        # Display side by side comparison
                                        compare_col1, compare_col2 = st.columns(2)
                                        with compare_col1:
                                            st.subheader("Before:")
                                            st.json(selected_hotel_data)
                                        with compare_col2:
                                            st.subheader("After:")
                                            st.json(updated_data)

                                        # Get updated record to verify
                                        try:
                                            verify_response = requests.get(f"{API_URL}/hotels/{hotel_id}")
                                            if verify_response.status_code == 200:
                                                st.subheader("Verified Updated Data in Database:")
                                                st.json(verify_response.json())
                                        except:
                                            pass

                                        # Clear the selection to prevent accidental updates
                                        if 'hotel_options' in st.session_state:
                                            del st.session_state.hotel_options
                                        if 'found_hotels' in st.session_state:
                                            del st.session_state.found_hotels
                                    else:
                                        st.error(f"Error updating hotel: {response.text}")
                                except Exception as e:
                                    st.error(f"Error connecting to API: {str(e)}")

        elif hotel_operation == "Delete Hotel":
            # Similar search functionality as update
            search_col1, search_col2 = st.columns([3, 1])
            with search_col1:
                search_term = st.text_input("Search for hotel by name", key="hotel_delete_search")
            with search_col2:
                search_button = st.button("Search", key="hotel_delete_search_btn")

            if search_button and search_term:
                try:
                    response = requests.get(f"{API_URL}/hotels/search", params={"name": search_term})
                    if response.status_code == 200:
                        hotels = response.json()
                        if hotels:
                            st.session_state.delete_hotels = hotels
                            hotel_options = [f"{h['hotel_name']} (ID: {h['ID']})" for h in hotels]
                            st.session_state.delete_hotel_options = hotel_options
                            st.success(f"Found {len(hotels)} hotels matching '{search_term}'")

                            # Display the found hotels in a table
                            st.subheader("Found Hotels:")
                            st.dataframe(pd.DataFrame(hotels))
                        else:
                            st.warning(f"No hotels found matching '{search_term}'")
                    else:
                        st.error(f"Error searching hotels: {response.text}")
                except Exception as e:
                    st.error(f"Error connecting to API: {str(e)}")

                    # If hotels were found, show selection for deletion
                    if hasattr(st.session_state, 'delete_hotel_options') and st.session_state.delete_hotel_options:
                        selected_hotel = st.selectbox("Select hotel to delete", st.session_state.delete_hotel_options)
                        if selected_hotel:
                            hotel_id = int(re.search(r'ID: (\d+)', selected_hotel).group(1))

                            # Display the hotel to be deleted
                            hotel_to_delete = next((h for h in st.session_state.delete_hotels if h['ID'] == hotel_id),
                                                   None)
                            if hotel_to_delete:
                                st.subheader("Hotel to delete:")
                                st.json(hotel_to_delete)

                            # Confirm deletion
                            if st.checkbox("I confirm I want to delete this hotel", key="confirm_hotel_delete"):
                                if st.button("Delete Hotel", key="delete_hotel_btn"):
                                    try:
                                        response = requests.delete(f"{API_URL}/hotels/{hotel_id}")
                                        if response.status_code == 200:
                                            st.success(f"Successfully deleted hotel: {selected_hotel}")

                                            # Display what was deleted
                                            st.subheader("Deleted Hotel Data:")
                                            st.json(hotel_to_delete)

                                            # Clear the selection
                                            if 'delete_hotel_options' in st.session_state:
                                                del st.session_state.delete_hotel_options
                                            if 'delete_hotels' in st.session_state:
                                                del st.session_state.delete_hotels
                                        else:
                                            st.error(f"Error deleting hotel: {response.text}")
                                    except Exception as e:
                                        st.error(f"Error connecting to API: {str(e)}")
                            else:
                                st.info("Please confirm deletion by checking the box above")

        # Flight Database Modification
        with mod_tab2:
            st.subheader("Modify Flight Data")

            flight_operation = st.radio(
                "Select Operation",
                ["Add New Flight", "Update Flight", "Delete Flight"],
                horizontal=True
            )

            if flight_operation == "Add New Flight":
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
        st.markdown("- **rate_complete_view**(ID, rating, sleep quality, service, rooms, cleanliness)")

        st.markdown("### Flight Database Schema:")
        st.markdown(
            "- **flights**(_id, originalId, startingAirport, destinationAirport, totalFare, totalTripDuration, ...)")
        st.markdown("- **flights_segments**(_id, originalId, segmentsAirlineName, ...)")
