import streamlit as st
import requests
import os
import json
import pandas as pd
import re
from urllib.parse import quote

# Configuration
API_URL = os.environ.get("API_URL", "http://backend:8000")
OLLAMA_API = os.environ.get("OLLAMA_HOST", "http://ollama:11434")

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
        # Create the prompt for Ollama
        prompt = f"""
        You are a MongoDB query generator for a flight database.
        Convert the following natural language query to a MongoDB query:
        "{natural_language_query}"
        
        The MongoDB database has these collections:
        1. flights - contains flight information with fields:
           - originalId: Unique flight identifier
           - startingAirport: Airport code for departure (e.g., LAX, JFK)
           - destinationAirport: Airport code for arrival
           - totalFare: Price of the flight in dollars
           - totalTripDuration: Duration of the flight in minutes
        
        2. segments - contains airline information with fields:
           - originalId: Matches with flights.originalId
           - segmentsAirlineName: Name of the airline (e.g., Delta, United)
        
        ONLY return the MongoDB query as a string formatted like:
        db.collection.find({{}})
        
        DO NOT include any explanations, just return the MongoDB query.
        Limit all results to 20 documents maximum.
        """
        
        # Call Ollama API
        response = requests.post(
            f"{OLLAMA_API}/api/generate",
            json={
                "model": "llama3",  # Using llama3 model - adjust as needed
                "prompt": prompt,
                "stream": False
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            generated_query = result.get("response", "").strip()
            
            # Ensure it's a valid MongoDB query
            # Make sure it starts with db.
            if not generated_query.startswith("db."):
                generated_query = "db.flights.find({}).limit(20)"
                
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
            return "db.flights.find({}).limit(20)"  # Default fallback query
            
    except Exception as e:
        st.error(f"Error generating MongoDB query: {str(e)}")
        return "db.flights.find({}).limit(20)"  # Default fallback query

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
    query_type = "all_flights"
    params = {"limit": 20}
    
    try:
        # Extract collection name
        collection_match = re.search(r'db\.(\w+)\.', query_string)
        collection = collection_match.group(1) if collection_match else "flights"
        
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
        
        # Determine query type based on parameters
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
                
        return query_type, params, query_string
        
    except Exception as e:
        st.error(f"Error parsing MongoDB query: {str(e)}")
        return "all_flights", {"limit": 20}, query_string
