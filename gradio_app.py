import gradio as gr
import requests
import os
import json

# Get API URL and Ollama URL (from environment variables)
API_URL = os.environ.get("API_URL", "http://backend:8000")
OLLAMA_API = os.environ.get("OLLAMA_HOST", "http://ollama:11434")

def query_flights(query_type, param1="", param2=""):
    """
    Query backend API for flight information
    """
    try:
        if query_type == "all_flights":
            response = requests.get(f"{API_URL}/flights")
        elif query_type == "by_airports":
            response = requests.get(f"{API_URL}/flights/airports", 
                                  params={"starting": param1, "destination": param2})
        elif query_type == "by_airline":
            response = requests.get(f"{API_URL}/flights/airline", 
                                  params={"airline": param1})
        else:
            return "Invalid query type"
        
        if response.status_code == 200:
            flights = response.json()
            return format_flights(flights)
        else:
            return f"Error: {response.status_code} - {response.text}"
    except Exception as e:
        return f"Error connecting to API: {str(e)}"

def format_flights(flights):
    """Format flight data for display"""
    if not flights:
        return "No flights found."
    
    result = ""
    for i, flight in enumerate(flights):
        result += f"Flight {i+1}:\n"
        result += f"  Departure Airport: {flight.get('startingAirport', 'N/A')}\n"
        result += f"  Destination Airport: {flight.get('destinationAirport', 'N/A')}\n"
        result += f"  Airline: {flight.get('segmentsAirlineName', 'N/A')}\n"
        result += f"  Price: ${flight.get('totalFare', 'N/A')}\n"
        result += f"  Duration: {flight.get('totalTripDuration', 'N/A')} minutes\n\n"
    
    return result

def query_hotels(county="", state="", min_rating=None):
    """
    Query backend API for hotel information
    """
    try:
        params = {}
        if county:
            params["county"] = county
        if state:
            params["state"] = state
        if min_rating:
            params["min_rating"] = min_rating
            
        if county and state:
            response = requests.get(f"{API_URL}/hotels", params=params)
        elif min_rating is not None:
            # Assuming there's an endpoint for rating-based filtering
            # If not implemented yet, this would need a backend update
            response = requests.get(f"{API_URL}/hotels", params=params)
        elif county:
            response = requests.get(f"{API_URL}/hotels/county/{county}", params=params)
        elif state:
            response = requests.get(f"{API_URL}/hotels/state/{state}", params=params)
        else:
            response = requests.get(f"{API_URL}/hotels")
        
        if response.status_code == 200:
            hotels = response.json()
            return format_hotels(hotels)
        else:
            return f"Error: {response.status_code} - {response.text}"
    except Exception as e:
        return f"Error connecting to API: {str(e)}"

def format_hotels(hotels):
    """Format hotel data for display"""
    if not hotels:
        return "No hotels found."
    
    result = ""
    for i, hotel in enumerate(hotels):
        result += f"Hotel {i+1}: {hotel.get('hotel_name', 'N/A')}\n"
        result += f"  County: {hotel.get('county', 'N/A')}\n"
        result += f"  State: {hotel.get('state', 'N/A')}\n"
        result += f"  Rating: {hotel.get('rating', 'N/A')}\n"
        result += f"  Cleanliness: {hotel.get('cleanliness', 'N/A')}\n"
        result += f"  Service: {hotel.get('service', 'N/A')}\n\n"
    
    return result

def call_ollama(prompt):
    """
    Directly call Ollama API
    """
    try:
        payload = {
            "model": "tinyllama",
            "prompt": prompt,
            "stream": False
        }
        
        response = requests.post(f"{OLLAMA_API}/api/generate", json=payload)
        
        if response.status_code == 200:
            return response.json().get("response", "No response")
        else:
            return f"Ollama API error: {response.status_code} - {response.text}"
    except Exception as e:
        return f"Error calling Ollama: {str(e)}"

def natural_language_query(query):
    """
    Use Ollama to process natural language queries
    """
    system_prompt = """
You are a travel database assistant that needs to parse natural language queries into structured requests.

Determine if the query is about flights or hotels:
1. If it's about flights, determine if it's:
   a) All flights
   b) Flights between specific airports (extract departure and destination airports)
   c) Flights from a specific airline (extract airline name)

2. If it's about hotels, determine if it's:
   a) All hotels
   b) Hotels in a specific county (extract county name)
   c) Hotels in a specific state (extract state name)
   d) Hotels in a specific county and state (extract both)
   e) Hotels with a minimum rating (extract minimum rating)

Return answer in JSON format:
Flight queries:
{
  "type": "flights",
  "query_type": "all_flights" or "by_airports" or "by_airline",
  "params": {
    "starting": "departure airport code (if applicable)",
    "destination": "destination airport code (if applicable)",
    "airline": "airline name (if applicable)"
  }
}

Hotel queries:
{
  "type": "hotels",
  "query_type": "all_hotels" or "by_county" or "by_state" or "by_county_and_state" or "by_rating",
  "params": {
    "county": "county name (if applicable)",
    "state": "state name (if applicable)",
    "min_rating": "minimum rating value (if applicable)"
  }
}

Note:
- Hotel data is stored in an SQLite database, not in MongoDB
- All hotel queries should be routed to hotel-related API endpoints
- Query types must exactly match those listed above

Do not provide additional explanations, just return the JSON object.
"""

    full_prompt = f"{system_prompt}\n\nUser query: {query}"

    try:
        # Call Ollama API
        response = call_ollama(full_prompt)

        # Try to extract JSON from response
        try:
            # Find the position where JSON starts
            json_start = response.find('{')
            json_end = response.rfind('}') + 1

            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                parsed = json.loads(json_str)
            else:
                # If no JSON format is found, fall back to keyword matching
                return fallback_natural_language_query(query)

            # Route to appropriate function based on parsing result
            if parsed.get("type") == "flights":
                query_type = parsed.get("query_type")
                params = parsed.get("params", {})

                if query_type == "all_flights":
                    return query_flights("all_flights")
                elif query_type == "by_airports":
                    return query_flights("by_airports", params.get("starting", ""), params.get("destination", ""))
                elif query_type == "by_airline":
                    return query_flights("by_airline", params.get("airline", ""))

            elif parsed.get("type") == "hotels":
                query_type = parsed.get("query_type")
                params = parsed.get("params", {})

                if query_type == "all_hotels":
                    return query_hotels()
                elif query_type == "by_county":
                    return query_hotels(county=params.get("county", ""))
                elif query_type == "by_state":
                    return query_hotels(state=params.get("state", ""))
                elif query_type == "by_county_and_state":
                    return query_hotels(county=params.get("county", ""), state=params.get("state", ""))
                elif query_type == "by_rating":
                    return query_hotels(county=params.get("county", ""), state=params.get("state", ""), 
                                       min_rating=params.get("min_rating", None))

            return "Could not understand query. Please try a more specific formulation."
        except json.JSONDecodeError:
            # If JSON parsing fails, fall back to keyword matching
            return fallback_natural_language_query(query)
    except Exception as e:
        # If Ollama call fails, fall back to keyword matching
        return f"Error processing query: {str(e)}\n\nAttempting keyword matching...\n\n{fallback_natural_language_query(query)}"

def fallback_natural_language_query(query):
    """
    Fallback natural language processing (based on keyword matching) when Ollama is unavailable
    """
    # Simple keyword matching
    query_lower = query.lower()
    
    # Check if it's a flight query
    if "flight" in query_lower or "plane" in query_lower or "airline" in query_lower or "airport" in query_lower:
        # Check if it includes airport codes
        airport_keywords = ["from", "to", "between"]
        
        # Check if there's a specific airline
        airline_keywords = ["airline", "airways", "air"]
        
        # Prioritize checking airports
        for keyword in airport_keywords:
            if keyword in query_lower:
                # Simple parsing, assuming format like "from LAX to JFK" or "LAX to JFK"
                parts = query_lower.split(keyword)
                if len(parts) > 1:
                    # Very simplified handling, real application would need more complex logic
                    starting = parts[0].strip().upper()
                    destination = parts[1].strip().upper()
                    if starting and destination:
                        return query_flights("by_airports", starting, destination)
        
        # Check for airline
        for keyword in airline_keywords:
            if keyword in query_lower:
                parts = query_lower.split(keyword)
                if len(parts) > 1:
                    airline = parts[1].strip()
                    if airline:
                        return query_flights("by_airline", airline)
        
        # Default to return all flights
        return query_flights("all_flights")
    
    # Check if it's a hotel query
    elif "hotel" in query_lower or "lodging" in query_lower or "accommodation" in query_lower:
        county = None
        state = None
        min_rating = None
        
        # Check if it contains a county name
        county_keywords = ["county"]
        for keyword in county_keywords:
            if keyword in query_lower:
                parts = query_lower.split(keyword)
                if len(parts) > 0:
                    county = parts[0].strip()
        
        # Check if it contains a state name
        state_keywords = ["state", "in"]
        for keyword in state_keywords:
            if keyword in query_lower:
                parts = query_lower.split(keyword)
                if len(parts) > 0:
                    state = parts[0].strip()
        
        # Check if it contains rating requirements
        rating_keywords = ["rating", "rated", "stars", "star"]
        for keyword in rating_keywords:
            if keyword in query_lower:
                # Try to extract numbers
                import re
                numbers = re.findall(r'\d+', query_lower)
                if numbers:
                    min_rating = int(numbers[0])
        
        # Query based on extracted information
        if min_rating is not None:
            return query_hotels(county, state, min_rating=min_rating)
        elif county and state:
            return query_hotels(county, state)
        elif county:
            return query_hotels(county=county)
        elif state:
            return query_hotels(state=state)
        else:
            return query_hotels()
    
    # Could not determine query type
    else:
        return "Could not understand your query. Please try a clearer formulation, such as 'show all flights' or 'find hotels in California'."

# Create Gradio interface
with gr.Blocks(title="Travel Database Query") as demo:
    gr.Markdown("# Travel Database Query")
    gr.Markdown("Search for flights and hotel information")
    
    with gr.Tab("Natural Language Query"):
        with gr.Row():
            nl_input = gr.Textbox(label="Your Question", placeholder="Example: Show flights from LAX to JFK")
            nl_button = gr.Button("Search")
        nl_output = gr.Textbox(label="Results", lines=10)
        nl_button.click(natural_language_query, inputs=[nl_input], outputs=[nl_output])
    
    with gr.Tab("Flight Search"):
        with gr.Row():
            query_type = gr.Radio(
                ["All Flights", "Search by Airports", "Search by Airline"],
                label="Query Type",
                value="All Flights"
            )
        
        with gr.Row():
            starting_airport = gr.Textbox(label="Departure Airport Code", visible=False)
            destination_airport = gr.Textbox(label="Destination Airport Code", visible=False)
            airline_name = gr.Textbox(label="Airline Name", visible=False)
        
        def update_visibility(query_type):
            if query_type == "All Flights":
                return gr.update(visible=False), gr.update(visible=False), gr.update(visible=False)
            elif query_type == "Search by Airports":
                return gr.update(visible=True), gr.update(visible=True), gr.update(visible=False)
            else:  # Search by Airline
                return gr.update(visible=False), gr.update(visible=False), gr.update(visible=True)
        
        query_type.change(
            update_visibility,
            inputs=[query_type],
            outputs=[starting_airport, destination_airport, airline_name]
        )
        
        flight_button = gr.Button("Search Flights")
        flight_results = gr.Textbox(label="Results", lines=10)
        
        def process_flight_query(query_type, starting, destination, airline):
            if query_type == "All Flights":
                return query_flights("all_flights")
            elif query_type == "Search by Airports":
                return query_flights("by_airports", starting, destination)
            else:  # Search by Airline
                return query_flights("by_airline", airline)
        
        flight_button.click(
            process_flight_query,
            inputs=[query_type, starting_airport, destination_airport, airline_name],
            outputs=[flight_results]
        )
    
    with gr.Tab("Hotel Search"):
        with gr.Row():
            county = gr.Textbox(label="County")
            state = gr.Textbox(label="State")
            min_rating = gr.Slider(label="Minimum Rating", minimum=1, maximum=5, step=0.5, value=None)
        
        hotel_button = gr.Button("Search Hotels")
        hotel_results = gr.Textbox(label="Results", lines=10)
        
        def process_hotel_query(county, state, min_rating):
            return query_hotels(county, state, min_rating if min_rating else None)
        
        hotel_button.click(
            process_hotel_query,
            inputs=[county, state, min_rating],
            outputs=[hotel_results]
        )

# Start the application
if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
