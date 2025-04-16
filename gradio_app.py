import gradio as gr
import requests
import os
from langchain_community.llms.ollama import Ollama
import json

# Get API URL from environment variables
API_URL = os.environ.get("API_URL", "http://localhost:8000")
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")

# Initialize Ollama client
llm = Ollama(model="llama3", base_url=OLLAMA_HOST)


def query_flights(query_type, param1="", param2=""):
    """
    Query the backend API for flight information
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
        result += f"Flight {i + 1}:\n"
        result += f"  Starting Airport: {flight.get('startingAirport', 'N/A')}\n"
        result += f"  Destination Airport: {flight.get('destinationAirport', 'N/A')}\n"
        result += f"  Airline: {flight.get('segmentsAirlineName', 'N/A')}\n"
        result += f"  Price: ${flight.get('totalFare', 'N/A')}\n"
        result += f"  Duration: {flight.get('totalTripDuration', 'N/A')} minutes\n\n"

    return result


def query_hotels(county="", state=""):
    """
    Query the backend API for hotel information
    """
    try:
        if county and state:
            response = requests.get(f"{API_URL}/hotels",
                                    params={"county": county, "state": state})
        elif county:
            response = requests.get(f"{API_URL}/hotels/county/{county}")
        elif state:
            response = requests.get(f"{API_URL}/hotels/state/{state}")
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
        result += f"Hotel {i + 1}: {hotel.get('hotel_name', 'N/A')}\n"
        result += f"  County: {hotel.get('county', 'N/A')}\n"
        result += f"  State: {hotel.get('state', 'N/A')}\n"
        result += f"  Rating: {hotel.get('rating', 'N/A')}\n"
        result += f"  Cleanliness: {hotel.get('cleanliness', 'N/A')}\n"
        result += f"  Service: {hotel.get('service', 'N/A')}\n\n"

    return result


def natural_language_query(query):
    """
    Use LLM to parse natural language queries and route to appropriate endpoints
    """
    prompt = f"""
Parse the following natural language query about travel. Determine if it's about flights or hotels.
If it's about flights, determine if it's asking for:
1. All flights
2. Flights between specific airports (extract starting and destination airports)
3. Flights by a specific airline (extract airline name)

If it's about hotels, determine if it's asking for:
1. All hotels
2. Hotels in a specific county (extract county)
3. Hotels in a specific state (extract state)
4. Hotels in a specific county and state (extract both)

Return your answer as a JSON object with the following structure:
For flights:
{{
  "type": "flights",
  "query_type": "all_flights" or "by_airports" or "by_airline",
  "params": {{
    "starting": "starting airport code if applicable",
    "destination": "destination airport code if applicable",
    "airline": "airline name if applicable"
  }}
}}

For hotels:
{{
  "type": "hotels",
  "query_type": "all_hotels" or "by_county" or "by_state" or "by_county_and_state",
  "params": {{
    "county": "county name if applicable",
    "state": "state name if applicable"
  }}
}}

Query: {query}
"""
    try:
        # Get structured response from LLM
        response = llm.invoke(prompt)
        parsed = json.loads(response)

        # Route to appropriate function
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

        return "Couldn't understand the query. Please try again."
    except Exception as e:
        return f"Error processing query: {str(e)}\n\nRaw LLM response: {response}"


# Create Gradio interface
with gr.Blocks(title="Travel Database ChatBot") as demo:
    gr.Markdown("# Travel Database ChatBot")
    gr.Markdown("Ask questions about flights and hotels in natural language")

    with gr.Tab("Natural Language Query"):
        with gr.Row():
            nl_input = gr.Textbox(label="Your Question", placeholder="e.g., Show me flights from LAX to JFK")
            nl_button = gr.Button("Search")
        nl_output = gr.Textbox(label="Results", lines=10)
        nl_button.click(natural_language_query, inputs=[nl_input], outputs=[nl_output])

    with gr.Tab("Flights Search"):
        with gr.Row():
            query_type = gr.Radio(
                ["All Flights", "Search by Airports", "Search by Airline"],
                label="Query Type",
                value="All Flights"
            )

        with gr.Row():
            starting_airport = gr.Textbox(label="Starting Airport Code", visible=False)
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

    with gr.Tab("Hotels Search"):
        with gr.Row():
            county = gr.Textbox(label="County")
            state = gr.Textbox(label="State")

        hotel_button = gr.Button("Search Hotels")
        hotel_results = gr.Textbox(label="Results", lines=10)

        hotel_button.click(
            query_hotels,
            inputs=[county, state],
            outputs=[hotel_results]
        )

# Launch the app
if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)