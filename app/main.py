from fastapi import FastAPI, Query, HTTPException
from typing import List, Dict, Any, Optional
import os

from .mongo_agent import get_all_flights, get_flights_by_airports, get_flights_by_airline
from .sql_agent import get_all_reviews, get_reviews_by_county, get_reviews_by_state

app = FastAPI(title="Travel Database API")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Travel Database API"}

# Flight endpoints
@app.get("/flights", response_model=List[Dict[str, Any]])
def get_flights():
    """
    Get all flights from the MongoDB database
    """
    try:
        flights = get_all_flights()
        return flights
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving flights: {str(e)}")

@app.get("/flights/airports", response_model=List[Dict[str, Any]])
def get_flights_by_airport(
    starting: str = Query(..., description="Starting airport code"),
    destination: str = Query(..., description="Destination airport code")
):
    """
    Get flights between specific airports
    """
    try:
        flights = get_flights_by_airports(starting, destination)
        return flights
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving flights: {str(e)}")

@app.get("/flights/airline", response_model=List[Dict[str, Any]])
def get_flights_by_airline_name(
    airline: str = Query(..., description="Airline name")
):
    """
    Get flights operated by a specific airline
    """
    try:
        flights = get_flights_by_airline(airline)
        return flights
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving flights: {str(e)}")

# Hotel endpoints
@app.get("/hotels", response_model=List[Dict[str, Any]])
def get_hotels(
    county: Optional[str] = None,
    state: Optional[str] = None
):
    """
    Get hotel reviews with optional county and state filters
    """
    try:
        if county and state:
            # Filter by both county and state
            # Since there's no direct function for this, we'll get by county and filter in Python
            hotels = get_reviews_by_county(county)
            hotels = [hotel for hotel in hotels if hotel[8] == state]  # Assuming state is at index 8
        elif county:
            hotels = get_reviews_by_county(county)
        elif state:
            hotels = get_reviews_by_state(state)
        else:
            hotels = get_all_reviews()
        
        # Convert tuple data to dictionaries
        result = []
        for hotel in hotels:
            result.append({
                "rating": hotel[0],
                "sleepquality": hotel[1],
                "service": hotel[2],
                "rooms": hotel[3],
                "cleanliness": hotel[4],
                "value": hotel[5],
                "hotel_name": hotel[6],
                "county": hotel[7],
                "state": hotel[8]
            })
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving hotels: {str(e)}")

@app.get("/hotels/county/{county}", response_model=List[Dict[str, Any]])
def get_hotels_by_county(county: str):
    """
    Get hotel reviews for a specific county
    """
    try:
        hotels = get_reviews_by_county(county)
        
        # Convert tuple data to dictionaries
        result = []
        for hotel in hotels:
            result.append({
                "rating": hotel[0],
                "sleepquality": hotel[1],
                "service": hotel[2],
                "rooms": hotel[3],
                "cleanliness": hotel[4],
                "value": hotel[5],
                "hotel_name": hotel[6],
                "county": hotel[7],
                "state": hotel[8]
            })
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving hotels: {str(e)}")

@app.get("/hotels/state/{state}", response_model=List[Dict[str, Any]])
def get_hotels_by_state(state: str):
    """
    Get hotel reviews for a specific state
    """
    try:
        hotels = get_reviews_by_state(state)
        
        # Convert tuple data to dictionaries
        result = []
        for hotel in hotels:
            result.append({
                "rating": hotel[0],
                "sleepquality": hotel[1],
                "service": hotel[2],
                "rooms": hotel[3],
                "cleanliness": hotel[4],
                "value": hotel[5],
                "hotel_name": hotel[6],
                "county": hotel[7],
                "state": hotel[8]
            })
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving hotels: {str(e)}")