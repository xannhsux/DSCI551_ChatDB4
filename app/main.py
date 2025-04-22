from fastapi import FastAPI, Query, HTTPException
from typing import List, Dict, Any, Optional
import logging

from .mongo_agent import (
    get_all_flights,
    get_flights_by_airports,
    get_flights_by_airline,
    find_with_projection
)
from .sql_agent import (
    get_all_reviews,
    get_reviews_by_county,
    get_reviews_by_state,
    find_hotels_with_min_rating
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Travel Database API")


@app.get("/", response_model=Dict[str, str])
def read_root():
    return {"message": "Welcome to the Travel Database API"}


 Flight endpoints
@app.get("/flights", response_model=List[Dict[str, Any]])
def get_flights():
    """
    Get all flights from the MongoDB database
    """
    try:
        flights = get_all_flights()
        return flights
    except Exception as e:
        logger.error(f"Error retrieving flights: {e}")
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
        logger.error(f"Error retrieving flights: {e}")
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
        logger.error(f"Error retrieving flights: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving flights: {str(e)}")
@app.get("/flights/segments", response_model=List[Dict[str, Any]])
def get_flight_segments(limit: int = 10):
    """
    Get flight segments data
    """
    try:
        segments = find_with_projection("flights_segments", {}, None, limit)
        return segments
    except Exception as e:
        logger.error(f"Error retrieving flight segments: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving flight segments: {str(e)}")
# Hotel endpoints
@app.get("/hotels", response_model=List[Dict[str, Any]])
def get_hotels(
    county: Optional[str] = None,
    state: Optional[str] = None,
    min_rating: Optional[float] = None
):
    """
    Get hotel reviews with optional county, state, and rating filters
    """
    try:
        logger.info(f"Getting hotels with filters: county={county}, state={state}, min_rating={min_rating}")
        
        if county and state:
            # Filter by both county and state
            hotels = get_reviews_by_county(county)
            # Normalize state parameter for case-insensitive comparison
            state_upper = state.upper() if state else None
            hotels = [hotel for hotel in hotels if hotel[8].upper() == state_upper]
        elif min_rating is not None:
            # Get hotels with minimum rating
            from .sql_agent import find_hotels_with_min_rating
            hotels = find_hotels_with_min_rating(min_rating)
        elif county:
            hotels = get_reviews_by_county(county)
        elif state:
            logger.info(f"Searching for hotels in state: {state}")
            hotels = get_reviews_by_state(state)
        else:
            hotels = get_all_reviews()
        
        # Convert tuple data to dictionaries
        result = []
        for hotel in hotels:
            result.append({
                "rating": hotel[0],
                "sleep quality": hotel[1],  # Updated field name with space
                "service": hotel[2],
                "rooms": hotel[3],
                "cleanliness": hotel[4],
                "value": hotel[5],
                "hotel_name": hotel[6],
                "county": hotel[7],
                "state": hotel[8]
            })
        
        logger.info(f"Found {len(result)} hotels matching criteria")
        return result
    except Exception as e:
        logger.error(f"Error retrieving hotels: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving hotels: {str(e)}")

@app.get("/hotels/county/{county}", response_model=List[Dict[str, Any]])
def get_hotels_by_county(county: str):
    """
    Get hotel reviews for a specific county
    """
    try:
        logger.info(f"Getting hotels for county: {county}")
        hotels = get_reviews_by_county(county)
        
        # Convert tuple data to dictionaries
        result = []
        for hotel in hotels:
            result.append({
                "rating": hotel[0],
                "sleep quality": hotel[1],  # Updated field name with space
                "service": hotel[2],
                "rooms": hotel[3],
                "cleanliness": hotel[4],
                "value": hotel[5],
                "hotel_name": hotel[6],
                "county": hotel[7],
                "state": hotel[8]
            })
        
        logger.info(f"Found {len(result)} hotels in county: {county}")
        return result
    except Exception as e:
        logger.error(f"Error retrieving hotels for county {county}: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving hotels: {str(e)}")

@app.get("/hotels/state/{state}", response_model=List[Dict[str, Any]])
def get_hotels_by_state(state: str):
    """
    Get hotel reviews for a specific state
    """
    try:
        # Clean up and normalize the state parameter
        clean_state = state.strip()
        # Remove any query parameters that might have been included
        if "?" in clean_state:
            clean_state = clean_state.split("?")[0]
            
        logger.info(f"Getting hotels for state: {clean_state}")
        
        # Try to connect to MongoDB to check connection
        try:
            from .mongo_agent import get_client
            client = get_client()
            if client:
                logger.info("Connected to cloud MongoDB successfully")
        except Exception as e:
            logger.warning(f"MongoDB connection check failed: {e}")
        
        # Get hotel data
        hotels = get_reviews_by_state(clean_state)
        
        # Convert tuple data to dictionaries
        result = []
        for hotel in hotels:
            result.append({
                "rating": hotel[0],
                "sleep quality": hotel[1],  # Updated field name with space
                "service": hotel[2],
                "rooms": hotel[3],
                "cleanliness": hotel[4],
                "value": hotel[5],
                "hotel_name": hotel[6],
                "county": hotel[7],
                "state": hotel[8]
            })
        
        logger.info(f"Found {len(result)} hotels in state: {clean_state}")
        return result
    except Exception as e:
        logger.error(f"Error retrieving hotels for state {state}: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving hotels: {str(e)}")


# Add these imports if not already present
from pydantic import BaseModel
from typing import Optional, List


# Define models for request validation
class FlightModel(BaseModel):
    originalId: str
    startingAirport: str
    destinationAirport: str
    totalFare: float
    totalTripDuration: int


class SegmentModel(BaseModel):
    originalId: str
    segmentsAirlineName: str


# Add these endpoints to your main.py

@app.post("/flights", status_code=201)
async def create_flight(flight: FlightModel):
    """Add a new flight to the database"""
    from app.mongo_agent import insert_one

    flight_data = flight.dict()
    result = insert_one("flights", flight_data)

    if result["acknowledged"]:
        return {"success": True, "message": "Flight added successfully", "id": result["inserted_id"]}
    else:
        raise HTTPException(status_code=500, detail="Failed to add flight")


@app.post("/segments", status_code=201)
async def create_segment(segment: SegmentModel):
    """Add a new flight segment to the database"""
    from app.mongo_agent import insert_one

    segment_data = segment.dict()
    result = insert_one("segments", segment_data)

    if result["acknowledged"]:
        return {"success": True, "message": "Segment added successfully", "id": result["inserted_id"]}
    else:
        raise HTTPException(status_code=500, detail="Failed to add segment")


@app.get("/flights/id/{original_id}")
async def get_flight_by_id(original_id: str):
    """Get a flight by its originalId"""
    from app.mongo_agent import find_with_projection

    # Print debugging info
    print(f"Searching for flight with originalId: {original_id}")

    # Query the flights collection - use flights_basic collection name
    flights = find_with_projection("flights_basic", {"originalId": original_id})

    if not flights:
        # Try searching by _id in case originalId is actually an ObjectId
        from bson import ObjectId
        try:
            object_id = ObjectId(original_id)
            flights = find_with_projection("flights_basic", {"_id": object_id})
        except:
            pass

    if not flights:
        raise HTTPException(status_code=404, detail="Flight not found")

    return flights


@app.get("/segments/id/{original_id}")
async def get_segment_by_id(original_id: str):
    """Get a flight segment by its originalId"""
    from app.mongo_agent import find_with_projection

    # Query the segments collection
    segments = find_with_projection("segments", {"originalId": original_id})

    if not segments:
        raise HTTPException(status_code=404, detail="Segment not found")

    return segments


from pydantic import BaseModel
from typing import Optional, List
from fastapi import HTTPException


# Define models for request validation
class FlightModel(BaseModel):
    originalId: str
    startingAirport: str
    destinationAirport: str
    totalFare: float
    totalTripDuration: int


class SegmentModel(BaseModel):
    originalId: str
    segmentsAirlineName: str


# Add these endpoints to your main.py
from pydantic import BaseModel
from typing import Optional


# Create a model specifically for updates that makes fields optional
class FlightUpdateModel(BaseModel):
    originalId: Optional[str] = None
    startingAirport: Optional[str] = None
    destinationAirport: Optional[str] = None
    totalFare: Optional[float] = None
    totalTripDuration: Optional[int] = None


class SegmentUpdateModel(BaseModel):
    originalId: Optional[str] = None
    segmentsAirlineName: Optional[str] = None


@app.put("/flights/id/{original_id}")
async def update_flight(original_id: str, flight_update: dict):
    """Update a flight by its originalId"""
    from app.mongo_agent import update_one

    # Log the incoming request for debugging
    print(f"Updating flight with ID: {original_id}")
    print(f"Update data: {flight_update}")

    # Create the update query
    update_query = {"$set": flight_update}

    # Update the flight record
    result = update_one("flights_basic", {"originalId": original_id}, update_query)

    # Check if flight was found and updated
    if result["matched_count"] == 0:
        # Try alternative query if originalId isn't working
        print(f"Flight not found with originalId: {original_id}, trying alternative lookups")
        return {"success": False, "message": f"Flight with ID {original_id} not found"}



@app.put("/segments/id/{original_id}")
async def update_segment(original_id: str, segment: SegmentUpdateModel):
    """Update a flight segment by its originalId"""
    from app.mongo_agent import update_one

    # Extract update data, excluding any None values
    update_data = {k: v for k, v in segment.dict().items() if v is not None}

    # Create the update query
    update_query = {"$set": update_data}

    # Update the segment record
    result = update_one("flights_segments", {"originalId": original_id}, update_query)

    # Check if segment was found and updated
    if result["matched_count"] == 0:
        raise HTTPException(status_code=404, detail="Segment not found")

    return {"success": True, "message": f"Segment with ID {original_id} successfully updated"}


@app.delete("/flights/id/{original_id}")
async def delete_flight(original_id: str):
    """Delete a flight by its originalId"""
    from app.mongo_agent import delete_one

    # Delete the flight record
    flight_result = delete_one("flights_basic", {"originalId": original_id})

    # Check if flight was deleted
    if flight_result["deleted_count"] == 0:
        raise HTTPException(status_code=404, detail="Flight not found")

    return {"success": True, "message": f"Flight with ID {original_id} successfully deleted"}


@app.delete("/segments/id/{original_id}")
async def delete_segment(original_id: str):
    """Delete a flight segment by its originalId"""
    from app.mongo_agent import delete_one

    # Delete the segment record
    segment_result = delete_one("flights_segments", {"originalId": original_id})

    # Return success even if no segments were found (they might have been deleted already)
    return {"success": True, "message": f"Segment with ID {original_id} deleted if it existed"}


@app.get("/flights/list")
async def list_all_flights():
    """List all flights in the database"""
    from app.mongo_agent import find_with_projection

    # Get all flights with a high limit
    flights = find_with_projection("flights_basic", {}, limit=1000)

    # Return the list of flights with their IDs
    return [{"id": flight.get("originalId", "unknown"), "from": flight.get("startingAirport", ""),
             "to": flight.get("destinationAirport", "")} for flight in flights]


@app.get("/flights/id/{original_id}")
async def get_flight_by_id(original_id: str):
    """Get a flight by its originalId with segment details"""
    from app.mongo_agent import aggregate

    # Use aggregation to join flights and segments
    pipeline = [
        {"$match": {"originalId": original_id}},
        {"$lookup": {
            "from": "flights_segments",
            "localField": "originalId",
            "foreignField": "originalId",
            "as": "segmentDetails"
        }}
    ]

    flights = aggregate("flights_basic", pipeline)

    if not flights:
        raise HTTPException(status_code=404, detail="Flight not found")

    return flights


@app.put("/flights/id/{original_id}")
async def update_flight(original_id: str, flight_update: dict):
    """Update a flight by its originalId"""
    from app.mongo_agent import update_one, aggregate

    # Create the update query
    update_query = {"$set": flight_update}

    # Update the flight record
    result = update_one("flights_basic", {"originalId": original_id}, update_query)

    # Check if flight was found and updated
    if result["matched_count"] == 0:
        raise HTTPException(status_code=404, detail="Flight not found")

    # Return the updated flight with segment details
    pipeline = [
        {"$match": {"originalId": original_id}},
        {"$lookup": {
            "from": "flights_segments",
            "localField": "originalId",
            "foreignField": "originalId",
            "as": "segmentDetails"
        }}
    ]

    updated_flight = aggregate("flights_basic", pipeline)

    return updated_flight[0] if updated_flight else {"message": "Flight updated but could not retrieve details"}
