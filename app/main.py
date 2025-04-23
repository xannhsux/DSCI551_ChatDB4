from fastapi import FastAPI, Query, HTTPException
from typing import List, Dict, Any, Optional
import logging
from pydantic import BaseModel

from .mongo_agent import (
    get_all_flights,
    get_flights_by_airports,
    get_flights_by_airline,
    find_with_projection,
    insert_one as mongo_insert_one,
    update_one as mongo_update_one,
    delete_one as mongo_delete_one
)
from .sql_agent import (
    get_all_reviews,
    get_reviews_by_county,
    get_reviews_by_state,
    find_hotels_with_min_rating,
    add_hotel,
    update_hotel,
    delete_hotel,
    search_hotels_by_name,
    get_hotel_by_id
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Travel Database API")


@app.get("/", response_model=Dict[str, str])
def read_root():
    return {"message": "Welcome to the Travel Database API"}


# Flight endpoints
@app.get("/flights", response_model=List[Dict[str, Any]])
def get_flights(limit: int = 100):
    """
    Get all flights from the MongoDB database
    """
    try:
        flights = get_all_flights(limit)
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
                "rating": hotel[0],  # Updated field name with space
                "service": hotel[2],
                "rooms": hotel[3],
                "cleanliness": hotel[4],
                "value": hotel[5],
                "hotel_name": hotel[6],
                "county": hotel[7],
                "state": hotel[8],
                "ID": hotel[9] if len(hotel) > 9 else None  # Include ID if available
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
                "rating": hotel[0],  # Updated field name with space
                "service": hotel[2],
                "rooms": hotel[3],
                "cleanliness": hotel[4],
                "value": hotel[5],
                "hotel_name": hotel[6],
                "county": hotel[7],
                "state": hotel[8],
                "ID": hotel[9] if len(hotel) > 9 else None  # Include ID if available
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
                "state": hotel[8],
                "ID": hotel[9] if len(hotel) > 9 else None  # Include ID if available
            })

        logger.info(f"Found {len(result)} hotels in state: {clean_state}")
        return result
    except Exception as e:
        logger.error(f"Error retrieving hotels for state {state}: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving hotels: {str(e)}")


# Define model for hotel data
class HotelModel(BaseModel):
    hotel_name: str
    county: str
    state: str
    rating: float
    cleanliness: float
    service: float
    rooms: float


@app.post("/hotels", status_code=201)
async def create_new_hotel(hotel: HotelModel):
    """Add a new hotel to the database"""
    try:
        hotel_id = add_hotel(
            hotel.hotel_name,
            hotel.county,
            hotel.state,
            hotel.rating,
            hotel.service,
            hotel.rooms,
            hotel.cleanliness
        )
        return {"success": True, "message": "Hotel added successfully", "id": hotel_id}
    except Exception as e:
        logger.error(f"Error adding hotel: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to add hotel: {str(e)}")


@app.put("/hotels/{hotel_id}")
async def update_existing_hotel(hotel_id: int, hotel: HotelModel):
    """Update a hotel by its ID"""
    try:
        success = update_hotel(
            hotel_id,
            hotel.hotel_name,
            hotel.county,
            hotel.state,
            hotel.rating,
            hotel.service,
            hotel.rooms,
            hotel.cleanliness
        )
        if success:
            return {"success": True, "message": f"Hotel with ID {hotel_id} successfully updated"}
        else:
            raise HTTPException(status_code=404, detail=f"Hotel with ID {hotel_id} not found")
    except Exception as e:
        logger.error(f"Error updating hotel: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update hotel: {str(e)}")


@app.delete("/hotels/{hotel_id}")
async def delete_existing_hotel(hotel_id: int):
    """Delete a hotel by its ID"""
    try:
        success = delete_hotel(hotel_id)
        if success:
            return {"success": True, "message": f"Hotel with ID {hotel_id} successfully deleted"}
        else:
            raise HTTPException(status_code=404, detail=f"Hotel with ID {hotel_id} not found")
    except Exception as e:
        logger.error(f"Error deleting hotel: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete hotel: {str(e)}")


@app.get("/hotels/{hotel_id}")
async def get_single_hotel(hotel_id: int):
    """Get a hotel by its ID"""
    try:
        hotel = get_hotel_by_id(hotel_id)
        if hotel:
            return hotel
        else:
            raise HTTPException(status_code=404, detail=f"Hotel with ID {hotel_id} not found")
    except Exception as e:
        logger.error(f"Error getting hotel: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get hotel: {str(e)}")


@app.get("/hotels/search", response_model=List[Dict[str, Any]])
async def find_hotels_by_name(name: str):
    """Search for hotels by name"""
    try:
        hotels = search_hotels_by_name(name)
        return hotels
    except Exception as e:
        logger.error(f"Error searching hotels: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to search hotels: {str(e)}")


# Flight CRUD operations
class FlightModel(BaseModel):
    originalId: str
    startingAirport: str
    destinationAirport: str
    totalFare: float
    totalTripDuration: int


class SegmentModel(BaseModel):
    originalId: str
    segmentsAirlineName: str


@app.post("/flights", status_code=201)
async def create_flight(flight: FlightModel):
    """Add a new flight to the database"""
    flight_data = flight.dict()
    result = mongo_insert_one("flights", flight_data)

    if result["acknowledged"]:
        return {"success": True, "message": "Flight added successfully", "id": result["inserted_id"]}
    else:
        raise HTTPException(status_code=500, detail="Failed to add flight")


@app.post("/segments", status_code=201)
async def create_segment(segment: SegmentModel):
    """Add a new flight segment to the database"""
    segment_data = segment.dict()
    result = mongo_insert_one("segments", segment_data)

    if result["acknowledged"]:
        return {"success": True, "message": "Segment added successfully", "id": result["inserted_id"]}
    else:
        raise HTTPException(status_code=500, detail="Failed to add segment")


@app.get("/flights/id/{original_id}")
async def get_flight_by_id(original_id: str):
    """Get a flight by its originalId"""
    flights = find_with_projection("flights", {"originalId": original_id})

    if not flights:
        raise HTTPException(status_code=404, detail="Flight not found")

    return flights


@app.get("/segments/id/{original_id}")
async def get_segment_by_id(original_id: str):
    """Get a flight segment by its originalId"""
    segments = find_with_projection("segments", {"originalId": original_id})

    if not segments:
        raise HTTPException(status_code=404, detail="Segment not found")

    return segments


@app.put("/flights/id/{original_id}")
async def update_flight(original_id: str, flight_update: dict):
    """Update a flight by its originalId"""
    # Create the update query
    update_query = {"$set": flight_update}

    # Update the flight record
    result = mongo_update_one("flights", {"originalId": original_id}, update_query)

    # Check if flight was found and updated
    if result["matched_count"] == 0:
        raise HTTPException(status_code=404, detail="Flight not found")

    return {"success": True, "message": f"Flight with ID {original_id} successfully updated"}


@app.put("/segments/id/{original_id}")
async def update_segment(original_id: str, segment_update: dict):
    """Update a flight segment by its originalId"""
    # Create the update query
    update_query = {"$set": segment_update}

    # Update the segment record
    result = mongo_update_one("segments", {"originalId": original_id}, update_query)

    # Check if segment was found and updated
    if result["matched_count"] == 0:
        raise HTTPException(status_code=404, detail="Segment not found")

    return {"success": True, "message": f"Segment with ID {original_id} successfully updated"}


@app.delete("/flights/id/{original_id}")
async def delete_flight(original_id: str):
    """Delete a flight by its originalId"""
    # Delete the flight record
    flight_result = mongo_delete_one("flights", {"originalId": original_id})

    # Check if flight was deleted
    if flight_result["deleted_count"] == 0:
        raise HTTPException(status_code=404, detail="Flight not found")

    return {"success": True, "message": f"Flight with ID {original_id} successfully deleted"}


@app.delete("/segments/id/{original_id}")
async def delete_segment(original_id: str):
    """Delete a flight segment by its originalId"""
    # Delete the segment record
    segment_result = mongo_delete_one("segments", {"originalId": original_id})

    # Return success even if no segments were found (they might have been deleted already)
    return {"success": True, "message": f"Segment with ID {original_id} deleted if it existed"}


@app.post("/execute_mongo_query")
async def execute_mongo_query(query_data: Dict[str, Any]):
    """Execute a MongoDB query"""
    try:
        collection = query_data.get("collection", "flights")
        query_string = query_data.get("query", "db.flights.find({}).limit(10)")

        # Extract the query part from the string
        import re

        if "flights_segments" in query_string or collection == "segments":
            collection_name = "segments"
        else:
            collection_name = "flights"

        # Extract query parameters - this is a simplified version
        query_params = {}
        sort_params = None

        # Check for find pattern
        find_pattern = r'find\(\s*(\{.*?\})\s*\)'
        find_match = re.search(find_pattern, query_string)
        if find_match:
            query_json = find_match.group(1)
            try:
                import json
                query_params = json.loads(query_json)
            except:
                # If parsing fails, use empty query
                query_params = {}

        # Check for sort pattern
        sort_pattern = r'sort\(\s*(\{.*?\})\s*\)'
        sort_match = re.search(sort_pattern, query_string)
        if sort_match:
            sort_json = sort_match.group(1)
            try:
                import json
                sort_params = json.loads(sort_json)
            except:
                sort_params = None

        # Get results based on collection
        if collection_name == "segments":
            results = find_with_projection("segments", query_params, limit=50)
        else:
            results = find_with_projection("flights", query_params, limit=50)

        return results
    except Exception as e:
        logger.error(f"Error executing MongoDB query: {e}")
        raise HTTPException(status_code=500, detail=f"Error executing MongoDB query: {str(e)}")