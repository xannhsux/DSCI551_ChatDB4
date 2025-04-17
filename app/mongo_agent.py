from pymongo import MongoClient
import os

# Use environment variable if available, otherwise use the hardcoded URI
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://flightsdata:dsci551@flightsdata.y57hp.mongodb.net/?retryWrites=true&w=majority")

# Alternative connection string for local MongoDB
# Format: mongodb://username:password@host:port/
MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
MONGO_PORT = os.environ.get("MONGO_PORT", "27017")
LOCAL_MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"

def get_client():
    """
    Get a MongoDB client using either the cloud or local URI
    Try cloud first, fallback to local if that fails
    """
    try:
        # Try cloud MongoDB first
        client = MongoClient(MONGO_URI)
        # Test connection
        client.server_info()
        print("Connected to cloud MongoDB")
        return client
    except Exception as e:
        print(f"Error connecting to cloud MongoDB: {e}")
        try:
            # Try local MongoDB as fallback
            client = MongoClient(LOCAL_MONGO_URI)
            client.server_info()
            print("Connected to local MongoDB")
            return client
        except Exception as e:
            print(f"Error connecting to local MongoDB: {e}")
            # Re-raise the exception
            raise

# Get a client connection
client = get_client()
db = client["flights"]
collection = db["DSCI551_Project"]

def get_all_flights():
    """
    Get all flights from the collection
    """
    return list(collection.find({}, {"_id": 0}))

def get_flights_by_airports(starting, destination):
    """
    Get flights by starting and destination airports
    """
    return list(collection.find({
        "startingAirport": starting,
        "destinationAirport": destination
    }, {"_id": 0}))

def get_flights_by_airline(airline_name):
    """
    Get flights by airline name
    """
    return list(collection.find({
        "segmentsAirlineName": airline_name
    }, {"_id": 0}))