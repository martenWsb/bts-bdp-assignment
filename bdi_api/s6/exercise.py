from __future__ import annotations
import os
import pymongo
from typing import Annotated
from fastapi import APIRouter, status, HTTPException
from fastapi.params import Query
from pydantic import BaseModel
from typing import Optional



# 1. Use the environment variable for the URL
mongo_url = os.getenv("BDI_MONGO_URL")
client = pymongo.MongoClient(mongo_url)

# 2. Database: bdi_aircraft | Collection: positions
db = client["bdi_aircraft"]
collection = db["positions"]

# settings = Settings()

s6 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s6",
    tags=["s6"],
)


class AircraftPosition(BaseModel):
    icao: str
    registration: Optional[str] = None
    type: str | None = None
    lat: float
    lon: float
    alt_baro: float | None = None
    ground_speed: float | None = None
    timestamp: str


@s6.post("/aircraft")
def create_aircraft(position: AircraftPosition) -> dict:
    """Store an aircraft position document in MongoDB.

    Use the BDI_MONGO_URL environment variable to configure the connection.
    Start MongoDB with: make mongo
    Database name: bdi_aircraft
    Collection name: positions
    """
    # TODO: Connect to MongoDB using pymongo.MongoClient(settings.mongo_url)
    # TODO: Insert the position document into the 'positions' collection
    # TODO: Return {"status": "ok"}
    collection.insert_one(position.model_dump())
    return {"status": "ok"}



@s6.get("/aircraft/")
def list_aircraft(
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> list[dict]:
    # 1. Calculate how many to skip based on the page number
    skip_amount = (page - 1) * page_size
    
    pipeline = [
        # 2. Group by 'icao' to get unique aircraft. 
        # We take the 'first' registration and type we find for that ICAO.
        {
            "$group": {
                "_id": "$icao",
                "registration": {"$first": "$registration"},
                "type": {"$first": "$type"}
            }
        },
        # 3. Clean up the output: Rename _id back to icao and hide the internal _id
        {
            "$project": {
                "_id": 0,
                "icao": "$_id",
                "registration": 1,
                "type": 1
            }
        },
        # 4. Apply pagination
        {"$skip": skip_amount},
        {"$limit": page_size}
    ]
    
    return list(collection.aggregate(pipeline))


@s6.get("/aircraft/stats")
def aircraft_stats() -> list[dict]:
    """Return aggregated statistics: count of positions grouped by aircraft type."""
    
    pipeline = [
        # 1. Group by the 'type' field and count occurrences
        {
            "$group": {
                "_id": "$type", 
                "count": {"$sum": 1}
            }
        },
        # 2. Reshape the output to match the expected example
        {
            "$project": {
                "_id": 0,           # Exclude the internal _id
                "type": "$_id",     # Move the group key to 'type'
                "count": 1          # Keep the count
            }
        },
        # 3. Sort by count descending as requested
        {
            "$sort": {"count": -1}
        }
    ]
    
    # Execute the aggregation and return as a list
    return list(collection.aggregate(pipeline))


@s6.get("/aircraft/{icao}")
def get_aircraft(icao: str) -> dict:
    """Return the latest position for a specific aircraft."""
    
    # We find the most recent entry by sorting by timestamp descending
    aircraft = collection.find_one(
        {"icao": icao}, 
        {"_id": 0}, # Hide the MongoDB ID
        sort=[("timestamp", pymongo.DESCENDING)]
    )
    
    if not aircraft:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Aircraft {icao} not found"
        )
        
    return aircraft




@s6.delete("/aircraft/{icao}")
def delete_aircraft(icao: str) -> dict:
    """Remove all position records for an aircraft.

    Returns the number of deleted documents.
    """
    # Use delete_many to remove all snapshots of this specific aircraft
    result = collection.delete_many({"icao": icao})
    
    # Return the count of documents removed from the database
    return {"deleted": result.deleted_count}

























@s6.delete("/aircraft/{icao}")
def delete_aircraft(icao: str) -> dict:
    """Remove all position records for an aircraft.

    Returns the number of deleted documents.
    """
    # TODO: Connect to MongoDB
    # TODO: Delete all documents matching the icao
    # TODO: Return {"deleted": <count>}
    return {"deleted": 0}