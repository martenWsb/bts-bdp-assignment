from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from neo4j import GraphDatabase

from bdi_api.settings import Settings

settings = Settings()

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)


class PersonCreate(BaseModel):
    name: str
    city: str
    age: int


class RelationshipCreate(BaseModel):
    from_person: str
    to_person: str
    relationship_type: str = "FRIENDS_WITH"


@s7.post("/graph/person")
def create_person(person: PersonCreate) -> dict:
    """Create a person node in Neo4J.

    Use the BDI_NEO4J_URL environment variable to configure the connection.
    Start Neo4J with: make neo4j
    """
    driver = GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))
    try:
        with driver.session() as session:
            session.run(
                "CREATE (p:Person {name: $name, city: $city, age: $age})",
                name=person.name,
                city=person.city,
                age=person.age,
            )
    finally:
        driver.close()
    return {"status": "ok", "name": person.name}


@s7.get("/graph/persons")
def list_persons() -> list[dict]:
    """List all person nodes.

    Each result should include: name, city, age.
    """
    driver = GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))
    try:
        with driver.session() as session:
            result = session.run("MATCH (p:Person) RETURN p.name as name, p.city as city, p.age as age")
            persons = [dict(record) for record in result]
    finally:
        driver.close()
    return persons


@s7.get("/graph/person/{name}/friends")
def get_friends(name: str) -> list[dict]:
    """Get friends of a person.

    Returns all persons connected by a FRIENDS_WITH relationship (any direction).
    If person not found, return 404.
    """
    driver = GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))
    try:
        with driver.session() as session:
            # Check if person exists
            result = session.run("MATCH (p:Person {name: $name}) RETURN p", name=name)
            if not result.fetch(1):
                raise HTTPException(status_code=404, detail=f"Person '{name}' not found")
            
            # Get friends
            result = session.run(
                "MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person) RETURN friend.name as name, friend.city as city, friend.age as age",
                name=name,
            )
            friends = [dict(record) for record in result]
    finally:
        driver.close()
    return friends


@s7.post("/graph/relationship")
def create_relationship(rel: RelationshipCreate) -> dict:
    """Create a relationship between two persons.

    Both persons must exist. Returns 404 if either is not found.
    """
    driver = GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))
    try:
        with driver.session() as session:
            # Check if both persons exist
            from_result = session.run("MATCH (p:Person {name: $name}) RETURN p", name=rel.from_person)
            if not from_result.fetch(1):
                raise HTTPException(status_code=404, detail=f"Person '{rel.from_person}' not found")
            
            to_result = session.run("MATCH (p:Person {name: $name}) RETURN p", name=rel.to_person)
            if not to_result.fetch(1):
                raise HTTPException(status_code=404, detail=f"Person '{rel.to_person}' not found")
            
            # Create relationship
            session.run(
                f"MATCH (a:Person {{name: $from}}), (b:Person {{name: $to}}) CREATE (a)-[:{rel.relationship_type}]->(b)",
                **{"from": rel.from_person, "to": rel.to_person},
            )
    finally:
        driver.close()
    return {"status": "ok", "from": rel.from_person, "to": rel.to_person}


@s7.get("/graph/person/{name}/recommendations")
def get_recommendations(name: str) -> list[dict]:
    """Get friend recommendations for a person.

    Recommend friends-of-friends who are NOT already direct friends.
    Return them sorted by number of mutual friends (descending).
    If person not found, return 404.

    Each result should include: name, city, mutual_friends (count).
    """
    driver = GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))
    try:
        with driver.session() as session:
            # Check if person exists
            result = session.run("MATCH (p:Person {name: $name}) RETURN p", name=name)
            if not result.fetch(1):
                raise HTTPException(status_code=404, detail=f"Person '{name}' not found")
            
            # Get friend recommendations: friends-of-friends not already direct friends
            result = session.run(
                "MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person)-[:FRIENDS_WITH]-(fof:Person) WHERE fof.name <> $name AND NOT (p)-[:FRIENDS_WITH]-(fof) RETURN fof.name as name, fof.city as city, count(friend) as mutual_friends ORDER BY mutual_friends DESC",
                name=name,
            )
            recommendations = [dict(record) for record in result]
    finally:
        driver.close()
    return recommendations
