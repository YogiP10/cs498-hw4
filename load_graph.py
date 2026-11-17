import csv
import os

from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

query = """
UNWIND $rows AS row
MERGE (d:Driver {driver_id: row.driver_id})
MERGE (c:Company {name: row.company})
MERGE (d)-[:WORKS_FOR]->(c)
MERGE (a:Area {area_id: row.dropoff_area})
CREATE (d)-[:TRIP {trip_id: row.trip_id, fare: row.fare, trip_seconds: row.trip_seconds}]->(a)
"""

rows = []
with open("taxi_trips_clean.csv") as f:
    for r in csv.DictReader(f):
        rows.append({
            "trip_id": r["trip_id"],
            "driver_id": r["driver_id"],
            "company": r["company"],
            "dropoff_area": int(r["dropoff_area"]),
            "fare": float(r["fare"]),
            "trip_seconds": int(r["trip_seconds"]),
        })

driver = GraphDatabase.driver(URI, auth=("neo4j", PASSWORD))

with driver.session() as s:
    s.run("MATCH (n) DETACH DELETE n")

    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Driver) REQUIRE d.driver_id IS UNIQUE")
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS UNIQUE")
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Area) REQUIRE a.area_id IS UNIQUE")

    for i in range(0, len(rows), 1000):
        s.run(query, rows=rows[i:i + 1000])

    counts = s.run("""
        MATCH (d:Driver) WITH count(d) AS drivers
        MATCH (c:Company) WITH drivers, count(c) AS companies
        MATCH (a:Area) WITH drivers, companies, count(a) AS areas
        MATCH ()-[t:TRIP]->()
        RETURN drivers, companies, areas, count(t) AS trips
    """).single()
    print(dict(counts))

driver.close()
