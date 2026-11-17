import os

from flask import Flask, jsonify, request
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

app = Flask(__name__)

CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "taxi_trips_clean.csv")
neo = GraphDatabase.driver("bolt://localhost:7687",
                           auth=("neo4j", os.environ.get("NEO4J_PASSWORD", "password")))

# start spark once up front so the first request isn't slow
SparkSession.builder.appName("hw4").config("spark.driver.memory", "1g").getOrCreate()


def cypher(q, **params):
    with neo.session() as s:
        return [r.data() for r in s.run(q, **params)]


def load_trips():
    spark = SparkSession.builder.getOrCreate()
    return spark.read.csv(CSV, header=True, inferSchema=True)


# ---------- neo4j ----------

@app.route("/graph-summary")
def graph_summary():
    res = cypher("""
        MATCH (d:Driver) WITH count(d) AS driver_count
        MATCH (c:Company) WITH driver_count, count(c) AS company_count
        MATCH (a:Area) WITH driver_count, company_count, count(a) AS area_count
        MATCH ()-[t:TRIP]->()
        RETURN driver_count, company_count, area_count, count(t) AS trip_count
    """)
    return jsonify(res[0])


@app.route("/top-companies")
def top_companies():
    n = request.args.get("n", 5, type=int)
    res = cypher("""
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(:Area)
        RETURN c.name AS name, count(t) AS trip_count
        ORDER BY trip_count DESC, name
        LIMIT $n
    """, n=n)
    return jsonify({"companies": res})


@app.route("/high-fare-trips")
def high_fare_trips():
    area_id = request.args.get("area_id", type=int)
    min_fare = request.args.get("min_fare", 0.0, type=float)
    res = cypher("""
        MATCH (d:Driver)-[t:TRIP]->(:Area {area_id: $area_id})
        WHERE t.fare > $min_fare
        RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
        ORDER BY fare DESC, trip_id
    """, area_id=area_id, min_fare=min_fare)
    return jsonify({"trips": res})


@app.route("/co-area-drivers")
def co_area_drivers():
    driver_id = request.args.get("driver_id")
    res = cypher("""
        MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
        WHERE d2 <> d1
        RETURN d2.driver_id AS driver_id, count(DISTINCT a) AS shared_areas
        ORDER BY shared_areas DESC, driver_id
    """, driver_id=driver_id)
    return jsonify({"co_area_drivers": res})


@app.route("/avg-fare-by-company")
def avg_fare_by_company():
    res = cypher("""
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(:Area)
        RETURN c.name AS name, round(avg(t.fare), 2) AS avg_fare
        ORDER BY avg_fare DESC, name
    """)
    return jsonify({"companies": res})


@app.route("/area-stats")
def area_stats():
    area_id = request.args.get("area_id", type=int)
    df = load_trips()
    row = (df.filter(F.col("dropoff_area") == area_id)
             .groupBy()
             .agg(F.count("*").alias("trip_count"),
                  F.round(F.avg("fare"), 2).alias("avg_fare"),
                  F.round(F.avg("trip_seconds"), 2).alias("avg_trip_seconds"))
             .first())
    return jsonify({"area_id": area_id, **row.asDict()})


@app.route("/top-pickup-areas")
def top_pickup_areas():
    n = request.args.get("n", 5, type=int)
    df = load_trips()
    rows = (df.groupBy("pickup_area")
              .agg(F.count("*").alias("trip_count"))
              .orderBy(F.desc("trip_count"), "pickup_area")
              .limit(n)
              .collect())
    return jsonify({"areas": [r.asDict() for r in rows]})


@app.route("/company-compare")
def company_compare():
    c1 = request.args.get("company1", "")
    c2 = request.args.get("company2", "")

    spark = SparkSession.builder.getOrCreate()
    df = load_trips().withColumn("fare_per_minute", F.col("fare") / (F.col("trip_seconds") / 60.0))
    df.createOrReplaceTempView("trips")

    a, b = c1.replace("'", "\\'"), c2.replace("'", "\\'")
    rows = spark.sql(f"""
        SELECT company,
               COUNT(*) AS trip_count,
               ROUND(AVG(fare), 2) AS avg_fare,
               ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
               ROUND(AVG(trip_seconds), 2) AS avg_trip_seconds
        FROM trips
        WHERE company IN ('{a}', '{b}')
        GROUP BY company
    """).collect()

    found = {r["company"]: r.asDict() for r in rows}
    if c1 not in found or c2 not in found:
        return jsonify({"error": "one or more companies not found"})

    result = [found[c1]] if c1 == c2 else [found[c1], found[c2]]
    return jsonify({"comparison": result})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
