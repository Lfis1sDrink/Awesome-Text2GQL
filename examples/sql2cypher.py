import json
from pathlib import Path

from app.core.validator.db_client import QueryStatus
from app.impl.neo4j_cypher.db_client.neo4j_db_client import Neo4jDBClient
from app.impl.neo4j_cypher.translator.neo4j_cypher_query_translator import (
    Neo4jCypherQueryTranslator,
)

# Filter SQL queries with db_id = TARGET_DB from train.json, translate them to Cypher,
# execute them, and finally write the results to a JSON file.
JSON_FILE = "examples/sql2cypher_asset/sql2cypher_example.json"

# The target Neo4j database (subdomain) must be created beforehand! 
# Use:'CREATE DATABASE sql2cypherexample'
TARGET_DB = "sql2cypherexample"

# The db_id value in the train.json that we want to filter for and process.
DBid_InBIRD = "sql2cypherexample"

OUTPUT_FILE = Path(f"examples/sql2cypher_asset/results/{TARGET_DB}_sql2cypher_results.json")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# Neo4j Connection Configuration
NEO4J_URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PWD = "password"

# Create Neo4j DB Client
db_client_params = {
    "uri": NEO4J_URI,
    "user": NEO4J_USER,
    "password": NEO4J_PWD,
    "database": TARGET_DB,
}
neo4j_client = Neo4jDBClient(db_client_params)


# Main Logic
def main():
    print(f"Reading train.json, filtering for db_id = '{DBid_InBIRD}' SQL queries")

    sql2cypher_translator = Neo4jCypherQueryTranslator()

    with open(JSON_FILE, encoding="utf-8") as f:
        data = json.load(f)

    filtered = [row for row in data if row["db_id"].lower() == DBid_InBIRD]

    print(f"Found {len(filtered)} SQL records")

    results = []

    # Process each record
    for i, row in enumerate(filtered, start=1):
        sql = row["SQL"].strip()
        question = row.get("question", "")

        print(f"\n===== {i}/{len(filtered)} =====")
        print("Question:", question)
        print("SQL:", sql)

        # ---- Translation ----
        cypher = sql2cypher_translator.translate(sql)
        if not cypher:
            print("Translation Failed")
            results.append(
                {
                    "question": question,
                    "sql": sql,
                    "cypher": "",
                    "status": "FAIL_TRANSLATE",
                    "result": None,
                }
            )
            continue

        print("Cypher:", cypher)

        # ---- Execution ----
        exec_result = neo4j_client.execute_query(cypher)

        if exec_result.status_code == QueryStatus.SUCCESS:
            print("Execution Successful, records returned:", len(exec_result.data))
            results.append(
                {
                    "question": question,
                    "sql": sql,
                    "cypher": cypher,
                    "status": "OK",
                    "result": exec_result.data,
                }
            )
        else:
            print("Execution Failed:", exec_result.error)
            results.append(
                {
                    "question": question,
                    "sql": sql,
                    "cypher": cypher,
                    "status": "EXEC_ERROR",
                    "result": exec_result.error,
                }
            )

    # Write to JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    print(f"\nFinished! Results written to {OUTPUT_FILE}")

    # Close Neo4j client connection
    neo4j_client.close()


if __name__ == "__main__":
    main()
