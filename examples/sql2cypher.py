import csv
import json
import subprocess

from neo4j import GraphDatabase

# Filter SQL queries with db_id = TARGET_DB from train.json, translate them to Cypher,
# execute them, and finally write the results to a CSV file.
JSON_FILE = "/Users/gedion/Code/sql2cypher-neo4j/train/train.json"
TARGET_DB = "books"
DBid_InBIRD = "books"

JAR_CLASSPATH = "neo4j-jdbc-full-bundle-6.9.1.jar:Sql2CypherCLI"
JAVA_CMD = ["java", "-cp", JAR_CLASSPATH, "Sql2CypherCLI"]

OUTPUT_FILE = f"{TARGET_DB}_sql2cypher_results.csv"

# Neo4j Connection Configuration
NEO4J_URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PWD = "password"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PWD), database=TARGET_DB)


# SQL -> Cypher Translation
def translate_sql(sql: str):
    try:
        result = subprocess.run(JAVA_CMD + [sql], capture_output=True, text=True, timeout=10)
        cypher = result.stdout.strip()
        if not cypher or result.returncode != 0:
            return None
        if "Exception" in cypher:
            return None
        return cypher
    except Exception:
        return None


# Execute Cypher
def execute_cypher(cypher: str):
    try:
        with driver.session() as session:
            result = session.run(cypher)
            data = result.data()
            return {"status": "OK", "result": data}
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}


# Main Logic
def main():
    print(f"Reading train.json, filtering for db_id = '{DBid_InBIRD}' SQL queries")

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
        cypher = translate_sql(sql)
        if not cypher:
            print("Translation Failed")
            results.append([question, sql, "", "FAIL_TRANSLATE", ""])
            continue

        print("Cypher:", cypher)

        # ---- Execution ----
        exec_result = execute_cypher(cypher)

        if exec_result["status"] == "OK":
            print("Execution Successful, records returned:", len(exec_result["result"]))
            results.append([question, sql, cypher, "OK", exec_result["result"]])
        else:
            print("Execution Failed:", exec_result["error"])
            results.append([question, sql, cypher, "EXEC_ERROR", exec_result["error"]])

    # Write to CSV (including question column)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Question", "SQL", "Cypher", "Status", "Result/Error"])
        for r in results:
            writer.writerow(r)

    print(f"\nFinished! Results written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
