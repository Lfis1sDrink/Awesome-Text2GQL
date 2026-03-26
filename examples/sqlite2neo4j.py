from pathlib import Path

from app.impl.neo4j_cypher.db_client.neo4j_db_client import Neo4jDBClient
from app.impl.sqlite_sql.db_client.sqlite_db_client import SQLiteDBClient
from app.impl.sqlite_sql.migrator.sqlite_to_neo4j_migrator import SQLiteToNeo4jMigrator

"""
SQLite to Neo4j data migration example script.

This script demonstrates how to use SQLiteDBClient and SQLiteToNeo4jMigrator
to migrate SQLite database data to Neo4j graph database.
"""

# Note:
# 1. subdomain needs to match the database name in Neo4j
# and MUST be created beforehand! Use:'CREATE DATABASE sql2cypherexample'
# 2. In Neo4j, database names cannot contain special characters,
# such as underscores (_).
subdomain = "sql2cypherexample"  # e.g., "world", "olympics", "disney", "books"

# The actual SQLite database file path
SQLITE_PATH = Path("examples/sql2cypher_asset/sql2cypher_example.sqlite")

# Neo4j Connection Configuration
NEO4J_URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PWD = "password"

BATCH_SIZE = 5000


def main():
    print(f"Starting migration for: {subdomain}")

    # Create SQLite database client
    sqlite_client_params = {"db_path": SQLITE_PATH}

    sqlite_client = SQLiteDBClient(**sqlite_client_params)
    sqlite_client.connect()

    # Create Neo4j database client
    neo4j_db_client_params = {
        "uri": NEO4J_URI,
        "user": NEO4J_USER,
        "password": NEO4J_PWD,
        "database": subdomain,
    }

    # Note: The target Neo4j database (subdomain) must be created beforehand!
    neo4j_client = Neo4jDBClient(neo4j_db_client_params)

    # Create migrator instance
    migrator = SQLiteToNeo4jMigrator(
        sqlite_db_client=sqlite_client, neo4j_db_client=neo4j_client, batch_size=BATCH_SIZE
    )

    # Execute migration
    migrator.migrate(clear_before=True)

    # Clean up resources
    neo4j_client.close()
    sqlite_client.disconnect()


if __name__ == "__main__":
    main()
