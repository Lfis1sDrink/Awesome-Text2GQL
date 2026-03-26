from typing import Any, Dict

from neo4j import GraphDatabase
from neo4j.exceptions import CypherSyntaxError, DriverError, ServiceUnavailable

from app.core.validator.db_client import DB_Client, QueryResult, QueryStatus


class Neo4jDBClient(DB_Client):
    """
    Neo4jDBClient is a concrete implementation of DB_Client
    for connecting to and operating Neo4j database.
    """

    def __init__(self, db_client_params: Dict[str, Any]):
        """
        Initialize Neo4jDBClient instance and create database connection.
        Implements the functionality of the abstract method create_client.

        Args:
            db_client_params: Dictionary containing connection parameters:
                - uri: Neo4j connection URI (e.g., "neo4j://localhost:7687")
                - user: Neo4j username
                - password: Neo4j password
                - database: Database name (optional, defaults to "neo4j")
                - create_if_not_exists: Whether to create database if it doesn't exist
        """
        self.driver: GraphDatabase.driver | None = None
        self.db_client_params = db_client_params
        # Call internal method to handle connection creation logic
        self.driver = self.create_client(db_client_params)

    def create_client(self, db_client_params: Dict[str, Any]) -> GraphDatabase.driver | None:
        """
        Create a new Neo4j driver instance (implementing abstract method).

        Args:
            db_client_params: Dictionary containing connection parameters.

        Returns:
            Neo4j driver instance or None if connection fails.
        """
        uri = db_client_params.get("uri", "neo4j://localhost:7687")
        user = db_client_params.get("user", "neo4j")
        password = db_client_params.get("password", "")
        database = db_client_params.get("database", "neo4j")
        create_if_not_exists = db_client_params.get("create_if_not_exists", True)

        try:
            driver = GraphDatabase.driver(uri, auth=(user, password), database=database)

            # Execute a simple test query to verify connection
            with driver.session() as session:
                session.run("RETURN 1")

            print(f"Successfully created Neo4jDBClient for database '{database}'.")
            return driver

        except (ServiceUnavailable, DriverError) as e:
            error_msg = str(e)

            # Check if the error is due to database not existing
            # Neo4j error message for non-existent database 
            # typically contains "Database does not exist"
            # or "Database <name> does not exist"
            if (
                "does not exist" in error_msg.lower()
                 or "database not found" in error_msg.lower()
                 ) and create_if_not_exists:
                print(f"Database '{database}' does not exist. Attempting to create it...")
                if self._create_database(uri, user, password, database):
                    # Retry connection after creating database
                    return self.create_client({**db_client_params, "create_if_not_exists": False})

            # Connection failed
            print(f"Failed to create Neo4jDBClient: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error creating Neo4jDBClient: {e}")
            return None

    def _create_database(self, uri: str, user: str, password: str, database: str) -> bool:
        """
        Create a Neo4j database using the system database.

        Args:
            uri: Neo4j connection URI
            user: Neo4j username
            password: Neo4j password
            database: Name of the database to create

        Returns:
            True if database was created successfully, False otherwise
        """
        try:
            # Connect to the system database to create a new database
            # Note: This only works with Neo4j Enterprise Edition
            driver = GraphDatabase.driver(uri, auth=(user, password), database="system")

            with driver.session() as session:
                # Check if database already exists
                result = session.run("SHOW DATABASES")
                existing_dbs = [record["name"] for record in result]

                if database in existing_dbs:
                    print(f"Database '{database}' already exists.")
                    driver.close()
                    return True

                # Create the database
                session.run(f"CREATE DATABASE {database} IF NOT EXISTS")
                print(f"Successfully created database '{database}'.")

            driver.close()
            return True

        except Exception as e:
            print(f"Failed to create database '{database}': {e}")
            print("Note: Multi-database support requires Neo4j Enterprise Edition.")
            print("For Neo4j Community Edition, only the default 'neo4j' database is available.")
            return False

    def execute_query(self, query: str, parameters: Dict[str, Any] = None) -> QueryResult:
        """
        Execute a Neo4j Cypher query statement.

        Args:
            query: Query string to execute.
            parameters: Optional query parameters.

        Returns:
            Unified QueryResult object.
        """
        if not self.driver:
            # Database client not successfully initialized
            return QueryResult(
                status_code=QueryStatus.SERVER_ERROR,
                error="Neo4j driver is not initialized or connection failed.",
            )

        try:
            with self.driver.session() as session:
                if parameters:
                    result = session.run(query, parameters)
                else:
                    result = session.run(query)
                data = result.data()

                # Check if no records were returned
                if not data:
                    return QueryResult(status_code=QueryStatus.NO_RECORD, data=[])

                # Successfully executed and returned data
                return QueryResult(status_code=QueryStatus.SUCCESS, data=data)

        except CypherSyntaxError as e:
            # Cypher syntax error - client error
            error_msg = f"Cypher syntax error: {e.message}"
            print(f"Error executing query: {error_msg}")
            return QueryResult(status_code=QueryStatus.CLIENT_ERROR, error=error_msg)

        except (ServiceUnavailable, DriverError) as e:
            # Connection/server error
            error_msg = f"Neo4j connection error: {str(e)}"
            print(f"Error executing query: {error_msg}")
            return QueryResult(status_code=QueryStatus.SERVER_ERROR, error=error_msg)

        except Exception as e:
            # Other errors
            error_msg = str(e)
            print(f"Error executing query: {error_msg}")

            # Determine error type based on error message
            if "syntax" in error_msg.lower() or "cypher" in error_msg.lower():
                status_code = QueryStatus.CLIENT_ERROR
            else:
                status_code = QueryStatus.SERVER_ERROR

            return QueryResult(status_code=status_code, error=error_msg)

    def close(self):
        """Close the Neo4j driver connection."""
        if self.driver:
            self.driver.close()
            self.driver = None
            print("Neo4j driver connection closed.")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close the connection."""
        self.close()
