import os
import subprocess
from typing import List, Union

from app.core.clauses.clause import Clause
from app.core.translator.query_translator import QueryTranslator


class Neo4jCypherQueryTranslator(QueryTranslator):
    """Translator for converting SQL queries to Neo4j Cypher queries."""

    def __init__(self, jar_path: str = None):
        """
        Initialize the Neo4j SQL to Cypher translator.

        Args:
            jar_path: Optional path to the neo4j-jdbc-full-bundle jar file.
                     If not provided, uses the default path.
        """
        if jar_path is None:
            # Default path relative to the project root
            current_dir = os.path.dirname(os.path.abspath(__file__))
            jar_path = os.path.join(
                os.path.dirname(current_dir),
                "neo4j-jdbc-full-bundle-6.9.1.jar"
            )

        # Get the directory containing the Sql2CypherCLI.class (parent directory of translator/)
        cli_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        self.jar_classpath = f"{jar_path}:{cli_dir}"
        self.java_cmd = ["java", "-cp", self.jar_classpath, "Sql2CypherCLI"]

    def translate(self, query_pattern: Union[List[Clause], str]) -> str:
        """
        Translate SQL query to Cypher query.

        Args:
            query_pattern: Either a List[Clause] (for compatibility with QueryTranslator)
                          or a SQL string.

        Returns:
            Cypher query string, or None if translation fails.
        """
        # Handle both string (SQL) and List[Clause] inputs
        if isinstance(query_pattern, list):
            # If it's a list of clauses, we can't translate
            # this translator only handles SQL strings
            raise TypeError(
                "Neo4jCypherQueryTranslator only accepts SQL strings, "
                "not List[Clause]. Use translate_sql() method directly."
            )

        return self._translate_sql(query_pattern)

    def _translate_sql(self, sql: str) -> str:
        """
        Internal method to translate SQL to Cypher using the Java CLI tool.

        Args:
            sql: SQL query string.

        Returns:
            Cypher query string, or None if translation fails.
        """
        try:
            result = subprocess.run(
                self.java_cmd + [sql],
                capture_output=True,
                text=True,
                timeout=10
            )
            cypher = result.stdout.strip()
            if not cypher or result.returncode != 0:
                return None
            if "Exception" in cypher or "ERROR" in cypher:
                return None
            return cypher
        except (subprocess.TimeoutExpired, Exception):
            return None

    def grammar_check(self, query: str) -> bool:
        """
        Check if a Cypher query is syntactically valid.

        Note: This is a basic implementation. For full Cypher grammar checking,
        you would need to integrate a Cypher parser.

        Args:
            query: Cypher query string to check.

        Returns:
            True if the query appears valid, False otherwise.
        """
        # Basic check - ensure query is not empty and has basic Cypher keywords
        if not query or not query.strip():
            return False

        query_upper = query.upper()

        # Check for at least one Cypher keyword
        cypher_keywords = [
            "MATCH", "CREATE", "MERGE", "RETURN", "WHERE", "WITH",
            "DELETE", "DETACH", "SET", "REMOVE", "ORDER", "LIMIT",
            "SKIP", "UNWIND", "UNION", "CALL", "FOREACH"
        ]

        has_keyword = any(keyword in query_upper for keyword in cypher_keywords)

        # Check for balanced parentheses
        paren_count = query.count("(") - query.count(")")
        if paren_count != 0:
            return False

        return has_keyword
