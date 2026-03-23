import sqlite3
from typing import Any, Dict, List, Tuple


class SQLiteDBClient:
    """
    SQLite database client for connecting to SQLite databases and fetching metadata.
    """

    def __init__(self, db_path: str):
        """
        Initialize SQLite database client.

        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn: sqlite3.Connection | None = None
        self.cursor: sqlite3.Cursor | None = None

    def connect(self) -> bool:
        """
        Establish database connection.

        Returns:
            True if connection succeeds, False otherwise
        """
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            print(f"Successfully connected to SQLite database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            print(f"Failed to connect to SQLite database: {e}")
            return False

    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("SQLite database connection closed")

    def get_tables(self) -> List[str]:
        """
        Get all table names in the database.

        Returns:
            List of table names
        """
        if not self.cursor:
            raise RuntimeError("Database not connected")

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in self.cursor.fetchall()]
        return tables

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get all column names for a table.

        Args:
            table_name: Table name

        Returns:
            List of column names
        """
        if not self.cursor:
            raise RuntimeError("Database not connected")

        self.cursor.execute(f'PRAGMA table_info("{table_name}");')
        cols_info = self.cursor.fetchall()
        colnames = [col[1] for col in cols_info]
        return colnames

    def get_primary_key(self, table_name: str) -> str:
        """
        Get the primary key column name for a table.

        Args:
            table_name: Table name

        Returns:
            Primary key column name
        """
        if not self.cursor:
            raise RuntimeError("Database not connected")

        self.cursor.execute(f'PRAGMA table_info("{table_name}");')
        cols_info = self.cursor.fetchall()
        pks = [col[1] for col in cols_info if col[5] == 1]
        return pks[0] if pks else None

    def get_foreign_keys(self, table_name: str) -> List[Tuple[str, str, str]]:
        """
        Get foreign key information for a table.

        Args:
            table_name: Table name

        Returns:
            List of foreign keys, each FK is a tuple (ref_table, from_col, to_col)
        """
        if not self.cursor:
            raise RuntimeError("Database not connected")

        # PRAGMA foreign_key_list returns: (id, seq, table, from, to, on_update, on_delete, match)
        self.cursor.execute(f'PRAGMA foreign_key_list("{table_name}");')
        raw_fks = self.cursor.fetchall()
        fks = [(fk[2], fk[3], fk[4]) for fk in raw_fks]  # (ref_table, from_col, to_col)
        return fks

    def get_table_rows(self, table_name: str) -> List[Tuple]:
        """
        Get all data rows from a table.

        Args:
            table_name: Table name

        Returns:
            List of data rows
        """
        if not self.cursor:
            raise RuntimeError("Database not connected")

        self.cursor.execute(f'SELECT * FROM "{table_name}"')
        rows = self.cursor.fetchall()
        return rows

    def fetch_metadata(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all metadata from the database, including table structure,
        primary keys, foreign keys, and data.

        Returns:
            Metadata dictionary with table names as keys and table info as values
        """
        tables = self.get_tables()
        print(f"Found tables in SQLite: {tables}")

        meta = {}
        for table in tables:
            colnames = self.get_table_columns(table)
            pk_col = self.get_primary_key(table)
            fks = self.get_foreign_keys(table)
            rows = self.get_table_rows(table)

            meta[table] = {
                "colnames": colnames,
                "pk_col": pk_col,
                "fks": fks,
                "rows": rows
            }

        return meta

    def execute_query(self, query: str) -> List[Tuple]:
        """
        Execute SQL query and return results.

        Args:
            query: SQL query statement

        Returns:
            Query results as a list
        """
        if not self.cursor:
            raise RuntimeError("Database not connected")

        self.cursor.execute(query)
        return self.cursor.fetchall()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.disconnect()
