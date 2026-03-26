from typing import Any, Dict

from tqdm import tqdm

from app.impl.neo4j_cypher.db_client.neo4j_db_client import Neo4jDBClient
from app.impl.sqlite_sql.db_client.sqlite_db_client import SQLiteDBClient


class SQLiteToNeo4jMigrator:
    """
    Data migrator from SQLite to Neo4j.
    Responsible for migrating SQLite database data and structure to Neo4j graph database.
    """

    def __init__(self, 
                 sqlite_db_client: SQLiteDBClient, 
                 neo4j_db_client: Neo4jDBClient, 
                 batch_size: int = 5000):
        """
        Initialize the migrator.

        Args:
            sqlite_db_client: SQLite database client
            neo4j_db_client: Neo4j database client
            batch_size: Batch processing size
        """
        self.sqlite_client = sqlite_db_client
        self.neo4j_client = neo4j_db_client
        self.batch_size = batch_size

    @staticmethod
    def safe_label_for_neo4j(name: str) -> str:
        """
        Generate a safe label name for Neo4j.
        Preserves symbols like hyphens since Neo4j supports backtick wrapping.

        Args:
            name: Original name

        Returns:
            Safe label name
        """
        if not name:
            return "Node"
        return name

    @staticmethod
    def safe_prop_for_neo4j(name: str) -> str:
        """
        Generate a safe property key name for Neo4j.
        Preserves original casing (e.g., World DB needs LifeExpectancy instead of lifeexpectancy).

        Args:
            name: Original name

        Returns:
            Safe property key name
        """
        if not name:
            return "prop"
        return name

    def clear_neo4j_database(self):
        """Clear all data in the Neo4j database."""
        from app.core.validator.db_client import QueryStatus

        print("Clearing old data...")
        result = self.neo4j_client.execute_query("MATCH (n) DETACH DELETE n")
        if result.status_code == QueryStatus.SUCCESS:
            print("Old data cleared")
        else:
            print(f"Failed to clear data: {result.error}")

    def import_nodes(self, meta: Dict[str, Dict[str, Any]]):
        """
        Import SQLite tables as nodes into Neo4j.

        Args:
            meta: Metadata dictionary
        """
        print("\n=== 1. Importing Nodes ===")
        from app.core.validator.db_client import QueryStatus

        for table, data in meta.items():
            label = self.safe_label_for_neo4j(table)
            pk = data["pk_col"]
            colnames = data["colnames"]
            rows = data["rows"]

            if pk is None:
                print(f"  Skipping table {table}: no primary key")
                continue

            # Property names keep original casing but are wrapped in backticks in Cypher
            # e.g.: SET n.`LifeExpectancy` = row.`LifeExpectancy`
            props_clause = ", ".join([f"n.`{col}` = row.`{col}`" for col in colnames])

            # Create index
            index_result = self.neo4j_client.execute_query(
                f"CREATE INDEX IF NOT EXISTS FOR (n:`{label}`) ON (n.`{pk}`)"
            )
            if index_result.status_code != QueryStatus.SUCCESS:
                print(f"  Index creation warning: {index_result.error}")

            cypher = f"""
            UNWIND $batch AS row
            MERGE (n:`{label}` {{ `{pk}`: row.`{pk}` }})
            SET {props_clause}
            """

            batch = []
            for row in tqdm(rows, desc=f"Import {table}", ncols=100):
                rec = dict(zip(colnames, row, strict=False))
                if rec.get(pk) is None:
                    continue  # Skip records with null primary key
                batch.append(rec)

                if len(batch) >= self.batch_size:
                    result = self.neo4j_client.execute_query(cypher, {"batch": batch})
                    if result.status_code != QueryStatus.SUCCESS:
                        print(f"  Batch insert failed: {result.error}")
                    batch = []

            if batch:
                result = self.neo4j_client.execute_query(cypher, {"batch": batch})
                if result.status_code != QueryStatus.SUCCESS:
                    print(f"  Batch insert failed: {result.error}")

    def import_relationships(self, meta: Dict[str, Dict[str, Any]]):
        """
        Create edges in Neo4j based on SQLite foreign key relationships.

        Args:
            meta: Metadata dictionary
        """
        print("\n=== 2. Creating Relationships ===")
        from app.core.validator.db_client import QueryStatus

        count_created = 0

        for table, data in meta.items():
            fks = data["fks"]

            # Strategy B: Flattening Association Tables (M:N)
            # If a table has 2 or more foreign keys, we treat it as a Join Table
            # e.g.: games_competitor in Olympics
            if len(fks) >= 2:
                print(f"  Flattening join table: {table} (has {len(fks)} FKs)")
                fk1 = fks[0]  # (ref_table, from_col, to_col)
                fk2 = fks[1]

                ref1, col1_in_me, col1_in_ref = fk1
                ref2, col2_in_me, col2_in_ref = fk2

                if ref1 in meta and ref2 in meta:
                    # Relationship type: use table name directly, keeping original format
                    # s2c uses lowercase games_competitor in Olympics
                    rel_type = self.safe_label_for_neo4j(table)

                    label1 = self.safe_label_for_neo4j(ref1)
                    label2 = self.safe_label_for_neo4j(ref2)

                    # Property mapping
                    props_clause = ", ".join([f"r.`{c}` = row.`{c}`" for c in data["colnames"]])

                    cypher = f"""
                    UNWIND $batch AS row
                    MATCH (n1:`{label1}` {{ `{col1_in_ref}`: row.`{col1_in_me}` }})
                    MATCH (n2:`{label2}` {{ `{col2_in_ref}`: row.`{col2_in_me}` }})
                    MERGE (n2)-[r:`{rel_type}`]->(n1)
                    SET {props_clause}
                    """

                    batch = []
                    for row in tqdm(data["rows"], desc=f"Flatten {table}", ncols=100):
                        rec = dict(zip(data["colnames"], row, strict=False))
                        # Check that foreign keys are not null
                        if rec.get(col1_in_me) is not None and rec.get(col2_in_me) is not None:
                            batch.append(rec)
                            if len(batch) >= self.batch_size:
                                result = self.neo4j_client.execute_query(cypher, {"batch": batch})
                                if result.status_code != QueryStatus.SUCCESS:
                                    print(f"  Batch relationship creation failed: {result.error}")
                                batch = []

                    if batch:
                        result = self.neo4j_client.execute_query(cypher, {"batch": batch})
                        if result.status_code != QueryStatus.SUCCESS:
                            print(f"  Batch relationship creation failed: {result.error}")

                    count_created += 1
                    # After flattening, 
                    # we usually skip building standard FK edges to avoid redundancy
                    continue

            # Strategy A: Standard Foreign Key (1:N)
            # e.g.: World: City.CountryCode -> Country.Code
            from_label = self.safe_label_for_neo4j(table)

            for fk in fks:
                ref_table, from_col, to_col = fk

                if ref_table not in meta:
                    print(f"  Skipping FK in {table}: "
                          f"Target table '{ref_table}' not found in metadata.")
                    continue

                to_label = self.safe_label_for_neo4j(ref_table)

                # Naming fix: World DB throws error if expecting 'COUNTRYCODE'
                # Strategy: Take FK column name -> Convert to uppercase
                # e.g.: CountryCode -> COUNTRYCODE
                # e.g.: movie_title -> MOVIE_TITLE
                rel_type = from_col.upper()

                cypher = f"""
                UNWIND $batch AS row
                MATCH (a:`{from_label}` {{ `{from_col}`: row.val }})
                MATCH (b:`{to_label}`   {{ `{to_col}`:  row.val }})
                MERGE (a)-[:`{rel_type}`]->(b)
                """

                batch = []
                col_idx = data["colnames"].index(from_col)

                for row in data["rows"]:
                    val = row[col_idx]
                    if val is not None:
                        batch.append({"val": val})
                        if len(batch) >= self.batch_size:
                            result = self.neo4j_client.execute_query(cypher, {"batch": batch})
                            if result.status_code != QueryStatus.SUCCESS:
                                print(f"  Batch relationship creation failed: {result.error}")
                            batch = []

                if batch:
                    result = self.neo4j_client.execute_query(cypher, {"batch": batch})
                    if result.status_code != QueryStatus.SUCCESS:
                        print(f"  Batch relationship creation failed: {result.error}")

                count_created += 1

        print(f"\nRelationship processing completed. "
              f"Processed {count_created} relationship patterns")

    def migrate(self, clear_before: bool = True):
        """
        Execute the complete data migration process.

        Args:
            clear_before: Whether to clear the Neo4j database before migration
        """
        # Fetch metadata
        meta = self.sqlite_client.fetch_metadata()

        if not meta:
            print("No tables found, migration aborted")
            return

        # Clear target database
        if clear_before:
            self.clear_neo4j_database()

        # Import nodes
        self.import_nodes(meta)

        # Import relationships
        self.import_relationships(meta)

        print("Migration completed!")
