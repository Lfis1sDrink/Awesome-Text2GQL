import sqlite3

from neo4j import GraphDatabase
from tqdm import tqdm

# subdomain needs to match the database name in Neo4j and must be created beforehand
subdomain = "books"  # e.g., "world", "olympics", "disney", "books"
SQLITE_PATH = "train/train_databases/books/books.sqlite"

# Neo4j Connection Configuration
NEO4J_URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PWD = "password"

BATCH_SIZE = 5000


def safe_label_for_neo4j(name: str) -> str:
    """
    Used only when writing Neo4j Labels.
    Preserves symbols like hyphens since Neo4j supports backtick wrapping.
    """
    if not name:
        return "Node"
    return name


def safe_prop_for_neo4j(name: str) -> str:
    """
    Used only when writing Neo4j Property Keys.
    Preserves original casing (World DB needs LifeExpectancy instead of lifeexpectancy).
    """
    if not name:
        return "prop"
    return name


def fetch_metadata(cur):
    """
    Fetches all metadata, using original table names strictly as Keys.
    """
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [x[0] for x in cur.fetchall()]

    meta = {}
    print(f"📌 Found tables in SQLite: {tables}")

    for table in tables:
        # 1. Columns
        cur.execute(f'PRAGMA table_info("{table}");')
        cols_info = cur.fetchall()
        colnames = [c[1] for c in cols_info]

        # 2. PK (Primary Key)
        pks = [c[1] for c in cols_info if c[5] == 1]
        pk_col = pks[0] if pks else colnames[0]

        # 3. Foreign Keys (table, from, to)
        # PRAGMA foreign_key_list returns: (id, seq, table, from, to, on_update, on_delete, match)
        cur.execute(f'PRAGMA foreign_key_list("{table}");')
        fks = []
        raw_fks = cur.fetchall()
        for fk in raw_fks:
            # fk[2]=ref_table, fk[3]=from_col, fk[4]=to_col
            fks.append((fk[2], fk[3], fk[4]))

        # 4. Data Rows
        # Wrap table names in quotes to prevent keyword conflicts
        cur.execute(f'SELECT * FROM "{table}"')
        rows = cur.fetchall()

        meta[table] = {"colnames": colnames, "pk_col": pk_col, "fks": fks, "rows": rows}
    return meta


def import_nodes(session, meta):
    print("\n=== 1. Importing Nodes ===")
    for table, data in meta.items():
        label = safe_label_for_neo4j(table)
        pk = data["pk_col"]
        colnames = data["colnames"]
        rows = data["rows"]

        # Property names keep original casing but are wrapped in backticks in Cypher
        # e.g. SET n.`LifeExpectancy` = row.`LifeExpectancy`
        props_clause = ", ".join([f"n.`{col}` = row.`{col}`" for col in colnames])

        # Indexing
        try:
            session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:`{label}`) ON (n.`{pk}`)")
        except Exception as e:
            print(f"  Index warning: {e}")

        cypher = f"""
        UNWIND $batch AS row
        MERGE (n:`{label}` {{ `{pk}`: row.`{pk}` }})
        SET {props_clause}
        """

        batch = []
        for row in tqdm(rows, desc=f"Import {table}", ncols=100):
            rec = dict(zip(colnames, row, strict=False))
            if rec.get(pk) is None:
                continue  # Skip if PK is null
            batch.append(rec)

            if len(batch) >= BATCH_SIZE:
                session.run(cypher, {"batch": batch})
                batch = []
        if batch:
            session.run(cypher, {"batch": batch})


def import_relationships(session, meta):
    print("\n=== 2. Creating Relationships ===")

    count_created = 0

    for table, data in meta.items():
        fks = data["fks"]
        # Strategy B: Flattening Association Tables (M:N)
        # If a table has 2 or more foreign keys, we treat it as a Join Table
        # e.g., games_competitor in Olympics
        if len(fks) >= 2:
            print(f"  ⚡️ Flattening Join Table: {table} (has {len(fks)} FKs)")
            # Simplified logic: use the first two FKs to establish a direct connection
            # Naming: Use [Original Table Name] as the edge type (matching Olympics s2c style)
            # e.g.: games_competitor

            fk1 = fks[0]  # (ref_table, from_col, to_col)
            fk2 = fks[1]

            ref1, col1_in_me, col1_in_ref = fk1
            ref2, col2_in_me, col2_in_ref = fk2

            if ref1 in meta and ref2 in meta:
                # Rel type: use table name directly, keeping original 
                # s2c uses lowercase games_competitor in Olympics
                rel_type = safe_label_for_neo4j(table)

                label1 = safe_label_for_neo4j(ref1)
                label2 = safe_label_for_neo4j(ref2)

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
                        if len(batch) >= BATCH_SIZE:
                            session.run(cypher, {"batch": batch})
                            batch = []
                if batch:
                    session.run(cypher, {"batch": batch})
                count_created += 1
                # After flattening, we usually skip building standard FK edges to avoid redundancy
                continue

        # Strategy A: Standard Foreign Key (1:N)
        # e.g., World: City.CountryCode -> Country.Code
        from_label = safe_label_for_neo4j(table)

        for fk in fks:
            ref_table, from_col, to_col = fk

            if ref_table not in meta:
                print(f"  Skipping FK in {table}: Target table '{ref_table}' not found in meta.")
                continue

            to_label = safe_label_for_neo4j(ref_table)

            # Naming fix: World DB throws error if expecting 'COUNTRYCODE'
            # Strategy: Take FK column name -> Convert to uppercase
            # e.g.: CountryCode -> COUNTRYCODE
            # e.g.: movie_title -> MOVIE_TITLE
            rel_type = from_col.upper()

            # Special case: if column name is "movie", uppercase is "MOVIE", matching Disney logs

            cypher = f"""
            UNWIND $batch AS row
            MATCH (a:`{from_label}` {{ `{from_col}`: row.val }})
            MATCH (b:`{to_label}`   {{ `{to_col}`:  row.val }})
            MERGE (a)-[:`{rel_type}`]->(b)
            """

            batch = []
            col_idx = data["colnames"].index(from_col)

            # Debug info
            # print(f"  Build Edge: ({table}).{from_col} -[{rel_type}]-> ({ref_table}).{to_col}")

            for row in data["rows"]:
                val = row[col_idx]
                if val is not None:
                    batch.append({"val": val})
                    if len(batch) >= BATCH_SIZE:
                        session.run(cypher, {"batch": batch})
                        batch = []
            if batch:
                session.run(cypher, {"batch": batch})
            count_created += 1

    print(f"\nRelationship processing finished. Processed patterns: {count_created}")


def main():
    print(f"Starting import for: {subdomain}")
    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PWD), database=subdomain)

    # Fetch all metadata
    meta = fetch_metadata(cur)

    with driver.session() as session:
        # Clear old data
        print("🧹 Clearing old data...")
        session.run("MATCH (n) DETACH DELETE n")

        # 1. Import Nodes
        import_nodes(session, meta)

        # 2. Import Relationships
        import_relationships(session, meta)

    driver.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
