"""
Prompt templates of CorpusGenerator
"""

SYSTEM_PROMPT = """
You are an expert in graph databases and the Cypher query language. Your task is to generate new, high-quality, and diverse "natural language question-Cypher query" data pairs based on the provided graph schema and some validated query examples.
Please ensure that the Cypher queries you generate are syntactically correct and compatible with the provided graph schema.
Your output must be in strict JSON format, use English, as a list containing multiple objects.
"""  # noqa: E501


INSTRUCTION_TEMPLATE = """
# Command
Generate {num_per_iteration} new "question-query" data pairs based on the following information.

# 1. Graph Schema
This is the Schema definition of the graph you'll be working with:
```json
{schema_json}
```

2. Verified Query Examples (Context)
Here are some verified "question-query-result" examples that execute successfully. Use these as reference to understand the data patterns and query style in the graph. The result field shows partial data for reference.
```json
{examples_json}
```

3. Your Task
Now, based on the above Schema and examples, generate {num_per_iteration} new, more interesting, and potentially more complex "question-query" data pairs.
Please follow these guidelines:
Diversity: Create different types of queries, such as aggregations (COUNT, SUM, AVG), filtering (WHERE), multi-hop queries (MATCH (a)-[]->(b)-[]->(c)), optional matching (OPTIONAL MATCH), etc.
Increasing Complexity: Try to generate queries more complex than the examples, but ensure they are logically meaningful.
No Repetition: Do not generate items identical to the questions or queries in the examples above.
Strict Output Format: Your response must be a JSON list where each object contains both "question" and "query" keys. Do not add any explanations or comments outside the JSON content.

For example:
[
    {{
        "question": "(New natural language question 1)",
        "query": "(Corresponding Cypher query 1)"
    }},
    {{
        "question": "(New natural language question 2)",
        "query": "(Corresponding Cypher query 2)"
    }}
]
"""  # noqa: E501

ENHANCEMENT_PROMPT_TEMPLATE = """
# Command
Your task as a senior Cypher expert is to create more complex and insightful new "question-query" pairs based on existing queries.

# 1. Graph Schema
```json
{schema_json}
```
2. Verified Query Examples (Context)
Here are some verified, high-quality "question-query-result" pairs. They are your source of inspiration.

```json
{examples_json}
```

3. Your Task
Now, based on the above Schema and examples, generate {num_to_generate} new, more complex "question-query" data pairs.
Please follow these guidelines to increase complexity:

Combination Patterns: Combine query patterns from multiple examples. For instance, combine a filtering query with a multi-hop path query.

Increase Depth: Extend existing path queries by adding more hops (e.g., from A->B to A->B->C->D).

Use Advanced Functions: Introduce aggregation functions (COUNT, SUM, AVG, COLLECT), or use more complex logic in WHERE clauses (OR, NOT, IN).

Ask Deeper Questions: Move from "what" type questions to more analytical questions like "why", "how many types", "compare", etc.

No Repetition: Ensure newly generated questions and queries are significantly different from the examples.

4. Output Format
Return in JSON list format where each object contains both "question" and "query" keys.
For example:
[
    {{
        "question": "(New natural language question 1)",
        "query": "(Corresponding Cypher query 1)"
    }},
    {{
        "question": "(New natural language question 2)",
        "query": "(Corresponding Cypher query 2)"
    }}
]
"""  # noqa: E501


QUERY_ARCHETYPES = [
    "Aggregation and Counting: Statistics on certain types of nodes or relationships in the graph, such as calculating quantity, sum, average, maximum/minimum values. Example: 'Count the number of all type A nodes in the database.'",  # noqa: E501
    "Filtering and Sorting: Filter nodes that meet conditions based on one or more attribute values, and sort the results. Example: 'Find type A nodes where attribute X is greater than [some value] and attribute Y is [some string], sorted by attribute X in descending order.'",  # noqa: E501
    "Relationship Reachability Query: Query which other nodes can be reached from a specific node through specified relationships. Example: 'Which type B nodes have [R-type relationship] with the type A node named [instance name]?'",  # noqa: E501
    "Multi-hop Path Query: Query complex paths spanning two or more relationships. Example: 'Which type A nodes can connect to the type C node named [instance name] through type B nodes? (A->B->C)'",  # noqa: E501
    "Common Neighbors and Association Analysis: Find whether two or more nodes are connected through the same intermediate node, often used to discover indirect connections. Example: 'Which type A nodes and another type A node named [instance name] are both connected to the same type B node? (A1->B<-A2)'",  # noqa: E501
    "Existence and Boolean Checks: Check whether nodes or patterns that meet specific conditions exist in the graph, typically returning yes or no. Example: 'Does the database contain a type A node whose attribute X value is [some specific value]?'",  # noqa: E501
    "Attribute Comparison Query: Filter other nodes based on comparisons between different nodes or based on a node's attributes. Example: 'Find all other type A nodes whose attribute X value is greater than that of the type A node named [instance name].'",  # noqa: E501
    "Path Analysis and Traversal: Focus on analysis of paths themselves, such as finding the shortest path or all possible paths. Example: 'Find the shortest path between the type A node named [instance A] and the type B node named [instance B].'",  # noqa: E501
]


QUERY_TEMPLATE = [
    # 1. Basic node query
    """Node query with conditions: MATCH (n:label_1) WHERE n.prop_1 = "value_1" RETURN n.prop_1 LIMIT 10""",# noqa: E501
    """Node query exact match: MATCH (n:label_1) WHERE n.prop_1 = "value_1" RETURN n.id, n.prop_1 LIMIT 10""",# noqa: E501
    """Node query numeric filtering: MATCH (n:label_1) WHERE n.age > 20 RETURN n.name, n.age, n.email""",# noqa: E501
    """Node query numeric range: MATCH (n:label_1) WHERE n.price >= 100 AND n.price <= 500 RETURN n.product_id, n.price""",# noqa: E501
    """Node query starts with: MATCH (n:label_1) WHERE n.name STARTS WITH "A" RETURN n.id, n.name""",# noqa: E501
    """Node query contains: MATCH (n:label_1) WHERE n.description CONTAINS "tech" RETURN n.description LIMIT 5""",# noqa: E501
    """Node query existence check: MATCH (n:label_1) WHERE n.email IS NOT NULL RETURN n.id, n.email""",# noqa: E501
    """Node query IN list: MATCH (n:label_1) WHERE n.status IN ["active", "pending"] RETURN n.id, n.status""",# noqa: E501
    """One-hop relationship: MATCH (a:label_1)-[r:edge_1]->(b:label_2) RETURN a.name, b.title LIMIT 20""",# noqa: E501
    """One-hop with target condition: MATCH (a:label_1)-[:edge_1]->(b:label_2) WHERE b.status = "active" RETURN a.id, a.name""",# noqa: E501
    """One-hop edge property: MATCH (a:label_1)-[r:edge_1]->(b:label_2) WHERE r.weight > 0.8 RETURN a.id, r.weight, b.id""",# noqa: E501
    """Undirected one-hop: MATCH (a:label_1)--(b:label_2) WHERE a.name = "value_1" RETURN b.id, b.name LIMIT 5""",# noqa: E501
    """Node query with OR: MATCH (n:label_1) WHERE n.type = "A" OR n.type = "B" RETURN n.id, n.type""",# noqa: E501
    """Node query with NOT: MATCH (n:label_1) WHERE NOT n.status = "banned" RETURN n.id, n.status LIMIT 10""",# noqa: E501
    """Count query: MATCH (n:label_1) WHERE n.prop_1 = "value_1" RETURN count(n.id)""",# noqa: E501
    """Ordered query: MATCH (n:label_1) WHERE n.score > 60 RETURN n.name, n.score ORDER BY n.score DESC LIMIT 5""",# noqa: E501
    """Distinct query: MATCH (n:label_1) RETURN DISTINCT n.category"""# noqa: E501
    # 2. Node query with limit
    """Node query with conditions and return limit: MATCH (n:label_1) WHERE n.prop_1 = "value_1" RETURN n.prop_1 LIMIT 1""",# noqa: E501
    # 3. Two-hop path
    """Two-hop path query with conditions and return limit: MATCH p = (n1:label_1)-[e1]-(x)-[e2]-(n2:label_1) WHERE n1.prop_1 = "value_1" AND n2.prop_1 <> "value_1" RETURN p LIMIT 1""",# noqa: E501
    # 4. Variable-length path
    """Variable-length path query with conditions and return limit: MATCH (n1:label_1)-[e*1..3]-(n2:label_2) WHERE n1.prop_1 = "value_1" RETURN n1, n2 LIMIT 5""",# noqa: E501
    """Variable-length path query with conditions and return limit: MATCH (n1:label_1)-[e*1..3]-(n2:label_2) WHERE n1.prop_1 = "value_1" RETURN n1.prop_1, n2.prop_1 LIMIT 5""",# noqa: E501
    """Variable-length path short range: MATCH (n1:label_1)-[*1..3]->(n2:label_2) WHERE n1.prop_1 = "value_1" RETURN n2.id, n2.name LIMIT 5""",# noqa: E501
    """Variable-length path medium range: MATCH (n1:label_1)-[*2..5]->(n2:label_2) WHERE n1.id = "start_node" RETURN n2.name LIMIT 5""",# noqa: E501
    """Fixed-length path: MATCH (n1:label_1)-[*3]->(n2:label_2) WHERE n1.prop_1 = "value_1" RETURN n2.id, n2.prop_2 LIMIT 5""",# noqa: E501
    """Variable-length with edge type: MATCH (n1:label_1)-[:edge_1*1..3]->(n2:label_2) WHERE n1.name = "Alice" RETURN n2.name LIMIT 5""",# noqa: E501
    """Variable-length multiple edge types: MATCH (n1:label_1)-[:edge_1|edge_2*1..4]->(n2:label_2) RETURN n1.id, n2.id LIMIT 10""",# noqa: E501
    """Variable-length start/end filter: MATCH (n1:label_1)-[*1..3]->(n2:label_2) WHERE n1.status = "active" AND n2.age > 30 RETURN n1.id, n2.id LIMIT 5""",# noqa: E501
    """Undirected variable-length: MATCH (n1:label_1)-[*1..2]-(n2:label_2) WHERE n1.id = "user_123" RETURN n2.id, n2.name LIMIT 5""",# noqa: E501
    """Count reachable nodes: MATCH (n1:label_1)-[*1..3]->(n2:label_2) WHERE n1.prop_1 = "root" RETURN count(n2.id) LIMIT 5""",# noqa: E501
    """Distinct reachable nodes: MATCH (n1:label_1)-[*1..3]->(n2:label_2) WHERE n1.prop_1 = "root" RETURN count(DISTINCT n2.id) LIMIT 5"""# noqa: E501
    # 5. One-hop path
    """One-hop path query with conditions and edge filtering: MATCH (n1:label_1)-[e:label_2]->(n2:label_3) WHERE n1.prop_1 = "value_1" RETURN n1, e, n2 LIMIT 10""",# noqa: E501
    # 6. Path with edge property filtering
    """Variable-length path with path return: MATCH p=(n1:label_1)-[e:label_2*1..3]->(n2:label_1) WHERE n1.prop_1 = "value_1" AND n2.prop_1 = "value_3" RETURN n2.prop_1 LIMIT 5""",# noqa: E501
    # 7. UNION ALL
    """Multi-hop path with multiple path merging: MATCH p = (n1:label_1)-[e1]-(x)-[e2]-(n2:label_1) WHERE n1.prop_1 = "value_1" AND n2.prop_1 <> "value_1" RETURN p LIMIT 5 UNION ALL MATCH p = (n1:label_1)-[e1]-(x)-[e2]-(y)-[e3]-(n2:label_1) WHERE n1.prop_1 = "value_1" AND n2.prop_1 <> "value_1" RETURN p LIMIT 5""",# noqa: E501
    # 8. Multiple edge types
    """Path query with multiple edge type filtering: MATCH (n1:label_1)-[e:label_2|label_3]->(n2:label_4) RETURN e, n2 LIMIT 10""",# noqa: E501
    # 9. Complex aggregation
    """Query with complex return conditions: MATCH (n2:label_1)<-[e1:label_2]-(n1:label_3) WHERE n2.prop_1 = "value_1" RETURN n1, COUNT(e1) as cnte1, n2 ORDER BY cnte1 DESC SKIP 1 LIMIT 5""",# noqa: E501
    # 10. Count
    """Node count query: MATCH (n1:label_1)-[]->(n2:label_2) WHERE n1.prop_1 = "value_1" RETURN COUNT(DISTINCT n2) as count_alias""",# noqa: E501
    # 11. Shortest path
    """Shortest path query: MATCH p = shortestPath((a:label_1)-[:label_2*1..10]->(b:label_3)) WHERE a.prop_1 = "value_1" RETURN p, length(p) AS depth ORDER BY depth ASC LIMIT 5""",# noqa: E501
    # 12. Optional Match
    """Multi-attribute correlation query: MATCH (a:label_1)-[:label_2]->(b:label_3) WHERE a.prop_1 = "value_1" OPTIONAL MATCH (b)<-[:label_4]-(c:label_5) OPTIONAL MATCH (b)<-[:label_6]-(d:label_7) RETURN DISTINCT c.prop_2, d.prop_3 LIMIT 10""",# noqa: E501
    # 13. Multi-step query (WITH)
    """Multi-step query: MATCH (nodes_1:label_1) WHERE nodes_1.prop_1 = "value_1" WITH nodes_1 MATCH (nodes_1)-[edges_1:label_2]->(nodes_2:label_3) RETURN nodes_2, nodes_1, edges_1 LIMIT 10""",# noqa: E501
    # 14. Inequality
    """Inequality condition filtering: MATCH (a:label_1 {prop_1: "value_1"})-[e:label_2]->(n:label_3) WHERE e.prop_2 = "value_2" AND e.prop_4 > "value_3" RETURN n, e.prop_5 LIMIT 10""",# noqa: E501
]


QUERY_TEMPLATE_INSTRUCTION = """
    You are a Cypher query generator expert for TuGraph.

    I have run some exploration queries on a Graph Database and got the following RAW RESULT DATA. 
    
    --- RAW DATA START ---
    {raw_data_str}
    --- RAW DATA END ---

    I also have a list of CYPHER TEMPLATES. 
    Your task is to generate {current_batch_size} new (Question, Query) pairs by filling these templates using the REAL DATA extracted from the RAW DATA above.

    --- TEMPLATES ---
    {selected_templates}

    --- CRITICAL RULES (Follow these or query will fail) ---
    1. **Correct Syntax**: NEVER put a `WHERE` clause inside the node parentheses. 
    - WRONG: `MATCH (n:Person WHERE n.age > 10)`
    - RIGHT: `MATCH (n:Person) WHERE n.age > 10`
    2. **Distinguish Node vs Edge**: 
    - Look closely at the RAW DATA. 
    - 'src' and 'dst' fields indicate an EDGE. 
    - 'identity' without 'src/dst' indicates a NODE.
    - Do NOT use a Node label as an Edge type (e.g. if 'GENRE' is a node, do not write `-[r:GENRE]-`).
    3. **Data Types**:
    - If a value is a string in RAW DATA, put quotes around it in Cypher (e.g. `name = "John"`).
    - If a value is a number, do not use quotes (e.g. `age = 40`).
    4. **JSON Output**: Output MUST be a strict JSON list of objects: [{{"question": "...", "query": "..."}}]
    """  # noqa: E501


EXPLORATION_PROMPT_TEMPLATE = """
# Command
Your task is to brainstorm and generate diverse natural language questions. Focus on the breadth and depth of questions, without considering how to write Cypher queries for now.

# 1. Graph Schema
```json
{schema_json}
```

2. Verified Query Examples (Context)
Here are some verified "question-query-result" examples that execute successfully. Use these as reference to understand the data patterns and query style in the graph. The result field shows partial data for reference.
```json
{examples_json}
```

3. Task Guidance
Please generate {num_to_generate} different, meaningful natural language questions around the following "query intent". These questions should fully utilize various nodes, relationships, and attributes defined in the Schema.
"Query Intent": {archetype}

4. Output Format
Return in JSON list format, where each element is a string (question).
For example:
[
"Question 1...",
"Question 2..."
]

"""  # noqa: E501

TRANSLATION_PROMPT_TEMPLATE = """
Command
Your task as a Cypher expert is to accurately translate the given natural language question into a Cypher query statement.

1. Graph Schema
This is the Schema of the graph the query is based on:
```JSON
{schema_json}
```

2. Question to be Translated
```json
{question}
```

3. !!! Important Rules !!!
Rule 1: Attribute Ownership: When specifying an attribute for a node (e.g., (n:Label)) in a WHERE clause, you must ensure the attribute clearly belongs to the Label node in the Schema definition.

Rule 2: Strict Prohibition of Confusion: Absolutely do not use attributes of relationships (EDGE) on nodes (VERTEX). For example, if compliance_status is an attribute of a relationship, then WHERE n.compliance_status = 'compliant' is a fatal error. The correct usage is to access it through the relationship variable, e.g., -[r:HAS_STATUS]-> and WHERE r.compliance_status = 'compliant'.

Rule 3: Faithfulness to Schema: Only use Schema

Rule 4: Use '%Y-%m-%d %H:%M:%S' format for time representation

{error_context}

3. Output Format
Return in JSON object format containing only the "query" key. Do not add any additional explanations.
For example:
{{
"query": "MATCH (m:Movie) WHERE m.title = 'some movie' RETURN m"
}}
"""  # noqa: E501
