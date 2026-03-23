NL2GQL_BATCH_DIFFICULTY_SYSTEM = r"""
You are a graph database expert. 
Analyze the query complexity. 
Output JSON list only with fields: id, difficulty (easy, medium, hard, extra hard)."""  # noqa: E501

NL2GQL_BATCH_DIFFICULTY_PROMPT_TEMPLATE = r"""
You are a graph database expert.
You need to grade the complexity level of the following set of Cypher query statements.

### List of Queries to Analyze (JSON format)
{queries_json}

### Grading Standards
1. **Easy**: Single node/edge matching, no aggregation/complex conditions (e.g., MATCH (n) RETURN n)
2. **Medium**: One-hop path, simple aggregation/filtering (e.g., MATCH (a)-[]->(b) RETURN COUNT(b))
3. **Hard**: Multi-hop (<=2), multiple conditions, non-nested aggregation (e.g., MATCH (a)-[]->(b)-[]->(c) RETURN a, c)
4. **Extra Hard**: Complex path (>=3), multi-step MATCH, nested aggregation.(e.g. MATCH (a)-[:KNOWS*1..5]->(b) WITH b MATCH (b)-[:WORKS]->(c) RETURN avg(c.age))

### Output Requirements
Please return a JSON list where each object contains `id` and `grade_info`.
Format example:
[
    {{
        "id": ,
        "grade_info": {{ "difficulty": "Easy", "reasoning": "Simple match" }}
    }},
    {{
        "id": ,
        "grade_info": {{ "difficulty": "Hard", "reasoning": "2-hop path" }}
    }}
]

**Note:**
- Must return a standard JSON list.
- Ensure the number of returned results matches the number of input queries.
- Do not output Markdown formatting.
"""  # noqa: E501
