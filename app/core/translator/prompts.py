HIERARCHICAL_PROMPT_TEMPLATE = """You are an expert in Graph Query Language (GQL) and natural language understanding. Your task is to generate 3 levels of natural language descriptions for the given GQL query, following the framework from "Accessible Visualization via Natural Language Descriptions: A Four-Level Model of Semantic Content".

## 3-Level Framework Definition:

**Level 1: Structural/Syntactic Elements**
- Purpose: Basic query components and syntax
- Characteristics: Directly mentions node labels, relationship types, properties, and graph patterns
- Example: "List 'Entity' nodes that were incorporated on the property value '23-MAR-2006'."

**Level 2: Semantic/Logical Operations**
- Purpose: Query logic and computational meaning
- Characteristics: Describes what the query does without explicit graph terminology, focuses on the logical operation
- Example: "List entities incorporated on March 23, 2006."

**Level 3: Analytical/Strategic Patterns**
- Purpose: Data analysis strategy and methodological insights
- Characteristics: Describes the analytical approach, patterns being investigated, or research methodology
- Example: "Analyze company incorporation trends throughout the year 2026."

---

## Given Information:

**Data Schema (optional):**
```
{data_schema}
```

**GQL Query:**
```gql
{gql_query}
```

---

## Task:

Based on the GQL query above, generate 3 levels of natural language descriptions.

**Important Guidelines:**
1. Each level should be a complete, standalone natural language question or statement.
2. Level 1 should include explicit graph terminology (nodes, relationships, properties).
3. Level 2 should be semantically clear without graph-specific terms.
4. Level 3 should focus on analytical methodology or data exploration strategy.
5. Ensure smooth progression from concrete (L1) to abstract (L3).
6. Each level should be distinct from the others in abstraction and focus.

**Output Format:**
Please provide your response in the following JSON format:

```json
{{
  "level_1": "Your Level 1 description here",
  "level_2": "Your Level 2 description here",
  "level_3": "Your Level 3 description here",
  "explanation": "Brief explanation of the progression and key differences between levels"
}}
```"""

EXTERNAL_KNOWLEDGE_PROMPT_TEMPLATE = """
You are an expert in Graph Query Language (GQL) and natural language (NL) understanding. Your task is to generate the external knowledge for high level NL based on the gql query and basic NL query. The external knowledge should be specific and clear, and should be able to help the model understand the high level NL query better.

## Given Information:
**GQL Query:**
```gql
{gql_query}
```
**Basic NL:**
```
{level_2}
```
**High Level NL:**
```
{level_3}
```

## Task:
Generate external knowledge that bridges the gap between the High Level NL and Basic NL queries.

**Guidelines:**
1. **Identify missing specifics:** Extract exact values, thresholds, entity names, or technical terms from the GQL/Basic NL that are absent or vague in the High Level NL.
2. **Be precise:** State concrete numbers, definitions, or clarifications (e.g., "a high rating means rating >= 4.0", "recent refers to the last 30 days").
3. **Keep it minimal:** Limit to 1-2 sentences (max 50 words). Only include what is necessary to disambiguate the High Level NL.
4. **Return empty if unnecessary:** If the High Level NL already contains all specifics from the Basic NL, return an empty string "".

## Output Format:
Please provide your response in the following JSON format:
```json
{{
    "external_knowledge": "Your external knowledge here"
}}
```
"""