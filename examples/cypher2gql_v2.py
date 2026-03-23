import json
import re

from app.impl.iso_gql.translator.iso_gql_query_translator import (
    IsoGqlQueryTranslator as GQLTranslator,
)
from app.impl.tugraph_cypher.ast_visitor.tugraph_cypher_ast_visitor import TugraphCypherAstVisitor
from app.impl.tugraph_cypher.translator.tugraph_cypher_query_translator import (
    TugraphCypherQueryTranslator as CypherTranslator,
)


def cypher2gql(query):
    query_visitor = TugraphCypherAstVisitor()
    gql_translator = GQLTranslator()
    cypher_translator = CypherTranslator()
    translated_query = "Unable to Translate to GQL"
    category = ""
    if cypher_translator.grammar_check(query):
        if not gql_translator.grammar_check(query):
            # test if musk reserved world can solve the grammar problem:
            musk_query = query
            for word in gql_translator.get_reserved_worlds():
                musk_query = re.sub(
                    rf"\[:{word}\]", f"[:`{word}`]", musk_query, flags=re.IGNORECASE
                )
            if gql_translator.grammar_check(musk_query):
                translated_query = musk_query
                category = "Comply with ISO-GQL by Musking Reserved Words"
            else:
                # translate the entire query
                success, query_pattern = query_visitor.get_query_pattern(query)
                if success:
                    query_gql = gql_translator.translate(query_pattern)
                    if gql_translator.grammar_check(query_gql):
                        translated_query = query_gql
                        category = "Graph-IL Translatable"
                    else:
                        category = "No Related ISO-GQL Standard"
                else:
                    category = "Graph-IL Not Support"
        else:
            translated_query = query
            category = "Comply with ISO-GQL"
    else:
        category = "Not Comply with OpenCypher"

    return translated_query, category


# get test query list
with open("./test_cypher_queries.json", encoding="utf-8") as f:
    query_list = json.load(f)

output_query_list = []

for query in query_list:
    new_row = {}
    new_row["cypher"] = query
    new_row["gql"], new_row["category"] = cypher2gql(query)
    output_query_list.append(new_row)

output_path = "test_gql_query.json"
with open(output_path, "w", encoding="utf-8") as file:
    json.dump(output_query_list, file, ensure_ascii=False, indent=4)
