import re

from antlr4 import CommonTokenStream, InputStream

from app.impl.iso_gql.translator.iso_gql_query_translator import (
    IsoGqlQueryTranslator as GQLTranslator,
)
from app.impl.tugraph_cypher.ast_visitor.tugraph_cypher_ast_visitor import TugraphCypherAstVisitor
from app.impl.tugraph_cypher.grammar.LcypherLexer import LcypherLexer
from app.impl.tugraph_cypher.grammar.LcypherParser import LcypherParser
from app.impl.tugraph_cypher.translator.tugraph_cypher_query_translator import (
    TugraphCypherQueryTranslator as CypherTranslator,
)

# print a cypher AST
query_cypher = "MATCH (pj:ProcessingJob)-[:Records]->(le:LineageEvent {status: 'Failed'}), (pj)-[t:Transforms]->(da:DataAsset {schema_version: '3.0'}) WITH pj, MAX(le.event_timestamp) AS latest_failure_time RETURN pj.name AS job_name, pj.owning_department AS owning_department, latest_failure_time"

input_stream_cypher = InputStream(query_cypher)
lexer_cypher = LcypherLexer(input_stream_cypher)
token_stream_cypher = CommonTokenStream(lexer_cypher)
parser_cypher = LcypherParser(token_stream_cypher)
tree_cypher = parser_cypher.oC_Cypher()

print(f"[Cypher AST]:{tree_cypher.toStringTree(recog=parser_cypher)}")

query_visitor = TugraphCypherAstVisitor()
gql_translator = GQLTranslator()
cypher_translator = CypherTranslator()

musk_query = query_cypher
for word in gql_translator.get_reserved_worlds():
    musk_query = re.sub(rf"\[:{word}\]", f"[:`{word}`]", musk_query, flags=re.IGNORECASE)
musk_query = musk_query.replace("CREATE", "INSERT")
print(musk_query)
if gql_translator.grammar_check(musk_query):
    print("CORRECT")


success, query_pattern = query_visitor.get_query_pattern(query_cypher)
if success:
    query_gql = gql_translator.translate(query_pattern)
    print(query_gql)
    if gql_translator.grammar_check(query_gql):
        print("CORRECT")
