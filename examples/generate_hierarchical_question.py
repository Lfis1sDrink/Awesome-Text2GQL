import json
import os

from app.core.llm.llm_client import LlmClient
from app.core.translator.question_translator import HierarchicalQuestionTranslator


def main():
    llm_client = LlmClient(model="qwen-plus")

    question_translator = HierarchicalQuestionTranslator(
        llm_client=llm_client, need_external_knowledge=True
    )

    query_list = [
        "MATCH (n:Person)-[:ACTED_IN]->(m:Movie) WHERE n.name = 'Tom Hanks' RETURN m.title",
        "MATCH (m:Movie)<-[:DIRECTED]-(d:Person) WHERE m.title = 'The Matrix' RETURN d.name",
    ]

    # NOTE: Data schema list is optional.
    # - If Data Schema is not provided, the question translator will only use the GQL query to generate the hierarchical questions.
    # - The structure of data schema is not strict, it is only used to provide additional information to the question translator.
    data_schema_list = [
        "Node labels: Person(name), Movie(title); Relationship: (:Person)-[:ACTED_IN|DIRECTED]->(:Movie)",
        "Node labels: Person(name), Movie(title); Relationship: (:Person)-[:ACTED_IN|DIRECTED]->(:Movie)"
    ]

    hierarchical_questions = question_translator.translate_hierachical_questions(
        query_list=query_list, data_schema_list=data_schema_list
    )
    print(json.dumps(hierarchical_questions, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
