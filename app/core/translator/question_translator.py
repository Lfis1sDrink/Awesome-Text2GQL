import json
from typing import List, Optional, Tuple

from tqdm import tqdm

from app.core.llm.llm_client import LlmClient

from app.core.translator.prompts import HIERARCHICAL_PROMPT_TEMPLATE, EXTERNAL_KNOWLEDGE_PROMPT_TEMPLATE

CONTENT_TEMPLATE = """
Original Query: {query_template}
Translated Question: {question_template}

Queries to translate:
{query_chunk_str}
"""

PROMPT = """
Imagine you are the frontend of a graph database, where users ask questions and you generate corresponding queries.
Now, I need you to reverse this process: translate the queries I provide into natural language questions that a graph database user might input.
The translation must:
1. Match the phrasing style of graph database users.
2. Accurately reflect the query semantics.
3. Preserve all keywords like DISTINCT, OPTIONAL, etc.
4. Convert to either questions or statements as needed.

For each query, I will provide a template with the same structure as the target query to help you understand its meaning. Here's an example:

Original Query: MATCH (m:keyword{name: 'news report'})<-[:has_keyword]-(a:movie) RETURN a,m
Translated Question: "What movies have the keyword 'news report'? Return the corresponding nodes."

Queries to translate:
MATCH (m:movie{title: 'The Dark Knight'})<-[:write]-(a:person) RETURN a,m
MATCH (m:user{login: 'Sherman'})<-[:is_friend]-(a:user) RETURN a,m

You should respond with:
"Who are the authors of the movie 'The Dark Knight'? Return the relevant nodes.
"Find the friend nodes of the logged-in user Sherman in the graph, returning the relevant node information."

Now, translate each of the following queries one by one. Do not indicate which query you're translating from.
Separate results with newline characters and ensure sentences include proper punctuation marks.
"""  # noqa: E501

CONTENT_TEMPLATE_MULTILINGUAL = """
Source Language: [{source_language}]
Target Language: [{target_language}]

Questions to translate:
{corpus_pair_chunk_str}
"""

PROMPT_MULTILINGUAL = """
Imagine you are an expert at multilingual translation and graph database.
I will give you a natural language question asked by graph database users and the corresponding graph query,
I need you to translate that natural language question from the source language into the target language we need.
The translation must:
1. Match the phrasing style of graph database users.
2. Accurately reflect the query semantics.
3. Preserve all keywords like DISTINCT, OPTIONAL, etc.
4. Convert to either questions or statements as needed.

For each question, I will provide the corresponding query to help you understand the its meaning. Here's an example:

Source Language: English
Target Language: Chinese

Questions to translate:
[Original Question]:"What movies have the keyword 'news report'? Return the corresponding nodes." [Corresponding Query]:MATCH (m:keyword{name: 'news report'})<-[:has_keyword]-(a:movie) RETURN a,m
[Original Question]:"Who are the authors of the movie 'The Dark Knight'? Return the relevant nodes." [Corresponding Query]:MATCH (m:movie{title: 'The Dark Knight'})<-[:write]-(a:person) RETURN a,m
[Original Question]:"Find the friend nodes of the logged-in user Sherman in the graph, returning the relevant node information." [Corresponding Query]:MATCH (m:user{login: 'Sherman'})<-[:is_friend]-(a:user) RETURN a,m

You should respond with:
"哪些电影包含关键词“news report”？返回相应的节点。"
"电影《The Dark Knight》的作者是谁？返回相关节点。"
"在图中查找登录用户Sherman的好友节点，返回相关节点信息。"

Now, translate each of the following questions one by one. Do not indicate which question you're translating from.
Separate results with newline characters and ensure sentences include proper punctuation marks.
"""  # noqa: E501


class QuestionTranslator:
    def __init__(self, llm_client: LlmClient, chunk_size):
        self.llm_client = llm_client
        self.chunk_size = chunk_size
        self.keywords_to_remove = [
            "Cypher: ",
            "   **Translation:**",
            "**",
            "    -",
            "`",
            ". ",
        ]

    def translate(
        self, query_template: str, question_template: str, query_list: List[str]
    ) -> List[Tuple[str, str]]:
        question_list = []
        chunk_size = self.chunk_size
        query_chunk_list = [
            query_list[i : i + chunk_size] for i in range(0, len(query_list), chunk_size)
        ]
        for query_chunk in query_chunk_list:
            query_chunk_str = ""
            for query in query_chunk:
                query_chunk_str += query + "\n"
            content = CONTENT_TEMPLATE.format(
                query_template=query_template,
                question_template=question_template,
                query_chunk_str=query_chunk_str,
            )

            messages = [
                {
                    "role": "system",
                    "content": PROMPT,
                },
                {"role": "user", "content": content},
            ]

            # 3. get response
            response = self.llm_client.call_with_messages(messages)

            # 4. postprocess and save
            if response != "":
                translated_question_list = self.post_process(response)

                # deal with unexpected questions length
                chunk_size = len(query_chunk)
                questions_size = len(translated_question_list)

                if questions_size > chunk_size:
                    translated_question_list = translated_question_list[0:chunk_size]
                elif questions_size < chunk_size:
                    filled_questions = ["Question translation failed."] * (
                        chunk_size - questions_size
                    )
                    translated_question_list = translated_question_list + filled_questions
                else:
                    pass
            else:
                translated_question_list = ["Question translation failed."] * (chunk_size)

            question_list += translated_question_list

        return question_list

    def translate_multilingual(
        self,
        source_language: str,
        target_language: str,
        question_list: List[str],
        query_list: List[str],
    ) -> List[Tuple[str, str]]:
        target_language_question_list = []
        corpus_pair_list = [(question_list[i], query_list[i]) for i in range(len(question_list))]
        chunk_size = self.chunk_size
        corpus_pair_chunk_list = [
            corpus_pair_list[i : i + chunk_size]
            for i in range(0, len(corpus_pair_list), chunk_size)
        ]
        for i in tqdm(
            range(len(corpus_pair_chunk_list)),
            desc=f"Translating {source_language} into {target_language}",
        ):
            corpus_pair_chunk = corpus_pair_chunk_list[i]
            corpus_pair_chunk_str = ""
            for corpus_pair in corpus_pair_chunk:
                corpus_pair_chunk_str += (
                    f"[Original Question]:{corpus_pair[0]} [Corresponding Query]:{corpus_pair[1]}\n"
                )
            content = CONTENT_TEMPLATE_MULTILINGUAL.format(
                source_language=source_language,
                target_language=target_language,
                corpus_pair_chunk_str=corpus_pair_chunk_str,
            )

            messages = [
                {
                    "role": "system",
                    "content": PROMPT_MULTILINGUAL,
                },
                {"role": "user", "content": content},
            ]

            # 3. get response
            response = self.llm_client.call_with_messages(messages)

            # 4. postprocess and save
            if response != "":
                translated_question_list = self.post_process(response)

                # deal with unexpected corpus pair list length
                chunk_size = len(corpus_pair_chunk)
                questions_size = len(translated_question_list)

                if questions_size > chunk_size:
                    translated_question_list = translated_question_list[0:chunk_size]
                elif questions_size < chunk_size:
                    filled_questions = ["Question translation failed."] * (
                        chunk_size - questions_size
                    )
                    translated_question_list = translated_question_list + filled_questions
                else:
                    pass
            else:
                translated_question_list = ["Question translation failed."] * (chunk_size)

            target_language_question_list += translated_question_list

        return target_language_question_list

    def post_process(self, response):
        lines = response.split("\n")
        translated_question_list = []
        for line in lines:
            # remove keywords
            for keyword in self.keywords_to_remove:
                if keyword == ". ":  # remove "1."
                    dot_index = line.find(". ")
                    if dot_index != -1:
                        line = line[dot_index + 2 :]
                        continue
                line = line.replace(keyword, "")
            # remove white space
            line = line.strip()
            if line:
                translated_question_list.append(line)
        return translated_question_list


class HierarchicalQuestionTranslator:
    '''
    This class can translate GQL into hierarchical natural language questions and add external knowledge to the questions if needed.

    usage:
    ```python
    question_translator = HierarchicalQuestionTranslator(llm_client, need_external_knowledge=True)
    sample_list = question_translator.translate_hierachical_questions(query_list)
    '''

    def __init__(self, llm_client: LlmClient, need_external_knowledge: bool = True):
        '''
        Initialize the HierarchicalQuestionTranslator.
        Args:
            llm_client: LlmClient
            need_external_knowledge: bool = True
        '''
        self.llm_client = llm_client
        self.hierarchical_prompt_template = HIERARCHICAL_PROMPT_TEMPLATE
        if need_external_knowledge:
            self.need_external_knowledge = True
            self.external_knowledge_prompt_template = EXTERNAL_KNOWLEDGE_PROMPT_TEMPLATE
        else:
            self.need_external_knowledge = False
            self.external_knowledge_prompt_template = None

    def translate_hierachical_questions(
        self, query_list: List[str], data_schema_list: Optional[List[str]] = None
    ) -> List[dict]:
        '''
        Translate GQL into hierarchical natural language questions, and add external knowledge to the questions if needed.
        
        Args:
            query_list: List[str]
            data_schema_list: Optional[List[str]] = None
        Returns:
            List[dict]
        Return example when need_external_knowledge=False:
            {
                "gql_query": "MATCH (n:Node) RETURN n",
                "level_1": "List all nodes in the graph.",
                "level_2": "List all nodes in the graph.",
                "level_3": "List all nodes in the graph.",
            }
        Return example when need_external_knowledge=True:
            {
                "gql_query": "MATCH (n:Node) RETURN n",
                "level_1": "List all nodes in the graph.",
                "level_2": "List all nodes in the graph.",
                "level_3": "List all nodes in the graph.",
                "external_knowledge": "The graph has 100 nodes."
            }
        '''

        if data_schema_list is None:
            data_schema_list = [""] * len(query_list)
        elif len(data_schema_list) != len(query_list):
            raise ValueError("data_schema_list length must match query_list length")

        # 1. generate prompt list
        prompt_list = [
            self.hierarchical_prompt_template.format(
                gql_query=query, data_schema=data_schema
            )
            for query, data_schema in zip(query_list, data_schema_list)
        ]
        # 2. call LLM with prompt list
        response_list = [
            self.llm_client.call_with_messages([{"role": "user", "content": prompt}])
            for prompt in prompt_list
        ]
        # 3. postprocess
        hierarchical_question_list = [
            self.post_process_hierarchical_questions_response(response, query)
            for response, query in zip(response_list, query_list)
        ]

        # 4. translate external knowledge
        if not self.need_external_knowledge:
            return hierarchical_question_list
        else:
            hierarchical_question_list_with_external_knowledge = self.add_external_knowledge(hierarchical_question_list)
            return hierarchical_question_list_with_external_knowledge

    def add_external_knowledge(self, hierarchical_question_list: List[dict]) -> List[dict]:
        '''
        Add external knowledge to the hierarchical natural language questions.
        Args:
            hierarchical_question_list: List[dict]
        Returns:
            List[dict]
        Return example:
            {
                "gql_query": "MATCH (n:Node) RETURN n",
                "level_1": "List all nodes in the graph.",
                "level_2": "List all nodes in the graph.",
                "level_3": "List all nodes in the graph.",
                "external_knowledge": "The graph has 100 nodes."
            }
        '''
        query_list = [item["gql_query"] for item in hierarchical_question_list]

        # 1. generate prompt list
        prompt_list = [
            self.external_knowledge_prompt_template.format(
                gql_query=query,
                level_2=hierarchical_question["level_2"],
                level_3=hierarchical_question["level_3"],
            )
            for query, hierarchical_question in zip(query_list, hierarchical_question_list)
        ]

        # 2. call LLM with prompt list
        response_list = [
            self.llm_client.call_with_messages([{"role": "user", "content": prompt}])
            for prompt in prompt_list
        ]

        # 3. postprocess
        postprocessed_list = [
            self.post_process_external_knowledge_response(
                response, query, hierarchical_question
            )
            for response, query, hierarchical_question in zip(
                response_list, query_list, hierarchical_question_list
            )
        ]

        return postprocessed_list

    def post_process_hierarchical_questions_response(self, response: str, query: str) -> dict:
        '''
        Postprocess the response of hierarchical natural language questions.
        Args:
            response: str
            query: str
        Returns:
            dict
        '''
        # Try to extract JSON from the response
        if "```json" in response:
            json_str = response.split("```json")[1].split("```")[0].strip()
        elif "```" in response:
            json_str = response.split("```")[1].split("```")[0].strip()
        else:
            json_str = response.strip()
        
        # Parse JSON
        result = json.loads(json_str)

        return {
            "gql_query": query,
            "level_1": result.get("level_1", ""),
            "level_2": result.get("level_2", ""),
            "level_3": result.get("level_3", ""),
        }
    
    def post_process_external_knowledge_response(self, response: str, query: str, hierarchical_question: dict) -> dict:
        '''
        Postprocess the response of external knowledge and add to question samples
        Args:
            response: str
            query: str
            hierarchical_question: dict
        Returns:
            dict
        '''
        # Try to extract JSON from the response
        if "```json" in response:
            json_str = response.split("```json")[1].split("```")[0].strip()
        elif "```" in response:
            json_str = response.split("```")[1].split("```")[0].strip()
        else:
            json_str = response.strip()
        
        # Parse JSON
        result = json.loads(json_str)

        return {
            "gql_query": query,
            "level_1": hierarchical_question["level_1"],
            "level_2": hierarchical_question["level_2"],
            "level_3": hierarchical_question["level_3"],
            "external_knowledge": result.get("external_knowledge", ""),
        }