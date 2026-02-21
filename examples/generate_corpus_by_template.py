import json
import logging
from pathlib import Path

from app.core.generator.corpus_generator import CorpusGenerator
from app.core.llm.llm_client import LlmClient
from app.core.validator.validator import CorpusValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("TemplateCorpusGeneratorScript")


def save_corpus_without_results(data: list, file_path: Path) -> None:
    """
    Save corpus data to JSON file, keeping only 'question' and 'query' fields.
    """
    filtered_data = [
        {"question": item.get("question", ""), "query": item.get("query", "")} for item in data
    ]

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(filtered_data, f, indent=4, ensure_ascii=False)
    logger.info(f"Successfully saved {len(filtered_data)} pairs to {file_path}")


def main():
    try:
        """
        Initializes the CorpusGenerator to generate corpus based on predefined TEMPLATES.
        This script:
        1. Connects to the DB to fetch real node/relationship data (Exploration).
        2. Fills templates from corpus.QUERY_TEMPLATE with this real data.
        3. Uses LLM to translate the filled queries into natural language questions.
        4. Validates the generated pairs and saves them.
        """
        # --- 1. Configuration and Initialization ---
        logger.info("Starting TEMPLATE-BASED corpus generation process...")

        # Graph database name
        graph = "Manufacturing_Production_Process_aa00"

        # Output file path
        output_file = Path(f"examples/generated_corpus/{graph}_template_corpus.json")

        # Ensure output directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # TuGraph client parameters
        tu_client_params = {
            "start_host_port": "localhost:7070",
            "username": "admin",
            "password": "73@TuGraph",
            "graph": graph,
        }

        # Initial exploration queries to fetch metadata (Nodes, Properties, Edges)
        # We need these results to know what valid "label_1", "prop_1", "value_1" are.
        explore_query = [
            {"question": "Explore 1", "query": "MATCH p = ()-[]-() RETURN p LIMIT 20"},
            {"question": "Explore 2", "query": "MATCH p = ()-[]-()-[]-() RETURN p LIMIT 20"},
        ]

        # Initialize Clients
        llm_client = LlmClient(model="qwen3-max-2025-09-23")
        generator = CorpusGenerator(llm_client=llm_client)
        validator = CorpusValidator(tu_client_params=tu_client_params)

        target_template_corpus_size = 50  # Target number of template-based pairs

        # --- 2. Data Exploration ---
        logger.info("Step 1: Executing exploration queries to fetch graph metadata...")

        # execution_results usually contains 
        # [{'question':..., 'query':..., 'context': {...JSON result...}}]
        execution_results = validator.execute_with_results(explore_query)

        # Extract only the result content (context) to pass to the generator's analyzer
        # Filtering out any failed queries (where context might be None)
        raw_exploration_data = [
            item.get("result") for item in execution_results if item.get("result")
        ]

        if not raw_exploration_data:
            logger.error(
                "Exploration queries returned no data. " \
                "Cannot fill templates. Check DB connection or data."
            )
            return

        logger.info(
            f"Collected metadata from {len(raw_exploration_data)} exploration query results."
        )

        # --- 3. Template Filling & Question Generation ---
        logger.info(f"Step 2: Generating {target_template_corpus_size} pairs based on templates...")

        # This calls the method we added to CorpusGenerator class
        template_corpus = generator.generate_template_based_corpus(
            exploration_results=raw_exploration_data, target_size=target_template_corpus_size
        )

        if not template_corpus:
            logger.warning("No template pairs were generated. Exiting.")
            return

        logger.info(f"Generated {len(template_corpus)} raw template pairs. Validating...")

        # --- 4. Validation (Optional but Recommended) ---
        # Although templates are syntactically correct, we validate to ensure
        # the specific values/combinations actually yield results or run without runtime errors.
        validated_corpus = validator.execute_with_results(template_corpus)

        logger.info(f"Validation complete. {len(validated_corpus)} pairs are valid.")

        # --- 5. Save Results ---
        save_corpus_without_results(validated_corpus, output_file)

        logger.info("Template-based corpus generation complete!")

    except Exception as e:
        logger.error(f"Program execution failed: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
