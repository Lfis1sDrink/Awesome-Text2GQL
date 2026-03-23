import json
import logging
import os
from pathlib import Path

from app.core.llm.llm_client import LlmClient
from app.core.translator.query_grader import QueryGrader

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def main():
    try:
        """Example script to grade generated queries using QueryGrader."""

        # Configuration
        input_file_path = "examples/generated_corpus/example_corpus.json"
        model_name = "qwen3-coder-plus-2025-07-22"
        llm_client = LlmClient(model=model_name)
        batch_size = 10
        concurrent = 5

        logger.info(f"Starting query grading with model: {model_name}")
        logger.info(f"Input: {input_file_path}")

        # Load query
        input_path = Path(input_file_path)
        if not input_path.exists():
            logger.error(f"Input file not found: {input_file_path}")
            return

        with open(input_path, encoding="utf-8") as f:
            query_data = json.load(f)
        logger.info(f"Loaded {len(query_data)} items")

        # Initialize QueryGrader
        grader = QueryGrader(llm_client=llm_client)

        # Grade query
        graded_query = grader.grade_query_sync(
            query_data, batch_size=batch_size, concurrent_batches=concurrent
        )

        temp_output_path = input_path.with_suffix(".tmp")
        try:
            with open(temp_output_path, "w", encoding="utf-8") as f:
                json.dump(graded_query, f, indent=2, ensure_ascii=False)

            # Only replace the original file after successful write (atomic operation)
            temp_output_path.replace(input_path)
            logger.info(f"Successfully updated {input_file_path}")
        except Exception as e:
            logger.error(f"Failed to save data safely: {e}")
            if temp_output_path.exists():
                os.remove(temp_output_path)
            raise

        # Define difficulty levels in logical order
        order = ["easy", "medium", "hard", "extra hard", "unknown", "error"]
        difficulty_counts = dict.fromkeys(order, 0)

        for item in graded_query:
            diff = item.get("difficulty", "unknown")
            difficulty_counts[diff] = difficulty_counts.get(diff, 0) + 1

        logger.info("Grading Summary:")
        for diff in order:
            count = difficulty_counts[diff]
            if count > 0:
                logger.info(f"  {diff.ljust(12)}: {count}")

    except KeyboardInterrupt:
        logger.warning("\nStopped by user. The current progress might not be saved.")
    except Exception as e:
        logger.error(f"Error during grading: {e}")
        raise


if __name__ == "__main__":
    main()
