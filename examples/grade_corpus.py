import asyncio
import json
import logging
from pathlib import Path
import random
import re
import sys
from typing import Dict, List

from app.core.llm.llm_client import LlmClient
from app.core.prompt.grade import (
    NL2GQL_BATCH_DIFFICULTY_PROMPT_TEMPLATE,
    NL2GQL_BATCH_DIFFICULTY_SYSTEM,
)

INPUT_FILE_PATH = (
    "examples/generated_corpus/Manufacturing_Production_Process_aa00_template_corpus.json"
)
OUTPUT_FILE_PATH = (
    "examples/generated_corpus/Manufacturing_Production_Process_aa00_template_graded_output.json"
)

MODEL_NAME = "qwen3-coder-plus-2025-07-22"
BATCH_SIZE = 10
CONCURRENT_BATCHES = 5
MAX_RETRIES = 5

VALID_DIFFICULTIES = {"easy", "medium", "hard", "extra hard"}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("CorpusGrader")


class CorpusGrader:
    def __init__(self):
        try:
            self.llm_client = LlmClient(model=MODEL_NAME)
        except Exception as e:
            logger.error(f"LlmClient initialization failed: {e}")
            sys.exit(1)

        self.semaphore = asyncio.Semaphore(CONCURRENT_BATCHES)

    def _extract_json_list(self, text: str) -> List[Dict]:
        """Extract JSON list from LLM response"""
        if not text:
            return []

        try:
            data = json.loads(text)
            if isinstance(data, list):
                return data
        except (json.JSONDecodeError, TypeError):
            pass

        try:
            clean = text.replace("```json", "").replace("```", "").strip()
            data = json.loads(clean)
            if isinstance(data, list):
                return data
        except (json.JSONDecodeError, TypeError):
            pass

        try:
            match = re.search(r"\[.*\]", text, re.DOTALL)
            if match:
                data = json.loads(match.group())
                if isinstance(data, list):
                    return data
        except (json.JSONDecodeError, TypeError, re.error):
            pass

        return []

    def _is_valid_difficulty(self, item: Dict) -> bool:
        diff = item.get("difficulty")
        return isinstance(diff, str) and diff.lower() in VALID_DIFFICULTIES

    async def process_batch(self, batch_items: List[Dict], batch_idx: int):
        """
        Process batch: concurrently call LLM to get difficulty,
        and update the dictionary objects in batch_items in place
        """
        if not batch_items:
            return

        # 1. Construct payload and record mapping
        payload = []
        # Use index to establish mapping for easy backfilling
        item_map = {}

        for idx, item in enumerate(batch_items):
            # Compatible with different field formats
            q_text = item.get("initial_question") or item.get("question") or ""
            g_text = item.get("initial_gql") or item.get("query") or ""

            temp_id = idx
            item_map[temp_id] = item
            payload.append({"id": temp_id, "question": q_text, "gql": g_text})

        queries_json = json.dumps(payload, indent=2, ensure_ascii=False)
        prompt_content = NL2GQL_BATCH_DIFFICULTY_PROMPT_TEMPLATE.format(queries_json=queries_json)
        system_content = NL2GQL_BATCH_DIFFICULTY_SYSTEM

        messages = [
            {"role": "system", "content": system_content},
            {"role": "user", "content": prompt_content},
        ]

        # 2. Asynchronous call with retry mechanism
        async with self.semaphore:
            for attempt in range(MAX_RETRIES):
                try:
                    # If LlmClient does not support async, wrap with to_thread
                    response = await asyncio.to_thread(self.llm_client.call_with_messages, messages)

                    if not response:
                        raise Exception("Empty response from LLM")
                    if "429" in str(response) or "Rate limit" in str(response):
                        raise Exception("Rate limit hit")

                    graded_results = self._extract_json_list(response)
                    if not graded_results:
                        raise Exception("Failed to parse JSON list from LLM response")

                    # 3. Backfill results
                    result_lookup = {g.get("id"): g for g in graded_results}
                    updated_count = 0

                    for temp_id, original_item in item_map.items():
                        grade_res = result_lookup.get(temp_id)
                        found_diff = None

                        if grade_res:
                            # Flexibly match various possible keys
                            for key in ["difficulty", "Difficulty", "grade"]:
                                if key in grade_res:
                                    found_diff = str(grade_res[key]).lower()
                                    break
                                # Compatible with nested format
                                if "grade_info" in grade_res and isinstance(
                                    grade_res["grade_info"], dict
                                ):
                                    if key in grade_res["grade_info"]:
                                        found_diff = str(grade_res["grade_info"][key]).lower()
                                        break

                        if found_diff in VALID_DIFFICULTIES:
                            original_item["difficulty"] = found_diff
                            updated_count += 1
                        else:
                            # If LLM returns invalid or no result, set to unknown
                            if "difficulty" not in original_item:
                                original_item["difficulty"] = "unknown"

                    updated_count = {updated_count}/{len(batch_items)}
                    logger.info(
                        f"[Batch {batch_idx}] Successfully graded {updated_count} items."
                    )
                    return  # Successfully processed, exit retry loop

                except Exception as e:
                    is_rate_limit = "429" in str(e) or "Rate limit" in str(e)
                    sleep_time = (2**attempt) + random.uniform(1, 3) + (10 if is_rate_limit else 0)

                    if attempt < MAX_RETRIES - 1:
                        logger.warning(
                            f"[Batch {batch_idx}] Attempt {attempt + 1} failed: {e}. "
                            f"Retrying in {sleep_time:.1f}s"
                        )
                        await asyncio.sleep(sleep_time)
                    else:
                        logger.error(f"[Batch {batch_idx}] FINAL FAILURE: {e}")
                        # Ensure field exists on final failure
                        for item in batch_items:
                            if "difficulty" not in item:
                                item["difficulty"] = "error"

    async def run(self):
        input_path = Path(INPUT_FILE_PATH)
        if not input_path.exists():
            logger.error(f"Input file not found: {INPUT_FILE_PATH}")
            return

        # 1. Load data
        try:
            with open(input_path, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                logger.error("Input JSON must be a list of objects.")
                return
        except Exception as e:
            logger.error(f"Failed to read JSON: {e}")
            return

        # 2. Filter items that need grading (skip those already with valid difficulty)
        items_to_grade = [item for item in data if not self._is_valid_difficulty(item)]

        if not items_to_grade:
            logger.info("All items already have a valid difficulty. Nothing to do.")
            return

        logger.info(f"Total items: {len(data)} | To be graded: {len(items_to_grade)}")

        # 3. Split into batches and execute
        chunks = [
            items_to_grade[i : i + BATCH_SIZE] for i in range(0, len(items_to_grade), BATCH_SIZE)
        ]
        tasks = [self.process_batch(chunk, i) for i, chunk in enumerate(chunks)]

        await asyncio.gather(*tasks)

        # 4. Save results
        output_path = Path(OUTPUT_FILE_PATH)
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            logger.info(f"✓ Process finished. Results saved to: {OUTPUT_FILE_PATH}")
        except Exception as e:
            logger.error(f"Error saving output file: {e}")


if __name__ == "__main__":
    grader = CorpusGrader()
    try:
        asyncio.run(grader.run())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
