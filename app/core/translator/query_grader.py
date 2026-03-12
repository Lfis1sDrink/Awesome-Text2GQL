"""Query Grader module for grading query complexity.

This module provides functionality to grade the difficulty level of graph queries
using LLM-based analysis. It supports batch processing and handles various query formats.
"""

import asyncio
import json
import logging
import random
import re
from typing import Any, Dict, List

from app.core.llm.llm_client import LlmClient
from app.core.prompt.grade import (
    NL2GQL_BATCH_DIFFICULTY_PROMPT_TEMPLATE,
    NL2GQL_BATCH_DIFFICULTY_SYSTEM,
)

logger = logging.getLogger(__name__)


class QueryGrader:
    """Grades the difficulty level of graph queries using LLM analysis.

    The QueryGrader processes queries in batches and assigns difficulty ratings
    (easy, medium, hard, extra hard) based on query complexity.

    Attributes:
        llm_client: The LLM client used for grading queries.
    """

    # Class-level constant for valid difficulty ratings
    VALID_DIFFICULTIES = {"easy", "medium", "hard", "extra hard"}

    def __init__(self, llm_client: LlmClient):
        self.llm_client = llm_client

    def _extract_json_list(self, text: str) -> List[Dict]:
        """Extract JSON list from LLM response.

        Args:
            text: The LLM response text to parse.

        Returns:
            A list of dictionaries parsed from the JSON response, or an empty list
            if parsing fails.
        """
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
            match = re.search(r"\[.*?\]", text, re.DOTALL)
            if match:
                data = json.loads(match.group())
                if isinstance(data, list):
                    return data
        except (json.JSONDecodeError, TypeError, re.error):
            pass

        return []

    def _is_valid_difficulty(self, item: Dict[str, Any]) -> bool:
        """Check if an item has a valid difficulty rating.

        Args:
            item: The query item to check.

        Returns:
            True if the item has a valid difficulty rating, False otherwise.
        """
        diff = item.get("difficulty")
        return isinstance(diff, str) and diff.lower() in self.VALID_DIFFICULTIES

    async def _process_batch(
        self,
        batch_items: List[Dict[str, Any]],
        batch_idx: int,
        semaphore: asyncio.Semaphore,
        max_retries: int,
    ) -> None:
        """Process a batch of items: call LLM to get difficulty and update in place.

        Args:
            batch_items: List of query items to grade.
            batch_idx: Index of the current batch for logging purposes.
            semaphore: Async semaphore for controlling concurrent batch processing.
            max_retries: Maximum number of retry attempts for failed LLM calls.
        """
        if not batch_items:
            return

        # 1. Construct payload and record mapping
        payload = []
        item_map: Dict[int, Dict[str, Any]] = {}

        for idx, item in enumerate(batch_items):
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
        async with semaphore:
            for attempt in range(max_retries):
                try:
                    response = await asyncio.to_thread(self.llm_client.call_with_messages, messages)

                    if not response:
                        # Fixed: Use more specific exceptions instead of generic Exception
                        raise ValueError("Empty response from LLM")
                    if "429" in str(response) or "Rate limit" in str(response):
                        raise ConnectionError("Rate limit hit")

                    graded_results = self._extract_json_list(response)
                    if not graded_results:
                        raise ValueError("Failed to parse JSON list from LLM response")

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

                        if found_diff in self.VALID_DIFFICULTIES:
                            original_item["difficulty"] = found_diff
                            updated_count += 1
                        else:
                            # If LLM returns invalid or no result, set to unknown
                            if "difficulty" not in original_item:
                                original_item["difficulty"] = "unknown"

                    logger.info(
                        f"[Batch {batch_idx}] Successfully graded "
                        f"{updated_count}/{len(batch_items)} items."
                    )
                    return  # Successfully processed, exit retry loop

                except Exception as e:
                    # Capturing general exceptions here is fine for retries, 
                    # but we log the specific error
                    is_rate_limit = "429" in str(e) or "Rate limit" in str(e)
                    sleep_time = (2**attempt) + random.uniform(1, 3) + (10 if is_rate_limit else 0)

                    if attempt < max_retries - 1:
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

    async def grade_query(
        self,
        query: List[Dict[str, Any]],
        batch_size: int = 10,
        concurrent_batches: int = 5,
        max_retries: int = 5,
    ) -> List[Dict[str, Any]]:
        """Grade the difficulty level of queries in a query list.

        Args:
            query: List of query items containing query/question pairs.
                    Each item should have 'question' and 'query' fields
                    (or 'initial_question' and 'initial_gql').
            batch_size: Number of items to process in a single LLM call.
            concurrent_batches: Maximum number of batches to process concurrently.
            max_retries: Maximum number of retries per batch.

        Returns:
            The same query list with difficulty ratings added to each item.
            Items with valid existing difficulty ratings are skipped.
        """
        if not query:
            logger.warning("Empty query provided for grading.")
            return query

        # Filter items that need grading (skip those already with valid difficulty)
        items_to_grade = [item for item in query if not self._is_valid_difficulty(item)]

        if not items_to_grade:
            logger.info("All items already have a valid difficulty. Nothing to do.")
            return query

        logger.info(f"Total items: {len(query)} | To be graded: {len(items_to_grade)}")

        # Split into batches and execute
        chunks = [
            items_to_grade[i : i + batch_size] for i in range(0, len(items_to_grade), batch_size)
        ]

        # Define semaphore dynamically inside the event loop that runs this async method
        semaphore = asyncio.Semaphore(concurrent_batches)

        tasks = [
            self._process_batch(chunk, i, semaphore, max_retries) for i, chunk in enumerate(chunks)
        ]

        await asyncio.gather(*tasks)

        return query

    def grade_query_sync(
        self,
        query: List[Dict[str, Any]],
        batch_size: int = 10,
        concurrent_batches: int = 5,
        max_retries: int = 5,
    ) -> List[Dict[str, Any]]:
        """Synchronous wrapper for grade_query.

        Args:
            query: List of query items containing query/question pairs.
            batch_size: Number of items to process in a single LLM call.
            concurrent_batches: Maximum number of batches to process concurrently.
            max_retries: Maximum number of retries per batch.

        Returns:
            The same query list with difficulty ratings added to each item.
        """
        import threading

        result = [None]
        exception = [None]

        def run_in_thread():
            try:
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    # Pass the dynamic arguments straight through
                    result[0] = new_loop.run_until_complete(
                        self.grade_query(query, batch_size, concurrent_batches, max_retries)
                    )
                finally:
                    new_loop.close()
            except Exception as e:
                exception[0] = e

        thread = threading.Thread(target=run_in_thread)
        thread.start()
        thread.join()

        if exception[0]:
            raise exception[0]

        return result[0]
