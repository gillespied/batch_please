import asyncio
import logging
import pickle
from typing import Any, Callable, List, Optional


class BatchProcessor:
    """
    # Example usage
    def example_process_single_item(string: str) -> str:
        sleep_time = random.uniform(2, 2.5)
        time.sleep(sleep_time)
        return f"Processed: {string} (took {sleep_time:.2f} seconds)"

    def main():
        input_items = [f"string{i + 1}" for i in range(10)]

        processor = BatchProcessor(
            process_func=example_process_single_item,
            batch_size=10,
            logfile="example.log"
        )

        processed_items, results = processor.process_items_in_batches(input_items)
        print(f"All processing complete. Total processed: {len(processed_items)}")

    main()
    """

    def __init__(
        self,
        process_func: Callable[[str], Any],
        batch_size: int = 100,
        pickle_file: str = "progress.pickle",
        logfile: Optional[str] = None,
    ):
        self.process_func = process_func
        self.batch_size = batch_size
        self.pickle_file = pickle_file
        self.processed_items = []
        self.results = []

        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.handlers = []  # Clear any existing handlers
        formatter = logging.Formatter("%(asctime)s - %(message)s")

        # File handler (if logfile is provided)
        if logfile:
            file_handler = logging.FileHandler(logfile)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def process_item(self, job_number: int, string: str):
        result = self.process_func(string)
        self.logger.info(f"Processed job {job_number}: {string}")
        return result

    def process_batch(self, batch: List[str], batch_number: int, total_jobs: int):
        batch_results = [self.process_item(i, s) for i, s in enumerate(batch)]

        self.processed_items.extend(batch)
        self.results.extend(batch_results)

        self.save_progress()

        completion_message = f"Batch {batch_number} completed. Total processed: {len(self.processed_items)}/{total_jobs}"
        print(completion_message)
        self.logger.info(completion_message)

    def save_progress(self):
        with open(self.pickle_file, "wb") as f:
            pickle.dump(
                {"processed_items": self.processed_items, "results": self.results},
                f,
            )

    def process_items_in_batches(self, input_items: List[str]):
        total_jobs = len(input_items)

        for i in range(0, total_jobs, self.batch_size):
            batch = input_items[i : i + self.batch_size]
            self.process_batch(batch, i // self.batch_size + 1, total_jobs)

        return self.processed_items, self.results


class AsyncBatchProcessor(BatchProcessor):
    """
    # Example usage
    async def example_process_single_item(string: str) -> str:
        sleep_time = random.uniform(2, 2.5)
        time.sleep(sleep_time)
        return f"Processed: {string} (took {sleep_time:.2f} seconds)"

    def main():
        input_items = [f"string{i + 1}" for i in range(10)]

        processor = AsyncBatchProcessor(
            process_func=example_process_single_item,
            batch_size=100,
            logfile="example.log",
            max_concurrent=5  # Limit to 5 concurrent operations
        )

        processed_items, results = processor.process_items_in_batches(input_items)
        print(f"All processing complete. Total processed: {len(processed_items)}")

    main()
    """

    def __init__(
        self,
        process_func: Callable[[str], Any],
        batch_size: int = 100,
        pickle_file: str = "progress.pickle",
        logfile: Optional[str] = None,
        max_concurrent: Optional[int] = None,
    ):
        super().__init__(process_func, batch_size, pickle_file, logfile)
        self.semaphore = (
            asyncio.Semaphore(max_concurrent) if max_concurrent is not None else None
        )

    async def process_item(self, job_number: int, string: str):
        async def _process():
            result = await self.process_func(string)
            self.logger.info(f"Processed job {job_number}: {string}")
            return result

        if self.semaphore:
            async with self.semaphore:
                return await _process()
        else:
            return await _process()

    async def process_batch(self, batch: List[str], batch_number: int, total_jobs: int):
        tasks = [
            self.process_item(i + (batch_number - 1) * self.batch_size, s)
            for i, s in enumerate(batch)
        ]
        batch_results = await asyncio.gather(*tasks)

        self.processed_items.extend(batch)
        self.results.extend(batch_results)

        self.save_progress()
        self.logger.info(
            f"Batch {batch_number} completed. Total processed: {len(self.processed_items)}/{total_jobs}"
        )

    async def process_items_in_batches(self, input_items: List[str]):
        total_jobs = len(input_items)

        for i in range(0, total_jobs, self.batch_size):
            batch = input_items[i : i + self.batch_size]
            await self.process_batch(batch, i // self.batch_size + 1, total_jobs)

        return self.processed_items, self.results
