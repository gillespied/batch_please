import asyncio
import logging
import os
import pickle
from typing import Callable, Generic, Iterable, List, Optional, Tuple, TypeVar

from tqdm import tqdm

# define some type generics
T = TypeVar("T")  # Type variable for input items
R = TypeVar("R")  # Type variable for result items


class BatchProcessor(Generic[T, R]):
    """
    A class for processing items in batches.

    This class takes an iterable of items, processes them in batches using a provided
    function, and optionally saves progress to allow for checkpoint recovery.

    Attributes:
        process_func (Callable[[T], R]): The function used to process each item.
        batch_size (int): The number of items to process in each batch.
        pickle_file (Optional[str]): The file to use for saving/loading progress.
        processed_items (List[T]): A list of items that have been processed.
        results (List[R]): A list of results from processing the items.
        recover_from_checkpoint (bool): Whether to attempt to recover from a checkpoint.
        use_tqdm (bool): Whether to use tqdm progress bars.
    """

    def __init__(
        self,
        process_func: Callable[[T], R],
        batch_size: int = 100,
        pickle_file: Optional[str] = None,
        logfile: Optional[str] = None,
        recover_from_checkpoint: bool = False,
        use_tqdm: bool = False,
    ):
        """
        Initialize the BatchProcessor.

        Args:
            process_func (Callable[[T], R]): The function to process each item.
            batch_size (int, optional): The number of items to process in each batch. Defaults to 100.
            pickle_file (Optional[str], optional): The file to use for saving/loading progress. Defaults to None.
            logfile (Optional[str], optional): The file to use for logging. Defaults to None.
            recover_from_checkpoint (bool, optional): Whether to attempt to recover from a checkpoint. Defaults to False.
            use_tqdm (bool, optional): Whether to use tqdm progress bars. Defaults to False.
        """
        self.process_func = process_func
        self.batch_size = batch_size
        self.pickle_file = pickle_file
        self.processed_items: List[T] = []
        self.results: List[R] = []
        self.recover_from_checkpoint = recover_from_checkpoint
        self.use_tqdm = use_tqdm

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

        # Recover from checkpoint if enabled
        if self.recover_from_checkpoint:
            self.load_progress()

    def process_item(self, job_number: int, item: T) -> R:
        """
        Process a single item.

        Args:
            job_number (int): The number of the job being processed.
            item (T): The item to process.

        Returns:
            R: The result of processing the item.
        """
        result = self.process_func(item)
        self.logger.info(f"Processed job {job_number}: {item}")
        return result

    def process_batch(self, batch: List[T], batch_number: int, total_jobs: int):
        """
        Process a batch of items.

        Args:
            batch (List[T]): The batch of items to process.
            batch_number (int): The number of the current batch.
            total_jobs (int): The total number of jobs to process.
        """
        if self.use_tqdm:
            batch_results = [
                self.process_item(i, item)
                for i, item in enumerate(tqdm(batch, desc=f"Batch {batch_number}"))
            ]
        else:
            batch_results = [self.process_item(i, item) for i, item in enumerate(batch)]

        self.processed_items.extend(batch)
        self.results.extend(batch_results)

        if self.pickle_file:
            self.save_progress()

        completion_message = f"Batch {batch_number} completed. Total processed: {len(self.processed_items)}/{total_jobs}"
        print(completion_message)
        self.logger.info(completion_message)

    def load_progress(self):
        """
        Load progress from a checkpoint file if it exists.
        """
        if self.pickle_file and os.path.exists(self.pickle_file):
            with open(self.pickle_file, "rb") as f:
                data = pickle.load(f)
                self.processed_items = data["processed_items"]
                self.results = data["results"]
            self.logger.info(
                f"Recovered {len(self.processed_items)} items from checkpoint"
            )
        else:
            self.logger.info(
                "No checkpoint file found or checkpoint recovery not enabled"
            )

    def save_progress(self):
        """
        Save current progress to a checkpoint file.
        """
        with open(self.pickle_file, "wb") as f:
            pickle.dump(
                {"processed_items": self.processed_items, "results": self.results},
                f,
            )

    def process_items_in_batches(
        self, input_items: Iterable[T]
    ) -> Tuple[List[T], List[R]]:
        """
        Process all input items in batches.

        Args:
            input_items (Iterable[T]): The items to process.

        Returns:
            Tuple[List[T], List[R]]: A tuple containing the list of processed items and their results.
        """
        input_items = list(input_items)  # Convert iterable to list
        total_jobs = len(input_items)
        start_index = len(self.processed_items) if self.recover_from_checkpoint else 0

        for i in range(start_index, total_jobs, self.batch_size):
            batch = input_items[i : i + self.batch_size]
            self.process_batch(batch, i // self.batch_size + 1, total_jobs)

        return self.processed_items, self.results


class AsyncBatchProcessor(BatchProcessor[T, R]):
    """
    An asynchronous version of the BatchProcessor.

    This class processes items in batches asynchronously, with optional concurrency limits.
    """

    def __init__(
        self,
        process_func: Callable[[T], R],
        batch_size: int = 100,
        pickle_file: Optional[str] = None,
        logfile: Optional[str] = None,
        recover_from_checkpoint: bool = False,
        max_concurrent: Optional[int] = None,
        use_tqdm: bool = False,
    ):
        """
        Initialize the AsyncBatchProcessor.

        Args:
            process_func (Callable[[T], R]): The function to process each item.
            batch_size (int, optional): The number of items to process in each batch. Defaults to 100.
            pickle_file (Optional[str], optional): The file to use for saving/loading progress. Defaults to None.
            logfile (Optional[str], optional): The file to use for logging. Defaults to None.
            recover_from_checkpoint (bool, optional): Whether to attempt to recover from a checkpoint. Defaults to False.
            max_concurrent (Optional[int], optional): The maximum number of concurrent operations. Defaults to None.
            use_tqdm (bool, optional): Whether to use tqdm progress bars. Defaults to False.
        """
        super().__init__(
            process_func,
            batch_size,
            pickle_file,
            logfile,
            recover_from_checkpoint,
            use_tqdm,
        )
        self.semaphore = (
            asyncio.Semaphore(max_concurrent) if max_concurrent is not None else None
        )

    async def process_item(self, job_number: int, item: T) -> R:
        """
        Process a single item asynchronously.

        Args:
            job_number (int): The number of the job being processed.
            item (T): The item to process.

        Returns:
            R: The result of processing the item.
        """

        async def _process():
            result = await self.process_func(item)
            self.logger.info(f"Processed job {job_number}: {item}")
            return result

        if self.semaphore:
            async with self.semaphore:
                return await _process()
        else:
            return await _process()

    async def process_batch(self, batch: List[T], batch_number: int, total_jobs: int):
        """
        Process a batch of items asynchronously.

        Args:
            batch (List[T]): The batch of items to process.
            batch_number (int): The number of the current batch.
            total_jobs (int): The total number of jobs to process.
        """
        if self.use_tqdm:
            pbar = tqdm(total=len(batch), desc=f"Batch {batch_number}")

        tasks = [
            self.process_item(i + (batch_number - 1) * self.batch_size, item)
            for i, item in enumerate(batch)
        ]

        batch_results = []
        for task in asyncio.as_completed(tasks):
            result = await task
            batch_results.append(result)
            if self.use_tqdm:
                pbar.update(1)

        if self.use_tqdm:
            pbar.close()

        self.processed_items.extend(batch)
        self.results.extend(batch_results)

        if self.pickle_file:
            self.save_progress()

        self.logger.info(
            f"Batch {batch_number} completed. Total processed: {len(self.processed_items)}/{total_jobs}"
        )

    async def process_items_in_batches(
        self, input_items: Iterable[T]
    ) -> Tuple[List[T], List[R]]:
        """
        Process all input items in batches asynchronously.

        Args:
            input_items (Iterable[T]): The items to process.

        Returns:
            Tuple[List[T], List[R]]: A tuple containing the list of processed items and their results.
        """
        input_items = list(input_items)  # Convert iterable to list
        total_jobs = len(input_items)
        start_index = len(self.processed_items) if self.recover_from_checkpoint else 0

        for i in range(start_index, total_jobs, self.batch_size):
            batch = input_items[i : i + self.batch_size]
            await self.process_batch(batch, i // self.batch_size + 1, total_jobs)

        return self.processed_items, self.results
