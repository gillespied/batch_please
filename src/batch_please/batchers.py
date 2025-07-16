import asyncio
import logging
import os
import pickle
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from tqdm.auto import tqdm

# define some type generics
T = TypeVar("T")  # Type variable for input items
R = TypeVar("R")  # Type variable for result items


class BatchProcessor(Generic[T, R]):
    """
    A class for processing items in batches.

    This class takes an iterable of items or a dictionary of key-value pairs, processes them
    in batches using a provided function, and optionally saves progress to allow for checkpoint recovery.
    When using dictionary input, the keys are used as unique identifiers and the values are unpacked
    as keyword arguments to the processing function.

    Attributes:
        process_func (Callable): The function used to process each item.
            Can accept either a single item or keyword arguments from dictionary values.
        batch_size (int): The number of items to process in each batch.
        pickle_file (Optional[str]): The file to use for saving/loading progress.
        processed_items (Dict[str, R]): A dictionary mapping item keys to their processing results.
        recover_from_checkpoint (bool): Whether to attempt to recover from a checkpoint.
        use_tqdm (bool): Whether to use tqdm progress bars.
    """

    def __init__(
        self,
        process_func: Callable,
        batch_size: int = 100,
        pickle_file: Optional[str] = None,
        logfile: Optional[str] = None,
        recover_from_checkpoint: bool = False,
        use_tqdm: bool = False,
    ):
        """
        Initialize the BatchProcessor.

        Args:
            process_func (Callable): The function to process each item. Can either:
                - Accept a single positional argument when processing iterables
                - Accept keyword arguments when processing dictionaries
            batch_size (int, optional): The number of items to process in each batch. Defaults to 100.
            pickle_file (Optional[str], optional): The file to use for saving/loading progress. Defaults to None.
            logfile (Optional[str], optional): The file to use for logging. Defaults to None.
            recover_from_checkpoint (bool, optional): Whether to attempt to recover from a checkpoint. Defaults to False.
            use_tqdm (bool, optional): Whether to use tqdm progress bars. Defaults to False.
        """
        self.process_func = process_func
        self.batch_size = batch_size
        self.pickle_file = pickle_file
        self.processed_items: Dict[str, R] = {}
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

    def process_item(self, job_number: int, item: Union[T, Dict[str, Any]]) -> R:
        """
        Process a single item.

        Args:
            job_number (int): The number of the job being processed.
            item (Union[T, Dict[str, Any]]): The item to process, either a direct value
                or a dictionary of keyword arguments.

        Returns:
            R: The result of processing the item.
        """
        if isinstance(item, dict):
            result = self.process_func(**cast(Dict[str, Any], item))
        else:
            result = self.process_func(item)  # type: ignore
        self.logger.info(f"Processed job {job_number}: {item}")
        return result

    def process_batch(
        self, batch: Union[List[T], Dict[str, Any]], batch_number: int, total_jobs: int
    ):
        """
        Process a batch of items.

        Args:
            batch (Union[List[T], Dict[str, Any]]): The batch of items to process, either a list of items
                or a dictionary where keys are identifiers and values are items or dictionaries of kwargs.
            batch_number (int): The number of the current batch.
            total_jobs (int): The total number of jobs to process.
        """
        if isinstance(batch, dict):
            # Dict input - keys are identifiers, values are kwargs dictionaries
            if self.use_tqdm:
                batch_items = list(batch.items())
                batch_results = {
                    key: self.process_item(i, value)
                    for i, (key, value) in enumerate(
                        tqdm(batch_items, desc=f"Batch {batch_number}")
                    )
                }
            else:
                batch_results = {
                    key: self.process_item(i, value)
                    for i, (key, value) in enumerate(batch.items())
                }
        else:
            # Standard list input
            if self.use_tqdm:
                batch_results = {
                    str(item): self.process_item(i, item)
                    for i, item in enumerate(tqdm(batch, desc=f"Batch {batch_number}"))
                }
            else:
                batch_results = {
                    str(item): self.process_item(i, item)
                    for i, item in enumerate(batch)
                }

        self.processed_items.update(batch_results)

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
                self.processed_items = data
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
                self.processed_items,
                f,
            )

    def process_items_in_batches(
        self, input_items: Union[Iterable[T], Dict[str, Any]]
    ) -> Dict[str, R]:
        """
        Process all input items in batches.

        Args:
            input_items (Union[Iterable[T], Dict[str, Any]]): The items to process. Can be either:
                - An iterable of items (each item is passed directly to the process function)
                - A dictionary where keys are identifiers and values are dictionaries of kwargs
                  to be unpacked into the process function

        Returns:
            Dict[str, R]: A dictionary containing the processed items and their results.
                For iterable inputs, the keys are the string representation of each item.
                For dictionary inputs, the original keys are preserved.
        """
        is_dict_input = isinstance(input_items, dict)

        if is_dict_input:
            # Handle dictionary input
            dict_input = cast(Dict[str, Any], input_items)

            if self.recover_from_checkpoint:
                recovered_items = set(self.processed_items.keys())
                dict_input = {
                    k: v for k, v in dict_input.items() if k not in recovered_items
                }

            total_jobs = len(dict_input)
            dict_items = list(dict_input.items())

            for i in range(0, total_jobs, self.batch_size):
                batch_items = dict_items[i : i + self.batch_size]
                batch_dict = dict(batch_items)
                self.process_batch(batch_dict, i // self.batch_size + 1, total_jobs)
        else:
            # Handle iterable input (original behavior)
            list_input = list(input_items)  # Convert iterable to list
            if self.recover_from_checkpoint:
                recovered_items = set(self.processed_items.keys())
                list_input = [
                    item for item in list_input if str(item) not in recovered_items
                ]

            total_jobs = len(list_input)

            for i in range(0, total_jobs, self.batch_size):
                batch = list_input[i : i + self.batch_size]
                self.process_batch(batch, i // self.batch_size + 1, total_jobs)

        return self.processed_items


class AsyncBatchProcessor(BatchProcessor[T, R]):
    """
    An asynchronous version of the BatchProcessor.

    This class processes items or dictionaries of kwargs in batches asynchronously,
    with optional concurrency limits. When using dictionary input, the keys are used
    as unique identifiers and the values are unpacked as keyword arguments to the
    processing function.
    """

    def __init__(
        self,
        process_func: Callable,
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
            process_func (Callable): The async function to process each item. Can either:
                - Accept a single positional argument when processing iterables
                - Accept keyword arguments when processing dictionaries
                Must return an awaitable.
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

    async def process_item(
        self, job_number: int, item: Union[T, Dict[str, Any]]
    ) -> Tuple[Union[T, str], R]:
        """
        Process a single item asynchronously.

        Args:
            job_number (int): The number of the job being processed.
            item (Union[T, Dict[str, Any]]): The item to process, either a direct value
                or a dictionary of keyword arguments.

        Returns:
            Tuple[Union[T, str], R]: A tuple containing the input item (or its key for dict inputs)
                and the result of processing it.
        """

        async def _process():
            if isinstance(item, dict):
                result = await self.process_func(**cast(Dict[str, Any], item))
            else:
                result = await self.process_func(item)  # type: ignore
            self.logger.info(f"Processed job {job_number}: {item}")
            return item, result

        if self.semaphore:
            async with self.semaphore:
                return await _process()
        else:
            return await _process()

    async def process_batch(
        self, batch: Union[List[T], Dict[str, Any]], batch_number: int, total_jobs: int
    ):
        """
        Process a batch of items asynchronously.

        Args:
            batch (Union[List[T], Dict[str, Any]]): The batch of items to process, either a list of items
                or a dictionary where keys are identifiers and values are items or dictionaries of kwargs.
            batch_number (int): The number of the current batch.
            total_jobs (int): The total number of jobs to process.
        """
        if self.use_tqdm:
            pbar = tqdm(total=len(batch), desc=f"Batch {batch_number}")

        if isinstance(batch, dict):
            # Dictionary input
            tasks = [
                self.process_item(i + (batch_number - 1) * self.batch_size, value)
                for i, (key, value) in enumerate(batch.items())
            ]

            batch_results = {}
            for task in asyncio.as_completed(tasks):
                item_or_value, result = await task
                # For dict inputs, item_or_value will be the value dict, so we need to find the original key
                if isinstance(item_or_value, dict):
                    # Find the key that maps to this value dict
                    for k, v in batch.items():
                        if v is item_or_value:  # Identity comparison
                            batch_results[k] = result
                            break
                else:
                    # This shouldn't happen with dict input, but just in case
                    batch_results[str(item_or_value)] = result

                if self.use_tqdm:
                    pbar.update(1)
        else:
            # List input (original behavior)
            tasks = [
                self.process_item(i + (batch_number - 1) * self.batch_size, item)
                for i, item in enumerate(batch)
            ]

            batch_results = {}
            for task in asyncio.as_completed(tasks):
                item, result = await task
                batch_results[str(item)] = result
                if self.use_tqdm:
                    pbar.update(1)

        if self.use_tqdm:
            pbar.close()

        self.processed_items.update(batch_results)

        if self.pickle_file:
            self.save_progress()

        self.logger.info(
            f"Batch {batch_number} completed. Total processed: {len(self.processed_items)}/{total_jobs}"
        )

    async def process_items_in_batches(
        self, input_items: Union[Iterable[T], Dict[str, Any]]
    ) -> Dict[str, R]:
        """
        Process all input items in batches asynchronously.

        Args:
            input_items (Union[Iterable[T], Dict[str, Any]]): The items to process. Can be either:
                - An iterable of items (each item is passed directly to the process function)
                - A dictionary where keys are identifiers and values are dictionaries of kwargs
                  to be unpacked into the process function

        Returns:
            Dict[str, R]: A dictionary containing the processed items and their results.
                For iterable inputs, the keys are the string representation of each item.
                For dictionary inputs, the original keys are preserved.
        """
        is_dict_input = isinstance(input_items, dict)

        if is_dict_input:
            # Handle dictionary input
            dict_input = cast(Dict[str, Any], input_items)

            if self.recover_from_checkpoint:
                recovered_items = set(self.processed_items.keys())
                dict_input = {
                    k: v for k, v in dict_input.items() if k not in recovered_items
                }

            total_jobs = len(dict_input)
            dict_items = list(dict_input.items())

            for i in range(0, total_jobs, self.batch_size):
                batch_items = dict_items[i : i + self.batch_size]
                batch_dict = dict(batch_items)
                await self.process_batch(
                    batch_dict, i // self.batch_size + 1, total_jobs
                )
        else:
            # Handle iterable input (original behavior)
            list_input = list(input_items)  # Convert iterable to list
            if self.recover_from_checkpoint:
                recovered_items = set(self.processed_items.keys())
                list_input = [
                    item for item in list_input if str(item) not in recovered_items
                ]

            total_jobs = len(list_input)

            for i in range(0, total_jobs, self.batch_size):
                batch = list_input[i : i + self.batch_size]
                await self.process_batch(batch, i // self.batch_size + 1, total_jobs)

        return self.processed_items
