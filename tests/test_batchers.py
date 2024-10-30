import asyncio
from typing import List

import pytest

from batch_processors.batchers import AsyncBatchProcessor, BatchProcessor


def sync_process_func(item: int) -> str:
    """A simple synchronous processing function."""
    return f"Processed: {item}"


async def async_process_func(item: int) -> str:
    """A simple asynchronous processing function."""
    await asyncio.sleep(0.01)  # Simulate some async work
    return f"Processed: {item}"


@pytest.fixture
def input_data() -> List[int]:
    """Fixture to provide input data for tests."""
    return list(range(100))


@pytest.fixture
def temp_pickle_file(tmp_path):
    """Fixture to provide a temporary pickle file path."""
    return str(tmp_path / "test_checkpoint.pkl")


def test_batch_processor_init():
    """
    Test the initialization of BatchProcessor.

    This test ensures that the BatchProcessor is correctly initialized
    with the given parameters.
    """
    processor = BatchProcessor(sync_process_func, batch_size=10, use_tqdm=True)
    assert processor.batch_size == 10
    assert processor.use_tqdm
    assert callable(processor.process_func)


def test_batch_processor_process_items(input_data):
    """
    Test the processing of items using BatchProcessor.

    This test checks if the BatchProcessor correctly processes all input items
    and returns the expected results.
    """
    processor = BatchProcessor(sync_process_func, batch_size=10)
    processed_items, results = processor.process_items_in_batches(input_data)

    assert len(processed_items) == len(input_data)
    assert len(results) == len(input_data)
    assert all(result.startswith("Processed:") for result in results)


def test_batch_processor_checkpoint(input_data, temp_pickle_file):
    """
    Test the checkpoint functionality of BatchProcessor.

    This test verifies that the BatchProcessor can save and recover progress
    using a checkpoint file.
    """
    # Process half the items and save checkpoint
    processor1 = BatchProcessor(
        sync_process_func, batch_size=10, pickle_file=temp_pickle_file
    )
    processor1.process_items_in_batches(input_data[:50])

    # Create a new processor and recover from checkpoint
    processor2 = BatchProcessor(
        sync_process_func,
        batch_size=10,
        pickle_file=temp_pickle_file,
        recover_from_checkpoint=True,
    )
    processed_items, results = processor2.process_items_in_batches(input_data)

    assert len(processed_items) == len(input_data)
    assert len(results) == len(input_data)
    assert all(result.startswith("Processed:") for result in results)


@pytest.mark.asyncio
async def test_async_batch_processor_process_items(input_data):
    """
    Test the processing of items using AsyncBatchProcessor.

    This test checks if the AsyncBatchProcessor correctly processes all input items
    asynchronously and returns the expected results.
    """
    processor = AsyncBatchProcessor(async_process_func, batch_size=10)
    processed_items, results = await processor.process_items_in_batches(input_data)

    assert len(processed_items) == len(input_data)
    assert len(results) == len(input_data)
    assert all(result.startswith("Processed:") for result in results)


@pytest.mark.asyncio
async def test_async_batch_processor_max_concurrent(input_data):
    """
    Test the max_concurrent parameter of AsyncBatchProcessor.

    This test verifies that the AsyncBatchProcessor respects the max_concurrent
    limit when processing items.
    """
    max_concurrent = 5
    processor = AsyncBatchProcessor(
        async_process_func, batch_size=10, max_concurrent=max_concurrent
    )

    start_time = asyncio.get_event_loop().time()
    await processor.process_items_in_batches(input_data)
    end_time = asyncio.get_event_loop().time()

    # Calculate the expected minimum duration based on max_concurrent
    expected_min_duration = (len(input_data) / max_concurrent) * 0.01

    assert end_time - start_time >= expected_min_duration


@pytest.mark.asyncio
async def test_async_batch_processor_checkpoint(input_data, temp_pickle_file):
    """
    Test the checkpoint functionality of AsyncBatchProcessor.

    This test verifies that the AsyncBatchProcessor can save and recover progress
    using a checkpoint file.
    """
    # Process half the items and save checkpoint
    processor1 = AsyncBatchProcessor(
        async_process_func, batch_size=10, pickle_file=temp_pickle_file
    )
    await processor1.process_items_in_batches(input_data[:50])

    # Create a new processor and recover from checkpoint
    processor2 = AsyncBatchProcessor(
        async_process_func,
        batch_size=10,
        pickle_file=temp_pickle_file,
        recover_from_checkpoint=True,
    )
    processed_items, results = await processor2.process_items_in_batches(input_data)

    assert len(processed_items) == len(input_data)
    assert len(results) == len(input_data)
    assert all(result.startswith("Processed:") for result in results)


def test_tqdm_usage(capsys, input_data):
    """
    Test the tqdm progress bar functionality.

    This test checks if the tqdm progress bar is displayed when use_tqdm is set to True.
    """
    processor = BatchProcessor(sync_process_func, batch_size=10, use_tqdm=True)
    processor.process_items_in_batches(input_data)

    captured = capsys.readouterr()
    assert "Batch 1" in captured.err  # tqdm output goes to stderr
