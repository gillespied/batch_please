import asyncio
import os

import pytest

from batch_processors.batchers import AsyncBatchProcessor, BatchProcessor


def sync_process_func(string: str) -> str:
    return f"Processed: {string}"


async def async_process_func(string: str) -> str:
    await asyncio.sleep(0.1)
    return f"Processed: {string}"


@pytest.fixture
def input_items():
    return [f"item{i}" for i in range(10)]


@pytest.fixture
def sync_processor():
    return BatchProcessor(
        process_func=sync_process_func, batch_size=3, pickle_file="test_progress.pickle"
    )


@pytest.fixture
def async_processor():
    return AsyncBatchProcessor(
        process_func=async_process_func,
        batch_size=3,
        pickle_file="test_progress.pickle",
        max_concurrent=2,
    )


def test_sync_batch_processor(sync_processor, input_items):
    """
    Test that the synchronous batch processor correctly processes all input items
    and returns the expected number of processed items and results.
    """
    processed_items, results = sync_processor.process_items_in_batches(input_items)

    assert len(processed_items) == len(input_items)
    assert len(results) == len(input_items)
    assert all(result.startswith("Processed:") for result in results)


@pytest.mark.asyncio
async def test_async_batch_processor(async_processor, input_items):
    """
    Test that the asynchronous batch processor correctly processes all input items
    and returns the expected number of processed items and results.
    """
    processed_items, results = await async_processor.process_items_in_batches(
        input_items
    )

    assert len(processed_items) == len(input_items)
    assert len(results) == len(input_items)
    assert all(result.startswith("Processed:") for result in results)


def test_sync_batch_size(sync_processor, input_items):
    """
    Test that the synchronous batch processor correctly processes all items
    when the number of items is not evenly divisible by the batch size.
    """
    sync_processor.process_items_in_batches(input_items)

    assert len(sync_processor.processed_items) == len(input_items)
    assert len(sync_processor.results) == len(input_items)


@pytest.mark.asyncio
async def test_async_batch_size(async_processor, input_items):
    """
    Test that the asynchronous batch processor correctly processes all items
    when the number of items is not evenly divisible by the batch size.
    """
    await async_processor.process_items_in_batches(input_items)

    assert len(async_processor.processed_items) == len(input_items)
    assert len(async_processor.results) == len(input_items)


def test_sync_save_progress(sync_processor, input_items):
    """
    Test that the synchronous batch processor correctly saves progress to a pickle file.
    """
    sync_processor.process_items_in_batches(input_items)

    assert os.path.exists(sync_processor.pickle_file)

    # Clean up
    os.remove(sync_processor.pickle_file)


@pytest.mark.asyncio
async def test_async_save_progress(async_processor, input_items):
    """
    Test that the asynchronous batch processor correctly saves progress to a pickle file.
    """
    await async_processor.process_items_in_batches(input_items)

    assert os.path.exists(async_processor.pickle_file)

    # Clean up
    os.remove(async_processor.pickle_file)


def test_sync_logging(tmp_path):
    """
    Test that the synchronous batch processor correctly logs processing information.
    """
    logfile = tmp_path / "test.log"
    processor = BatchProcessor(
        process_func=sync_process_func, batch_size=3, logfile=str(logfile)
    )
    processor.process_items_in_batches(["test1", "test2"])

    assert logfile.exists()
    log_content = logfile.read_text()
    assert "Processed job 0: test1" in log_content
    assert "Processed job 1: test2" in log_content


@pytest.mark.asyncio
async def test_async_logging(tmp_path):
    """
    Test that the asynchronous batch processor correctly logs processing information.
    """
    logfile = tmp_path / "test.log"
    processor = AsyncBatchProcessor(
        process_func=async_process_func, batch_size=3, logfile=str(logfile)
    )
    await processor.process_items_in_batches(["test1", "test2"])

    assert logfile.exists()
    log_content = logfile.read_text()
    assert "Processed job 0: test1" in log_content
    assert "Processed job 1: test2" in log_content


@pytest.mark.asyncio
async def test_async_concurrency(async_processor, input_items):
    """
    Test that the asynchronous batch processor respects the max_concurrent limit
    and processes items in the expected time frame.
    """
    asyncBatch = AsyncBatchProcessor(
        process_func=async_process_func,
        batch_size=10,
        pickle_file="test_progress.pickle",
        max_concurrent=2,
    )
    start_time = asyncio.get_event_loop().time()
    await asyncBatch.process_items_in_batches(input_items)
    end_time = asyncio.get_event_loop().time()

    # With max_concurrent=2, it should take about 0.5 seconds (5 batches * 0.1 seconds)
    assert 0.4 < (end_time - start_time) < 0.6
