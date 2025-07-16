import asyncio
from typing import Dict, List

import pytest

from batch_please.batchers import AsyncBatchProcessor, BatchProcessor


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
def input_data_dict() -> Dict[str, Dict[str, int]]:
    """Fixture to provide dictionary input data for tests."""
    return {f"item_{i}": {"value": i} for i in range(100)}


@pytest.fixture
def empty_dict() -> Dict:
    """Fixture to provide an empty dictionary for tests."""
    return {}


@pytest.fixture
def mixed_dict() -> Dict:
    """Fixture to provide a dictionary with mixed value types for tests."""
    return {
        "item_1": {"value": 1},  # Dictionary to unpack
        "item_2": 2,  # Non-dictionary value
        "item_3": {"nested": {"value": 3}},  # Nested dictionary
    }


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
    processed_items = processor.process_items_in_batches(input_data)

    # Verify correct number of results returned
    assert len(processed_items) == len(input_data)

    # Verify the keys in the result dict are string versions of input items
    assert set(processed_items.keys()) == set(map(str, input_data))

    # Verify each key (input item) is contained in its processed value
    # This works because sync_process_func returns f"Processed: {item}"
    assert all(keys in values for keys, values in processed_items.items())

    # Verify all results have expected format
    assert all(result.startswith("Processed:") for result in processed_items.values())


def dict_process_func(value):
    """A function that processes a dictionary with a 'value' key."""
    return f"Processed: {value}"


def test_batch_processor_with_dict_input(input_data_dict):
    """
    Test the processing of dictionary inputs using BatchProcessor.

    This test checks if the BatchProcessor correctly processes all dictionary input items
    and returns the expected results with the original keys preserved.
    """
    processor = BatchProcessor(dict_process_func, batch_size=10)
    processed_items = processor.process_items_in_batches(input_data_dict)

    # Verify correct number of results returned
    assert len(processed_items) == len(input_data_dict)

    # Verify the original dictionary keys are preserved
    assert set(processed_items.keys()) == set(input_data_dict.keys())

    # Verify all results have expected format
    assert all(result.startswith("Processed:") for result in processed_items.values())

    # Check that the values were correctly processed with dict_process_func
    # which extracts and processes the 'value' key from each dictionary
    for key, value in input_data_dict.items():
        assert processed_items[key] == f"Processed: {value['value']}"


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
    processed_items = processor2.process_items_in_batches(input_data)

    # Verify correct number of results for all input items
    assert len(processed_items) == len(input_data)

    # Verify all keys are present (from both first and second processor runs)
    assert set(processed_items.keys()) == set(map(str, input_data))

    # Verify each key (input item) is contained in its processed value
    # This works because sync_process_func returns f"Processed: {item}"
    assert all(keys in values for keys, values in processed_items.items())

    # Verify all results have expected format
    assert all(result.startswith("Processed:") for result in processed_items.values())


@pytest.mark.asyncio
async def test_async_batch_processor_process_items(input_data):
    """
    Test the processing of items using AsyncBatchProcessor.

    This test checks if the AsyncBatchProcessor correctly processes all input items
    asynchronously and returns the expected results.
    """
    processor = AsyncBatchProcessor(async_process_func, batch_size=10)
    processed_items = await processor.process_items_in_batches(input_data)

    # Verify correct number of results returned
    assert len(processed_items) == len(input_data)

    # Verify the keys in the result dict are string versions of input items
    assert set(processed_items.keys()) == set(map(str, input_data))

    # Verify each key (input item) is contained in its processed value
    # This works because async_process_func returns f"Processed: {item}"
    assert all(keys in values for keys, values in processed_items.items())

    # Verify all results have expected format
    assert all(result.startswith("Processed:") for result in processed_items.values())


async def async_dict_process_func(value):
    """An async function that processes a dictionary with a 'value' key."""
    await asyncio.sleep(0.01)  # Simulate some async work
    return f"Processed: {value}"


@pytest.mark.asyncio
async def test_async_batch_processor_with_dict_input(input_data_dict):
    """
    Test the processing of dictionary inputs using AsyncBatchProcessor.

    This test checks if the AsyncBatchProcessor correctly processes all dictionary input items
    asynchronously and returns the expected results with the original keys preserved.
    """
    processor = AsyncBatchProcessor(async_dict_process_func, batch_size=10)
    processed_items = await processor.process_items_in_batches(input_data_dict)

    # Verify correct number of results returned
    assert len(processed_items) == len(input_data_dict)

    # Verify the original dictionary keys are preserved
    assert set(processed_items.keys()) == set(input_data_dict.keys())

    # Verify all results have expected format
    assert all(result.startswith("Processed:") for result in processed_items.values())

    # Check that the values were correctly processed with async_dict_process_func
    # which extracts and processes the 'value' key from each dictionary
    for key, value in input_data_dict.items():
        assert processed_items[key] == f"Processed: {value['value']}"


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
    processed_items = await processor2.process_items_in_batches(input_data)

    # Verify correct number of results for all input items
    assert len(processed_items) == len(input_data)

    # Verify all keys are present (from both first and second processor runs)
    assert set(processed_items.keys()) == set(map(str, input_data))

    # Verify each key (input item) is contained in its processed value
    # This works because async_process_func returns f"Processed: {item}"
    assert all(keys in values for keys, values in processed_items.items())

    # Verify all results have expected format
    assert all(result.startswith("Processed:") for result in processed_items.values())


def test_tqdm_usage(capsys, input_data):
    """
    Test the tqdm progress bar functionality.

    This test checks if the tqdm progress bar is displayed when use_tqdm is set to True.
    """
    processor = BatchProcessor(sync_process_func, batch_size=10, use_tqdm=True)
    processor.process_items_in_batches(input_data)

    captured = capsys.readouterr()
    assert "Batch 1" in captured.err  # tqdm output goes to stderr


def test_tqdm_usage_with_dict_input(capsys, input_data_dict):
    """
    Test the tqdm progress bar functionality with dictionary input.

    This test checks if the tqdm progress bar is displayed when use_tqdm is set to True
    and a dictionary input is provided.
    """
    processor = BatchProcessor(dict_process_func, batch_size=10, use_tqdm=True)
    processor.process_items_in_batches(input_data_dict)

    captured = capsys.readouterr()
    assert "Batch 1" in captured.err  # tqdm output goes to stderr
