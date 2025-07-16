# batch_please

A flexible and efficient Python library for processing large datasets in batches, with support for both synchronous and asynchronous operations.

## Features

- Process large datasets in customizable batch sizes
- Support for both synchronous and asynchronous processing
- Flexible input handling - accepts iterables or dictionaries of keyword arguments
- Checkpoint functionality to resume processing from where it left off
- Optional progress bar using tqdm
- Configurable concurrency limit for asynchronous processing
- Logging support

## Installation

```bash
pip install batch_please
```

## Quick Start

### Synchronous Processing

```python
from batch_please import BatchProcessor

# Simple iterable input
def process_func(item):
    return f"Processed: {item}"

processor = BatchProcessor(
    process_func=process_func,
    batch_size=100,
    use_tqdm=True
)

input_data = range(1000)
processed_items = processor.process_items_in_batches(input_data)

# Dictionary input with keyword arguments
def process_with_params(name, value):
    return f"{name}: {value}"

processor = BatchProcessor(
    process_func=process_with_params,
    batch_size=100,
    use_tqdm=True
)

input_data_dict = {
    f"item_{i}": {"name": f"Item {i}", "value": i * 10} 
    for i in range(100)
}
processed_items = processor.process_items_in_batches(input_data_dict)
```

### Asynchronous Processing

```python
import asyncio
from batch_please import AsyncBatchProcessor

# Simple iterable input
async def async_process_func(item):
    await asyncio.sleep(0.1)
    return f"Processed: {item}"

async def main():
    processor = AsyncBatchProcessor(
        process_func=async_process_func,
        batch_size=100,
        max_concurrent=10,
        use_tqdm=True
    )

    input_data = range(1000)
    processed_items = await processor.process_items_in_batches(input_data)
    
    # Dictionary input with keyword arguments
    async def process_with_params(name, value):
        await asyncio.sleep(0.1)
        return f"{name}: {value}"
    
    processor = AsyncBatchProcessor(
        process_func=process_with_params,
        batch_size=100,
        max_concurrent=10,
        use_tqdm=True
    )
    
    input_data_dict = {
        f"item_{i}": {"name": f"Item {i}", "value": i * 10} 
        for i in range(100)
    }
    processed_items = await processor.process_items_in_batches(input_data_dict)

asyncio.run(main())
```

## Advanced Usage

### Checkpoint Recovery

```python
processor = BatchProcessor(
    process_func=process_func,
    batch_size=100,
    pickle_file="checkpoint.pkl",
    recover_from_checkpoint=True
)
```

### Logging

```python
processor = BatchProcessor(
    process_func=process_func,
    batch_size=100,
    logfile="processing.log"
)
```
