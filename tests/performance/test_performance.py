import pytest
import time
from src.extraction_module import ExtractionModule
import concurrent.futures

def test_throughput():
    module = ExtractionModule(output_dir="output", use_mock_llm=True)
    start_time = time.time()

    # Simulate processing 50 documents
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i in range(50):
            payload = f"payload {i}".encode()
            futures.append(executor.submit(module.process_document, f"perf-doc-{i}", payload))

        for future in concurrent.futures.as_completed(futures):
            future.result()

    end_time = time.time()
    duration = end_time - start_time

    throughput_per_sec = 50 / duration if duration > 0 else float('inf')
    throughput_per_hour = throughput_per_sec * 3600

    # With mock LLM (pure in-memory, no network) we must comfortably exceed 4500/hr.
    # 4500/hr = 1.25 docs/sec; 50 docs must complete in under 40 seconds.
    assert throughput_per_hour >= 4500, (
        f"Throughput {throughput_per_hour:.0f} docs/hr is below the 4500 docs/hr SLA. "
        f"50 docs took {duration:.2f}s."
    )
