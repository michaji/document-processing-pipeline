import pytest
import pandas as pd
import json
import os
from src.extraction_module import ExtractionModule

def test_schema_compliance_and_formats():
    module = ExtractionModule(output_dir="test_output", use_mock_llm=True)
    payload = b"quality test"
    
    module.process_document("doc-3", payload)
    
    # Validate JSON
    json_path = "test_output/json/doc-3.json"
    assert os.path.exists(json_path)
    with open(json_path, 'r') as f:
        data = json.load(f)
        assert data["total_amount"] >= 0
        
    # Validate Parquet
    parquet_path = "test_output/parquet/doc-3.parquet"
    assert os.path.exists(parquet_path)
    df = pd.read_parquet(parquet_path)
    assert len(df) == 1
    assert df.iloc[0]["invoice_number"].startswith("INV-")
