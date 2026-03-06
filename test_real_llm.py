import os
from src.extraction_module import ExtractionModule

# A raw, realistic payload
sample_invoice = b"""
Acme Corp
123 Main St, Springfield, IL 62701
Phone: (555) 123-4567

INVOICE
Invoice Number: INV-2023-084A
Date: 2023-10-24
Due Date: 2023-11-23

Bill To:
Wayne Enterprises
1007 Mountain Drive, Gotham
Contact: Bruce W.

Description                     Qty   Unit Price      Total
------------------------------------------------------------
Industrial Grappling Hook         2     $ 450.00   $  900.00
Heavy Duty Kevlar Plating         5     $ 120.00   $  600.00
Smoke Pellets (Box of 100)        1     $  50.00   $   50.00
------------------------------------------------------------
Subtotal:                                          $ 1550.00
Tax (8%):                                          $  124.00
Total Amount:                                      $ 1674.00
"""

def main():
    print("Initializing ExtractionModule (real LLM mode)...")
    module = ExtractionModule(output_dir="tmp_output", use_mock_llm=False)
    
    print("\nProcessing sample invoice...")
    status, result = module.process_document("doc-test-real-1", sample_invoice)
    
    print(f"\nStatus: {status}")
    print("\nExtracted Data:")
    for key in ["invoice_number", "vendor_name", "invoice_date", "total_amount"]:
        print(f"  {key}: {result.get(key)} (conf: {result.get(key + '_confidence')})")
        
    print(f"\nQuality Score: {result.get('quality_metrics', {}).get('quality_score')}")
    print(f"Needs Review? : {result.get('needs_review')}")

if __name__ == "__main__":
    main()
