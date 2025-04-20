# SAS to PySpark Converter

A powerful tool for automatically converting SAS code to PySpark using AWS Lambda and Claude AI.

## Features

- Intelligent adaptive chunking based on code size
- Smart boundary detection for preserving code structure
- Handles small to very large SAS code files
- Comprehensive logging of token usage and metrics
- Detailed annotations for better understanding
- AWS Lambda-optimized for efficient execution

## Usage

The converter automatically detects the size and complexity of SAS code and uses the appropriate conversion strategy:

- For code under 2000 lines: Direct conversion without chunking
- For code over 2000 lines: Intelligent chunking with 3 chunks at logical boundaries

## Requirements

- Python 3.13+
- AWS Lambda environment
- AWS Bedrock with Claude 3.7 Sonnet
- See requirements.txt for Python dependencies

## Example

```python
import boto3
import json
from lambda_function import convert_sas_to_pyspark

# Load SAS code
with open('your_sas_file.sas', 'r') as f:
    sas_code = f.read()

# Convert to PySpark
result = convert_sas_to_pyspark(sas_code)

# Get the converted code
pyspark_code = result['pyspark_code']
confidence = result['confidence_score']
annotations = result['annotations']

print(f"Conversion complete with confidence: {confidence}")
``` 