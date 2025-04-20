import json
import re
import logging
import time
from functools import lru_cache
import hashlib
from lambda_function import (
    bedrock_runtime,
    USE_BEDROCK,
    BEDROCK_MODEL_ID,
    _call_llm_with_retry,
    _invoke_claude37_sonnet,
    basic_lint_check,
    organize_imports,
    token_usage_stats,
    llm_response_cache
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Reuse configuration from original
MAX_RETRIES = 3
CONTEXT_SIZE_LIMIT = 200000

def assess_complexity(sas_code):
    """Enhanced heuristic analysis with weighted scoring"""
    metrics = {
        'lines': len(sas_code.split('\n')),
        'macros': len(re.findall(r'%\w+', sas_code)),
        'procs': len(re.findall(r'^\s*PROC\s+\w+', sas_code, re.MULTILINE|re.IGNORECASE)),
        'nested_macros': len(re.findall(r'%\w+\(.*%.*\)', sas_code)),
        'data_steps': len(re.findall(r'^\s*DATA\s+', sas_code, re.MULTILINE|re.IGNORECASE)),
        'includes': len(re.findall(r'%include\b', sas_code, re.IGNORECASE))
    }
    
    # Weighted scoring with adjusted weights
    score = (
        metrics['lines'] * 0.1 +
        metrics['macros'] * 2.5 +
        metrics['procs'] * 1.8 +
        metrics['nested_macros'] * 3.5 +
        metrics['data_steps'] * 1.2 +
        metrics['includes'] * 2.0
    )
    
    # Dynamic threshold based on code characteristics
    base_threshold = 15
    threshold = base_threshold + (metrics['lines'] // 1000)
    
    return {
        'strategy': 'Complex' if score > threshold else 'Simple',
        'score': score,
        'metrics': metrics,
        'threshold': threshold
    }

def hybrid_split(sas_code):
    """Intelligent splitting with macro boundary detection"""
    chunks = []
    current_chunk = []
    macro_stack = 0
    
    for line in sas_code.split('\n'):
        # Track macro nesting
        if re.match(r'^\s*%macro\b', line, re.IGNORECASE):
            macro_stack += 1
        elif re.match(r'^\s*%mend\b', line, re.IGNORECASE):
            macro_stack = max(0, macro_stack - 1)
        
        # Split at logical boundaries when not in macro
        if macro_stack == 0 and re.match(r'^\s*(PROC|DATA|%MACRO)\b', line, re.IGNORECASE):
            if current_chunk:
                chunks.append('\n'.join(current_chunk))
                current_chunk = []
        
        current_chunk.append(line)
    
    if current_chunk:
        chunks.append('\n'.join(current_chunk))
    
    return chunks

def convert_chunk(chunk, context):
    """Structured conversion prompt with code examples"""
    prompt = f"""Convert this SAS code to PySpark following these patterns:

SAS DATA Step:
DATA output;
  SET input;
  WHERE age > 25;
RUN;

PySpark:
input_df.filter(col("age") > 25)

SAS PROC SQL:
PROC SQL;
  CREATE TABLE result AS
  SELECT a.*, b.value
  FROM table1 a
  LEFT JOIN table2 b ON a.id = b.id;
QUIT;

PySpark:
table1_df.join(table2_df, col("a.id") == col("b.id"), "left") \\
          .select("a.*", "b.value")

Current SAS Code:
{chunk}

Context:
{json.dumps(context, indent=2)}

Output JSON with 'pyspark_code' and 'annotations'."""
    
    response = _call_llm_with_retry(
        lambda p: _invoke_claude37_sonnet(p),
        prompt,
        description="Chunk Conversion"
    )
    
    try:
        result = json.loads(response)
        return result.get('pyspark_code', ''), result.get('annotations', [])
    except json.JSONDecodeError:
        logger.error("Failed to parse LLM response")
        return response, []

def update_essential_context(prev_context, new_code):
    """Lightweight context tracking from original schema logic"""
    context = prev_context.copy()
    
    # Track datasets (from original schema_info logic)
    datasets = re.findall(r'(?:FROM|JOIN)\s+(\w+\.?\w+)', new_code, re.IGNORECASE)
    context.setdefault('datasets', set()).update(datasets)
    
    # Track macros (from original macro handling)
    macros = re.findall(r'%\s*(\w+)', new_code)
    context.setdefault('macros', set()).update(macros)
    
    return context

def validate_sas_syntax(sas_code):
    """Basic SAS syntax validation"""
    errors = []
    
    # Check for unclosed macros
    macro_start = len(re.findall(r'%macro\b', sas_code, re.IGNORECASE))
    macro_end = len(re.findall(r'%mend\b', sas_code, re.IGNORECASE))
    if macro_start != macro_end:
        errors.append(f"Unbalanced MACRO/MEND: {macro_start} vs {macro_end}")
    
    # Check for common syntax issues
    if re.search(r'\bRUN\s*;?\s*$', sas_code, re.IGNORECASE | re.MULTILINE):
        errors.append("Missing RUN statement")
    
    return {
        'valid': len(errors) == 0,
        'errors': errors
    }

def lambda_handler(event, context):
    """Enhanced handler with validation and improved chunk processing"""
    try:
        body = json.loads(event['body'])
        sas_code = body['sas_code']
        
        # 1. Complexity assessment (no LLM)
        complexity = assess_complexity(sas_code)
        chunks = hybrid_split(sas_code) if complexity['strategy'] == 'Complex' else [sas_code]
        
        # 2. Process chunks
        current_context = {}
        all_code = []
        annotations = []
        
        for chunk in chunks:
            chunk_code, chunk_ann = convert_chunk(chunk, current_context)
            all_code.append(chunk_code)
            annotations.extend(chunk_ann)
            current_context = update_essential_context(current_context, chunk_code)
            
            if time.time() > context.get_remaining_time_in_millis()/1000 - 30:
                logger.warning("Time limit approaching, stopping early")
                break

        # 3. Final validation (reuse original utilities)
        raw_code = '\n\n'.join(all_code)
        linted_code, _ = basic_lint_check(raw_code)
        final_code = organize_imports(linted_code)
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'pyspark_code': final_code,
                'annotations': annotations,
                'complexity': complexity,
                'chunks_processed': len(chunks),
                'token_usage': token_usage_stats
            })
        }
        
    except Exception as e:
        logger.error(f"Conversion failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# Reuse original test handler pattern
def test_handler(sas_code):
    test_event = {'body': json.dumps({'sas_code': sas_code})}
    return lambda_handler(test_event, {})
