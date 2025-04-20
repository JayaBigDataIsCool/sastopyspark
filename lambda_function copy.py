import json
import re
import logging
import time
import boto3
import os
import hashlib
from functools import lru_cache
from botocore.config import Config
import ast # For basic syntax check

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Bedrock Client Initialization ---
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
boto_config = Config(read_timeout=3600) # Increased timeout

try:
    bedrock_runtime = boto3.client('bedrock-runtime', region_name=AWS_REGION, config=boto_config)
    USE_BEDROCK = True
    logger.info("Successfully initialized Bedrock client")
except Exception as e:
    logger.warning(f"Failed to initialize Bedrock client: {e}. Will run in demo/test mode only.")
    bedrock_runtime = None
    USE_BEDROCK = False

# --- Configuration ---
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-3-sonnet-20240229-v1:0") # Defaulting to Sonnet 3
ANTHROPIC_VERSION = "bedrock-2023-05-31" # Required for Claude 3 models

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.environ.get("RETRY_DELAY_SECONDS", "2"))
RETRY_BACKOFF_FACTOR = float(os.environ.get("RETRY_BACKOFF_FACTOR", "2.0"))
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "4000")) # Default max tokens for Sonnet 3 response
CONTEXT_SIZE_LIMIT = int(os.environ.get("CONTEXT_SIZE_LIMIT", "150000")) # Approx limit before summarization
REFINEMENT_THRESHOLD = float(os.environ.get("REFINEMENT_THRESHOLD", "3.5")) # Confidence score below which refinement is triggered
MAX_REFINEMENT_ATTEMPTS = int(os.environ.get("MAX_REFINEMENT_ATTEMPTS", "1")) # Limit refinement loops

# Complexity Analyzer Thresholds (Configurable via Env Vars)
SIMPLE_MAX_LINES = int(os.environ.get("SIMPLE_MAX_LINES", "500"))
SIMPLE_MAX_MACROS = int(os.environ.get("SIMPLE_MAX_MACROS", "5"))
SIMPLE_MAX_PROCS = int(os.environ.get("SIMPLE_MAX_PROCS", "10"))
# Optional complexity thresholds (can be added if needed)
# SIMPLE_MAX_MACRO_CALLS = int(os.environ.get("SIMPLE_MAX_MACRO_CALLS", "20"))
# Optional complexity thresholds for more fine-grained control
# COMPLEX_PROC_LIST = os.environ.get("COMPLEX_PROC_LIST", "FCMP,LUA,PROTO,OPTMODEL,IML").split(',')
# CHECK_INCLUDES = os.environ.get("CHECK_INCLUDES", "FALSE").upper() == "TRUE"


# Caching Configuration
USE_LLM_CACHING = os.environ.get("USE_LLM_CACHING", "TRUE").upper() == "TRUE"
MAX_CACHE_ENTRIES = int(os.environ.get("MAX_CACHE_ENTRIES", "100"))
llm_response_cache = {}

# Define error handling retry statuses for Bedrock
BEDROCK_RETRY_EXCEPTIONS = [
    'ThrottlingException',
    'ServiceUnavailableException',
    'InternalServerException',
    'ModelTimeoutException',
    'ModelErrorException', # Added based on potential Bedrock errors
    'ValidationException' # Added for input validation issues
]

# Initialize global statistics
token_usage_stats = {
    'total_tokens': 0,
    'input_tokens': 0,
    'output_tokens': 0,
    'api_calls': 0
}
llm_call_counts = {
    'complexity_analysis': 0,
    'simple_conversion': 0,
    'structural_analysis': 0,
    'context_focus': 0, # Not implemented in this version, placeholder
    'chunk_conversion': 0,
    'refinement': 0,
    'summarization': 0
}

# --- Caching ---
def cache_key(prompt, model_id=None):
    """Generate a cache key from the prompt and optional model ID"""
    key_content = f"{prompt}:{model_id or BEDROCK_MODEL_ID}"
    return hashlib.md5(key_content.encode('utf-8')).hexdigest()

def manage_cache_size():
    """Removes oldest entries if cache exceeds max size."""
    if len(llm_response_cache) > MAX_CACHE_ENTRIES:
        # Simple FIFO eviction
        num_to_remove = len(llm_response_cache) - MAX_CACHE_ENTRIES
        keys_to_remove = list(llm_response_cache.keys())[:num_to_remove]
        for key in keys_to_remove:
            del llm_response_cache[key]
        logger.info(f"Cache full. Removed {num_to_remove} oldest entries.")

# --- Bedrock LLM Interaction ---
def _invoke_claude3(prompt, max_tokens=MAX_TOKENS):
    """
    Invokes a Claude 3 model on Bedrock (handles Sonnet/Haiku).
    Handles the specific request/response format for Anthropic models via Bedrock.
    Updates global token usage stats.
    """
    if not USE_BEDROCK or bedrock_runtime is None:
        logger.warning("Bedrock is not available. Returning demo response.")
        # Return a structured demo response matching expected output formats
        return """{"pyspark_code": "# Demo mode - Bedrock not available\nprint('Demo conversion')",
                   "annotations": [{"sas_lines": [1, 1], "pyspark_lines": [1, 1], "note": "Demo mode", "severity": "Info"}],
                   "confidence_score": 5, "refinement_notes": ""}""" # Include fields for all call types

    messages = [{"role": "user", "content": prompt}]
    body = {
        "anthropic_version": ANTHROPIC_VERSION,
        "max_tokens": max_tokens,
        "messages": messages,
        "temperature": float(os.environ.get("LLM_TEMPERATURE", "0.1")), # Lower temp for more deterministic output
        "top_p": float(os.environ.get("LLM_TOP_P", "0.9")),
    }

    try:
        logger.info(f"Invoking {BEDROCK_MODEL_ID} with max_tokens={max_tokens}")
        start_time = time.time()
        response = bedrock_runtime.invoke_model(
            body=json.dumps(body),
            modelId=BEDROCK_MODEL_ID,
            accept='application/json',
            contentType='application/json'
        )
        response_time = time.time() - start_time
        response_body = json.loads(response['body'].read())

        # Track token usage
        input_tokens = response_body.get("usage", {}).get("input_tokens", 0)
        output_tokens = response_body.get("usage", {}).get("output_tokens", 0)
        total_tokens = input_tokens + output_tokens

        logger.info(f"Token usage: {input_tokens} input + {output_tokens} output = {total_tokens} total tokens")
        logger.info(f"Response time: {response_time:.2f}s")

        # Update global stats
        token_usage_stats['total_tokens'] += total_tokens
        token_usage_stats['input_tokens'] += input_tokens
        token_usage_stats['output_tokens'] += output_tokens
        token_usage_stats['api_calls'] += 1

        # Extract content
        if response_body.get("type") == "message" and response_body.get("content"):
            llm_output = response_body["content"][0]["text"]
            return llm_output
        elif response_body.get("error"):
             error_type = response_body["error"].get("type")
             error_message = response_body["error"].get("message")
             logger.error(f"Bedrock API Error: {error_type} - {error_message}")
             # Raise specific exception types if needed for retry logic
             if error_type == 'overloaded_error':
                 raise Exception("Model overloaded") # Or a custom exception
             else:
                 raise Exception(f"Bedrock Error: {error_type} - {error_message}")
        else:
            logger.error(f"Unexpected Bedrock response format: {response_body}")
            raise ValueError("Unexpected response format from Bedrock Claude 3")

    except Exception as e:
        logger.error(f"Bedrock invoke_model failed: {e}", exc_info=True)
        raise # Re-raise to be caught by retry logic

def _call_llm_with_retry(invoke_function, prompt, description="LLM call", llm_call_type_key=None, bypass_cache=False, max_tokens=MAX_TOKENS):
    """
    Wrapper for retry logic around Bedrock invoke_model calls. Includes caching and exponential backoff.
    """
    # Check cache first
    cache_key_val = cache_key(prompt)
    if USE_LLM_CACHING and not bypass_cache and cache_key_val in llm_response_cache:
        logger.info(f"Cache hit for {description}! Using cached response.")
        return llm_response_cache[cache_key_val]

    # Proceed with API call
    last_exception = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            logger.info(f"Attempting {description} (Attempt {attempt + 1}/{MAX_RETRIES + 1})...")
            result = invoke_function(prompt, max_tokens=max_tokens) # Pass max_tokens here

            # Store in cache
            if USE_LLM_CACHING:
                llm_response_cache[cache_key_val] = result
                manage_cache_size() # Check and manage cache size after adding

            # Increment specific LLM call counter if key provided
            if llm_call_type_key and llm_call_type_key in llm_call_counts:
                llm_call_counts[llm_call_type_key] += 1

            return result # Success

        except Exception as e:
            last_exception = e
            error_name = type(e).__name__
            error_str = str(e)
            logger.warning(f"{description} attempt {attempt + 1} failed: {error_name} - {error_str}")

            # Check if retryable
            is_retryable = any(retry_err in error_name for retry_err in BEDROCK_RETRY_EXCEPTIONS) or \
                           any(retry_err in error_str for retry_err in BEDROCK_RETRY_EXCEPTIONS)

            if attempt < MAX_RETRIES and is_retryable:
                delay = RETRY_DELAY_SECONDS * (RETRY_BACKOFF_FACTOR ** attempt)
                logger.info(f"Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"{description} failed after {attempt + 1} attempts.")
                break # Exit loop after final attempt or non-retryable error

    # If loop finished without success, raise the last exception
    raise last_exception or Exception(f"{description} failed after {MAX_RETRIES + 1} attempts.")


# --- Complexity Analysis ---
def assess_complexity(sas_code, thresholds):
    """
    Analyzes SAS code complexity based on heuristics to determine the conversion path.

    Args:
        sas_code (str): The SAS code.
        thresholds (dict): Dictionary containing complexity thresholds like
                           {'SIMPLE_MAX_LINES': 500, 'SIMPLE_MAX_MACROS': 5, ...}.

    Returns:
        str: "Simple" or "Complex".
    """
    logger.info("Assessing SAS code complexity using heuristics...")
    loc = sas_code.count('\n') + 1
    # char_count = len(sas_code) # Less reliable metric on its own

    # Core metrics
    macro_defs = len(re.findall(r'^\s*%MACRO\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))
    proc_calls = len(re.findall(r'^\s*PROC\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))

    # Optional advanced metrics (can be enabled via config/thresholds)
    # macro_calls_approx = len(re.findall(r'%\w+\s*\(', sas_code, re.IGNORECASE)) # Approximate macro calls
    # complex_procs_found = []
    # if 'COMPLEX_PROC_LIST' in thresholds:
    #     complex_proc_pattern = r'^\s*PROC\s+(' + '|'.join(thresholds['COMPLEX_PROC_LIST']) + r')\b'
    #     complex_procs_found = re.findall(complex_proc_pattern, sas_code, re.IGNORECASE | re.MULTILINE)
    # has_includes = False
    # if thresholds.get('CHECK_INCLUDES', False):
    #     has_includes = bool(re.search(r'^\s*%INCLUDE\b', sas_code, re.IGNORECASE | re.MULTILINE))

    # Classification Rule
    is_simple = (
        loc <= thresholds.get('SIMPLE_MAX_LINES', 500) and
        macro_defs <= thresholds.get('SIMPLE_MAX_MACROS', 5) and
        proc_calls <= thresholds.get('SIMPLE_MAX_PROCS', 10) #and
        # Optional checks:
        # macro_calls_approx <= thresholds.get('SIMPLE_MAX_MACRO_CALLS', 20) and
        # not complex_procs_found and
        # not has_includes
    )

    complexity = "Simple" if is_simple else "Complex"
    logger.info(f"Complexity assessed as: {complexity} (LoC: {loc}, Macro Defs: {macro_defs}, PROC Calls: {proc_calls})")
    # logger.debug(f"Optional metrics - Approx Macro Calls: {macro_calls_approx}, Complex PROCs: {complex_procs_found}, Has Includes: {has_includes}")
    return complexity

# --- Code Splitting ---
def deterministic_split(sas_code, max_chunk_size=50000):
    """
    Splits SAS code into super-chunks based on PROC/DATA/MACRO boundaries.
    (Reusing the existing robust implementation)
    """
    logger.info("Performing deterministic splitting into super-chunks...")
    if len(sas_code) < 1000: # Optimization for small code
        logger.info("Code is small, treating as a single chunk.")
        return [sas_code]

    # Regex to find major boundaries (start of line, case-insensitive)
    boundary_pattern = re.compile(r'^\s*(PROC\s+\w+|DATA\s+[\w\.]+|%MACRO\s+[\w\.]+)\b.*?;', re.IGNORECASE | re.MULTILINE | re.DOTALL)
    boundaries = list(boundary_pattern.finditer(sas_code))

    if not boundaries:
        logger.warning("No clear PROC/DATA/MACRO boundaries found. Treating as one chunk.")
        return [sas_code]

    logger.info(f"Found {len(boundaries)} potential split points.")
    super_chunks = []
    start_index = 0
    current_chunk_start_index = 0

    for i, match in enumerate(boundaries):
        split_point = match.start()

        # If this isn't the first boundary and the current chunk exceeds max size, split before this boundary
        if i > 0 and (split_point - current_chunk_start_index > max_chunk_size):
            chunk = sas_code[current_chunk_start_index:start_index] # Split at the *previous* boundary
            if chunk.strip():
                super_chunks.append(chunk)
                logger.info(f"Created super-chunk (size: {len(chunk)} chars)")
                current_chunk_start_index = start_index # Start next chunk from previous boundary

        # Update the potential end of the current logical block
        start_index = match.start()

    # Add the final chunk (from the last boundary start to the end)
    final_chunk = sas_code[current_chunk_start_index:]
    if final_chunk.strip():
         # If the final chunk itself is too large, try an emergency split (e.g., by lines)
        if len(final_chunk) > max_chunk_size * 1.2: # Allow some overshoot
             logger.warning(f"Final chunk is very large ({len(final_chunk)} chars). Performing emergency line split.")
             lines = final_chunk.splitlines()
             temp_chunk = []
             current_len = 0
             for line in lines:
                 line_len = len(line) + 1 # Account for newline
                 if current_len + line_len > max_chunk_size and temp_chunk:
                     super_chunks.append("\n".join(temp_chunk))
                     logger.info(f"Created super-chunk (emergency split, size: {current_len} chars)")
                     temp_chunk = [line]
                     current_len = line_len
                 else:
                     temp_chunk.append(line)
                     current_len += line_len
             if temp_chunk: # Add remaining lines
                 super_chunks.append("\n".join(temp_chunk))
                 logger.info(f"Created super-chunk (emergency split, size: {current_len} chars)")
        else:
            super_chunks.append(final_chunk)
            logger.info(f"Created final super-chunk (size: {len(final_chunk)} chars)")


    logger.info(f"Split SAS code into {len(super_chunks)} super-chunks.")
    return [chunk for chunk in super_chunks if chunk.strip()]


# --- Structural Analysis (Complex Path) ---
def call_llm_for_structural_analysis(super_chunk):
    """Calls LLM to identify logical chunks within a super-chunk."""
    logger.info("Calling LLM for structural analysis...")
    # Optimization: Skip LLM for very small super-chunks
    if len(super_chunk.splitlines()) < 10: # Heuristic threshold
        logger.info("Super-chunk is small, creating a single default logical chunk.")
        return [{
            "type": "UNKNOWN", "name": "small_chunk",
            "start_line": 1, "end_line": len(super_chunk.splitlines()),
            "inputs": [], "outputs": []
        }]

    prompt = f"""Analyze the following SAS code block. Identify all distinct logical units (DATA steps, PROC steps, defined %MACROs).
Output ONLY a valid JSON list where each item represents a unit and includes:
{{
  "type": "DATA" | "PROC" | "MACRO",
  "name": "step/proc/macro_name", // Best guess for the name
  "start_line": <integer>, // Starting line number (1-based) within this super_chunk
  "end_line": <integer>, // Ending line number (1-based) within this super_chunk
  "inputs": ["dataset_or_macro_used"], // List of likely input dataset/macro names used
  "outputs": ["dataset_or_macro_created"] // List of likely output dataset/macro names created
}}
Ensure the output is ONLY the JSON list, starting with '[' and ending with ']'. Be precise with line numbers.

SAS Code Block:
```sas
{super_chunk}
```"""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Structural Analysis", llm_call_type_key='structural_analysis')
    try:
        # Clean potential markdown fences
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        logical_chunks = json.loads(cleaned_response)
        if isinstance(logical_chunks, list):
            # Basic validation
            for chunk in logical_chunks:
                if not all(k in chunk for k in ["type", "name", "start_line", "end_line", "inputs", "outputs"]):
                    raise ValueError("Invalid chunk structure in structural analysis response.")
            logger.info(f"LLM identified {len(logical_chunks)} logical chunks.")
            # Handle empty list case
            if not logical_chunks:
                 logger.warning("LLM returned empty chunk list, creating default chunk for the super-chunk.")
                 return [{
                    "type": "UNKNOWN", "name": "default_chunk",
                    "start_line": 1, "end_line": len(super_chunk.splitlines()),
                    "inputs": [], "outputs": []
                 }]
            return logical_chunks
        else:
            raise ValueError("LLM response for structural analysis was not a JSON list.")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON for structural analysis: {e}\nResponse: {response_text}")
        # Fallback: treat the whole super-chunk as one logical chunk
        return [{"type": "UNKNOWN", "name": "fallback_chunk", "start_line": 1, "end_line": len(super_chunk.splitlines()), "inputs": [], "outputs": []}]
    except Exception as e:
        logger.error(f"Error processing structural analysis: {e}\nResponse: {response_text}")
        raise # Re-raise other errors


# --- Context Management (Complex Path) ---
# Context Focusing - Placeholder for future optimization
# def call_llm_for_context_focus(overall_context, logical_chunk_code): ...

def update_overall_context(overall_context, logical_chunk, pyspark_output, annotations):
    """
    Updates the overall context with outputs from a processed logical chunk.
    Focuses on schema and macro definitions.
    """
    logger.debug(f"Updating overall context after processing chunk: {logical_chunk.get('name')}")
    if "schema_info" not in overall_context: overall_context["schema_info"] = {}
    if "macros_defined" not in overall_context: overall_context["macros_defined"] = {}
    if "processed_chunk_outputs" not in overall_context: overall_context["processed_chunk_outputs"] = {}

    chunk_type = logical_chunk.get('type', '').upper()
    chunk_name = logical_chunk.get('name', 'unnamed')

    # Store outputs (datasets/macros created)
    outputs = logical_chunk.get('outputs', [])
    overall_context["processed_chunk_outputs"][chunk_name] = outputs

    for output_name in outputs:
        if chunk_type == 'MACRO':
            # Basic macro definition tracking
            overall_context["macros_defined"][output_name] = {
                "status": "defined",
                "source_chunk": chunk_name,
                "parameters": [], # TODO: Could try to extract params from code/annotations
                "body_preview": pyspark_output[:200] # Store a preview
            }
            logger.debug(f"Added/Updated macro definition: {output_name}")
        elif chunk_type in ['DATA', 'PROC']:
            # Basic schema tracking - could be enhanced by LLM extracting schema from code/annotations
            # For now, just mark the dataset as created
            overall_context["schema_info"][output_name] = {
                "status": "created/modified",
                "source_chunk": chunk_name,
                "derived_from": logical_chunk.get('inputs', []),
                "columns": ["unknown"] # Placeholder - needs schema extraction logic
            }
            logger.debug(f"Added/Updated schema info for dataset: {output_name}")

    # Check context size and summarize if needed
    try:
        current_context_size = len(json.dumps(overall_context))
        logger.debug(f"Current estimated context size: {current_context_size} chars.")
        if current_context_size > CONTEXT_SIZE_LIMIT:
            logger.warning(f"Context size ({current_context_size}) exceeds limit ({CONTEXT_SIZE_LIMIT}). Attempting summarization...")
            overall_context = call_llm_for_summarization(overall_context)
            new_size = len(json.dumps(overall_context))
            logger.info(f"Context size after summarization: {new_size} chars.")
    except TypeError as e:
        logger.warning(f"Could not estimate context size or summarize due to content: {e}")
    except Exception as e:
         logger.error(f"Error during context size check/summarization: {e}")


    return overall_context

def call_llm_for_summarization(overall_context):
    """Calls LLM to summarize the overall context if it gets too large."""
    logger.info("Calling LLM for context summarization...")
    try:
        context_to_summarize_json = json.dumps(overall_context, indent=2)
    except TypeError:
         logger.error("Cannot serialize context for summarization. Skipping.")
         return overall_context # Return original if cannot serialize

    prompt = f"""Summarize the key information in the provided context JSON concisely, preserving essential details for downstream SAS to PySpark conversion.
Focus on:
1. Dataset schemas (column names, types if available) for the *most recently* created/modified datasets.
2. All defined macros and their parameters/previews.
3. Key dataset lineage (outputs of recent chunks).

Output ONLY a valid JSON object with the same structure as the input, but summarized. Remove older schema entries or less critical details if necessary to reduce size.

Context JSON to Summarize:
```json
{context_to_summarize_json}
```

Output ONLY the summarized JSON object."""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Context Summarization", llm_call_type_key='summarization')
    try:
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        summarized_context = json.loads(cleaned_response)
        if isinstance(summarized_context, dict):
            logger.info("Context summarization successful.")
            return summarized_context
        else:
            raise ValueError("LLM response for summarization was not a JSON object.")
    except Exception as e:
        logger.error(f"Failed to process summarization response: {e}. Returning original context.")
        return overall_context # Fallback to original context on error


# --- Code Conversion (Simple and Complex Paths) ---
def call_llm_for_conversion(code_to_convert, context, is_simple_path=False, chunk_info=None):
    """
    Calls LLM to convert SAS code (either full script or a chunk) to PySpark.
    Returns pyspark_code, annotations, and confidence score.
    """
    logger.info(f"Calling LLM for conversion ({'Simple Path' if is_simple_path else 'Chunk: '+chunk_info.get('name', 'N/A')})...")
    try:
        context_json = json.dumps(context, indent=2, default=str) # Use default=str for non-serializable items
    except TypeError as e:
        logger.warning(f"Could not serialize context for conversion: {e}. Using empty context.")
        context_json = "{}"

    path_specific_instructions = ""
    if is_simple_path:
        path_specific_instructions = "You are converting the entire SAS script in one go."
        llm_call_key = 'simple_conversion'
    else:
        path_specific_instructions = f"You are converting a logical chunk identified as '{chunk_info.get('name', 'N/A')}' (type: {chunk_info.get('type', 'N/A')}). Focus on this chunk, using the provided context for dependencies."
        llm_call_key = 'chunk_conversion'

    prompt = f"""Convert the following SAS code {'snippet' if not is_simple_path else 'script'} to equivalent PySpark code.

{path_specific_instructions}

Context (use this for dependencies like schemas, macros):
```json
{context_json}
```

SAS Code to Convert:
```sas
{code_to_convert}
```

Conversion Guidelines:
- Maintain semantic equivalence.
- Use idiomatic PySpark (DataFrame API).
- Include necessary imports (placeholders are fine, they will be consolidated later).
- Add comments explaining the conversion logic, especially for complex parts or assumptions. Reference original SAS line numbers if possible.
- Handle SAS-specific features (e.g., macro variables, data step logic, PROCs) appropriately.
- If a direct conversion isn't possible, implement the closest equivalent and add a 'Warning' annotation.
- Assess your confidence in the conversion quality for this specific piece of code.

Output ONLY a valid JSON object with the following structure:
{{
  "pyspark_code": "...",
  "annotations": [
    {{
      "sas_lines": [<start_line_int>, <end_line_int>], // Relative to the input SAS code snippet/script
      "pyspark_lines": [<start_line_int>, <end_line_int>], // Relative to the generated PySpark code
      "note": "Explanation...",
      "severity": "Info" | "Warning"
    }}
  ],
  "confidence_score": <float 1.0-5.0> // Your confidence in this specific conversion's accuracy
}}
Ensure the output is ONLY the JSON object.
"""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Conversion", llm_call_type_key=llm_call_key)
    try:
        # Clean potential markdown fences and control characters
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        cleaned_response = ''.join(c for c in cleaned_response if c.isprintable() or c in '\n\r\t') # Keep whitespace

        result = json.loads(cleaned_response)

        # Validate structure
        if isinstance(result, dict) and \
           all(k in result for k in ["pyspark_code", "annotations", "confidence_score"]) and \
           isinstance(result["pyspark_code"], str) and \
           isinstance(result["annotations"], list) and \
           isinstance(result["confidence_score"], (int, float)):

            # Basic validation of annotations
            valid_annotations = []
            for ann in result["annotations"]:
                if isinstance(ann, dict) and all(k in ann for k in ["sas_lines", "pyspark_lines", "note", "severity"]):
                    valid_annotations.append(ann)
                else:
                     logger.warning(f"Skipping invalid annotation structure: {ann}")
            result["annotations"] = valid_annotations

            logger.info(f"LLM conversion successful. Confidence: {result['confidence_score']:.1f}/5.0")
            return result["pyspark_code"], result["annotations"], float(result["confidence_score"])
        else:
            raise ValueError("LLM response for conversion has invalid structure.")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON for conversion: {e}\nResponse: {response_text}")
        # Fallback: return error code, annotation, and low confidence
        error_code = f"# ERROR: Failed to parse LLM conversion response.\n# Original SAS:\n# {code_to_convert.replace(chr(10), chr(10)+'# ')}"
        error_ann = [{'sas_lines': [1,1], 'pyspark_lines': [1,1], 'note': f'LLM response was not valid JSON: {e}', 'severity': 'Warning'}]
        return error_code, error_ann, 1.0 # Low confidence
    except Exception as e:
        logger.error(f"Error processing conversion response: {e}\nResponse: {response_text}")
        error_code = f"# ERROR: Exception processing conversion response: {e}\n# Original SAS:\n# {code_to_convert.replace(chr(10), chr(10)+'# ')}"
        error_ann = [{'sas_lines': [1,1], 'pyspark_lines': [1,1], 'note': f'Exception during conversion processing: {e}', 'severity': 'Warning'}]
        return error_code, error_ann, 1.0 # Low confidence


# --- Code Refinement (Complex Path) ---
def call_llm_for_refinement(pyspark_section, context, previous_notes=None):
    """Calls LLM to refine a PySpark code section and assess confidence."""
    logger.info("Calling LLM for refinement...")
    try:
        context_json = json.dumps(context, indent=2, default=str)
    except TypeError:
        context_json = "{}" # Send empty if serialization fails

    refinement_focus = "Focus on correctness against SAS semantics, Spark efficiency, PEP 8 style, and clarity."
    if previous_notes:
        refinement_focus = f"Confidence was low previously. Focus on addressing these notes:\n'''\n{previous_notes}\n'''\nAlso perform a general review."

    prompt = f"""Review and refine the following PySpark code section.
{refinement_focus}

Context (for reference):
```json
{context_json}
```

PySpark Code Section to Refine:
```python
{pyspark_section}
```

Output ONLY a valid JSON object with:
{{
  "refined_code": "...", // The improved PySpark code
  "confidence_score": <float 1.0-5.0>, // Updated confidence score
  "refinement_notes": "..." // Any *new* or *remaining* issues or suggestions
}}
Ensure the output is ONLY the JSON object.
"""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Refinement", llm_call_type_key='refinement')
    try:
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        cleaned_response = ''.join(c for c in cleaned_response if c.isprintable() or c in '\n\r\t')

        result = json.loads(cleaned_response)

        # Validate structure
        if isinstance(result, dict) and \
           all(k in result for k in ["refined_code", "confidence_score", "refinement_notes"]) and \
           isinstance(result["refined_code"], str) and \
           isinstance(result["confidence_score"], (int, float)) and \
           isinstance(result["refinement_notes"], str):
            logger.info(f"LLM refinement successful. New Confidence: {result['confidence_score']:.1f}/5.0")
            return result["refined_code"], float(result["confidence_score"]), result["refinement_notes"]
        else:
            raise ValueError("LLM response for refinement has invalid structure.")
    except Exception as e:
        logger.error(f"Failed to process refinement response: {e}. Returning original code and low confidence.")
        # Fallback: return original code, low confidence, error note
        return pyspark_section, 1.0, f"Error processing refinement response: {e}"


# --- Code Assembly & Formatting ---
def assemble_and_format_code(pyspark_pieces):
    """Joins PySpark code pieces, moves imports, and formats."""
    logger.info("Assembling and formatting final PySpark code...")
    full_code = "\n\n# --- Autogenerated Chunk Separator ---\n\n".join(pyspark_pieces)

    # Extract and consolidate imports
    imports = set()
    body_lines = []
    import_pattern = re.compile(r'^\s*(import\s+[\w\.,\s]+|from\s+\w+(\.\w+)*\s+import\s+[\w\*\.,\s\(\)]+)')

    for line in full_code.splitlines():
        match = import_pattern.match(line)
        if match:
            # Normalize and add unique imports
            import_statement = match.group(0).strip()
            imports.add(import_statement)
        else:
            body_lines.append(line)

    # Sort imports (basic alphabetical sort)
    sorted_imports = sorted(list(imports))
    final_code = "\n".join(sorted_imports) + "\n\n# --- End Imports ---\n\n" + "\n".join(body_lines)

    # Apply basic linting/formatting
    formatted_code, warnings = basic_lint_check(final_code)
    return formatted_code, warnings

def basic_lint_check(pyspark_code):
    """Performs basic syntax check and formatting."""
    logger.info("Performing basic lint check and formatting...")
    warnings = []
    formatted_code = pyspark_code # Start with original code

    # 1. Syntax Check using ast
    try:
        ast.parse(formatted_code)
        logger.info("Basic Python syntax check passed (ast.parse).")
    except SyntaxError as e:
        logger.error(f"Syntax error found: {e}")
        warnings.append(f"Syntax Error at line ~{e.lineno}: {e.msg}")
        # Don't attempt further formatting if basic syntax fails
        return formatted_code, warnings
    except Exception as e:
        logger.error(f"Error during AST syntax check: {e}")
        warnings.append(f"AST Check Error: {e}")

    # 2. Basic Formatting (if syntax is ok)
    try:
        # Attempt formatting with black if available
        try:
            import black
            mode = black.Mode(line_length=100) # Configurable line length
            formatted_code = black.format_str(formatted_code, mode=mode)
            logger.info("Applied 'black' formatting.")
        except ImportError:
            logger.warning("Optional formatter 'black' not installed. Skipping black formatting.")
        except Exception as e:
            logger.warning(f"Could not apply 'black' formatting: {e}")

        # Add other simple formatting rules here if needed (e.g., whitespace cleanup)

    except Exception as e:
        logger.error(f"Error during basic formatting: {e}")
        warnings.append(f"Formatting Error: {e}")
        # Return the last valid state of the code if formatting fails
        return pyspark_code, warnings

    return formatted_code, warnings


# --- Annotation Line Number Remapping ---
def remap_annotations(annotations, chunk_abs_start_line, chunk_pyspark_start_line):
    """Adjusts line numbers in annotations to be absolute within the final file."""
    remapped = []
    for ann in annotations:
        try:
            # Adjust SAS lines (relative to chunk -> absolute in original SAS)
            sas_start = ann['sas_lines'][0] + chunk_abs_start_line - 1
            sas_end = ann['sas_lines'][1] + chunk_abs_start_line - 1

            # Adjust PySpark lines (relative to chunk code -> absolute in final PySpark)
            # Note: This assumes chunks are simply concatenated. More complex assembly might need different logic.
            pyspark_start = ann['pyspark_lines'][0] + chunk_pyspark_start_line - 1
            pyspark_end = ann['pyspark_lines'][1] + chunk_pyspark_start_line - 1

            remapped.append({
                **ann, # Copy other fields
                "sas_lines": [sas_start, sas_end],
                "pyspark_lines": [pyspark_start, pyspark_end]
            })
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"Skipping annotation due to remapping error ({e}): {ann}")
    return remapped


# --- Main Lambda Handler ---
def lambda_handler(event, context):
    """
    Main AWS Lambda handler function. Orchestrates SAS to PySpark conversion using Adaptive Strategy.
    """
    start_invocation_time = time.time()
    logger.info("Lambda invocation started.")

    # Reset global stats for this invocation
    global token_usage_stats, llm_call_counts
    token_usage_stats = {'total_tokens': 0, 'input_tokens': 0, 'output_tokens': 0, 'api_calls': 0}
    llm_call_counts = {k: 0 for k in llm_call_counts}

    # Clear cache periodically if enabled
    if USE_LLM_CACHING and token_usage_stats['api_calls'] % 10 == 0: # Heuristic: clear every 10 calls
         manage_cache_size()

    # CORS Headers
    response_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'OPTIONS,POST',
        'Content-Type': 'application/json; charset=utf-8'
    }

    # Handle OPTIONS preflight request
    if event.get('httpMethod') == 'OPTIONS':
        logger.info("Handling OPTIONS preflight request.")
        return {'statusCode': 200, 'headers': response_headers, 'body': json.dumps('OK')}

    # --- Demo Mode Check ---
    if not USE_BEDROCK:
        logger.warning("Running in DEMO mode - Bedrock client not initialized.")
        # Provide a minimal demo response
        demo_pyspark = "# DEMO MODE - Bedrock not available\nprint('This is a demo conversion.')"
        demo_annotations = [{"sas_lines": [1, 1], "pyspark_lines": [1, 1], "note": "Running in demo mode.", "severity": "Info"}]
        return {
            'statusCode': 200,
            'headers': response_headers,
            'body': json.dumps({
                "status": "success",
                "pyspark_code": demo_pyspark,
                "annotations": demo_annotations,
                "warnings": ["Running in demo mode without Bedrock access."],
                "strategy_used": "Demo",
                "processing_stats": {"total_duration_seconds": 0.1, "llm_calls": 0, "token_usage": token_usage_stats, "llm_call_details": llm_call_counts},
                "processing_complete": True
            }, ensure_ascii=False)
        }

    # --- Request Parsing ---
    try:
        logger.info("Parsing request event...")
        # Handle potential API Gateway proxy structure
        if 'body' in event:
            try:
                body = json.loads(event.get('body', '{}'))
            except json.JSONDecodeError:
                 logger.error("Invalid JSON in request body.")
                 return {'statusCode': 400, 'headers': response_headers, 'body': json.dumps({'error': 'Invalid JSON body'})}
        else:
             body = event # Assume direct invocation

        sas_code = body.get('sas_code')
        options = body.get('options', {}) # e.g., {"add_detailed_comments": false}

        if not sas_code:
            logger.error("Missing 'sas_code' in request.")
            return {'statusCode': 400, 'headers': response_headers, 'body': json.dumps({'error': "Request must contain 'sas_code'"})}

        logger.info(f"Received SAS code ({len(sas_code)} chars). Options: {options}")

        # --- Adaptive Strategy Orchestration ---
        warnings = []
        final_pyspark_code = ""
        final_annotations = []
        strategy_used = "Unknown"

        # 1. Assess Complexity
        complexity_thresholds = {
            'SIMPLE_MAX_LINES': SIMPLE_MAX_LINES,
            'SIMPLE_MAX_MACROS': SIMPLE_MAX_MACROS,
            'SIMPLE_MAX_PROCS': SIMPLE_MAX_PROCS
        }
        complexity = assess_complexity(sas_code, complexity_thresholds)
        logger.info(f"Complexity assessed as '{complexity}'.")

        # --- Simple Path ---
        if complexity == "Simple":
            strategy_used = "Simple Direct"
            logger.info("Executing Simple Conversion Path...")
            try:
                pyspark_code, annotations, confidence = call_llm_for_conversion(
                    sas_code,
                    {"conversion_options": options}, # Pass options as context
                    is_simple_path=True
                )
                # Simple path doesn't remap annotations as lines are absolute
                final_annotations.extend(annotations)
                # Format the single block of code
                final_pyspark_code, format_warnings = basic_lint_check(pyspark_code)
                warnings.extend(format_warnings)
                logger.info("Simple Path conversion successful.")
            except Exception as e:
                logger.error(f"Simple Path failed: {e}", exc_info=True)
                warnings.append(f"Simple Path conversion failed: {e}")
                final_pyspark_code = f"# ERROR: Simple Path conversion failed: {e}\n# Original SAS:\n# {sas_code.replace(chr(10), chr(10)+'# ')}"


        # --- Complex Path ---
        else: # complexity == "Complex"
            strategy_used = "Complex Multi-Stage"
            logger.info("Executing Complex Conversion Path...")
            overall_context = {"conversion_options": options} # Initialize context with options
            final_pyspark_pieces = []
            cumulative_sas_line_offset = 0
            cumulative_pyspark_line_offset = 0 # Track PySpark lines for annotation remapping

            try:
                # 2. Split into Super-Chunks
                super_chunks = deterministic_split(sas_code)

                # 3. Process Super-Chunks Sequentially
                for i, super_chunk in enumerate(super_chunks):
                    logger.info(f"Processing Super-Chunk {i+1}/{len(super_chunks)}...")
                    super_chunk_start_line_abs = cumulative_sas_line_offset + 1
                    super_chunk_lines = super_chunk.splitlines()
                    pyspark_outputs_in_sc = {} # Track outputs within this super-chunk

                    # 3a. Structural Analysis (LLM)
                    logical_chunks = call_llm_for_structural_analysis(super_chunk)

                    # 3b. Process Logical Chunks Sequentially
                    for j, logical_chunk in enumerate(logical_chunks):
                        logger.info(f"  Processing Logical Chunk {j+1}/{len(logical_chunks)}: {logical_chunk.get('name')} ({logical_chunk.get('type')})")

                        # Extract code for this logical chunk
                        chunk_start_line_rel = logical_chunk.get('start_line', 1)
                        chunk_end_line_rel = logical_chunk.get('end_line', len(super_chunk_lines))
                        # Adjust for 0-based indexing and slice exclusivity
                        logical_chunk_code = "\n".join(super_chunk_lines[chunk_start_line_rel-1 : chunk_end_line_rel])

                        if not logical_chunk_code.strip():
                            logger.warning(f"  Logical chunk {logical_chunk.get('name')} is empty. Skipping.")
                            continue

                        # Calculate absolute SAS line numbers for this chunk
                        chunk_abs_start_line = super_chunk_start_line_abs + chunk_start_line_rel - 1
                        # chunk_abs_end_line = super_chunk_start_line_abs + chunk_end_line_rel - 1 # Less critical

                        # Prepare context (Simplified: pass overall context; TODO: implement focusing)
                        focused_context = overall_context

                        # 3c. Convert Chunk (LLM)
                        pyspark_code_chunk, annotations_chunk, confidence = call_llm_for_conversion(
                            logical_chunk_code,
                            focused_context,
                            is_simple_path=False,
                            chunk_info=logical_chunk
                        )

                        # 3d. Conditional Refinement (LLM)
                        refinement_attempts = 0
                        current_notes = None
                        while confidence < REFINEMENT_THRESHOLD and refinement_attempts < MAX_REFINEMENT_ATTEMPTS:
                            logger.warning(f"  Confidence ({confidence:.1f}) below threshold ({REFINEMENT_THRESHOLD}). Refining chunk...")
                            refinement_attempts += 1
                            pyspark_code_chunk, confidence, current_notes = call_llm_for_refinement(
                                pyspark_code_chunk,
                                focused_context,
                                previous_notes=current_notes
                            )
                            logger.info(f"  Refinement attempt {refinement_attempts} complete. New confidence: {confidence:.1f}")
                            if current_notes:
                                # Add refinement notes as an annotation
                                annotations_chunk.append({
                                    "sas_lines": [chunk_start_line_rel, chunk_end_line_rel], # Relative SAS lines
                                    "pyspark_lines": [1, len(pyspark_code_chunk.splitlines())], # Relative PySpark lines
                                    "note": f"Refinement Notes (Attempt {refinement_attempts}): {current_notes}",
                                    "severity": "Info"
                                })

                        # Calculate PySpark start line for remapping (based on *current* total lines)
                        current_pyspark_start_line = cumulative_pyspark_line_offset + 1

                        # 3e. Remap Annotation Lines
                        remapped_chunk_annotations = remap_annotations(
                            annotations_chunk,
                            chunk_abs_start_line,
                            current_pyspark_start_line
                        )
                        final_annotations.extend(remapped_chunk_annotations)

                        # 3f. Store result & Update PySpark line offset
                        final_pyspark_pieces.append(pyspark_code_chunk)
                        cumulative_pyspark_line_offset += len(pyspark_code_chunk.splitlines()) + 2 # Account for separators

                        # 3g. Update Overall Context
                        # Pass the *original* logical chunk info and the *final* (potentially refined) code
                        overall_context = update_overall_context(
                            overall_context,
                            logical_chunk,
                            pyspark_code_chunk,
                            annotations_chunk # Pass original annotations before remapping
                        )
                        # Store outputs for potential schema inference later
                        for output_name in logical_chunk.get('outputs', []):
                             pyspark_outputs_in_sc[output_name] = {"code_preview": pyspark_code_chunk[:200]}


                    # Update cumulative SAS line offset for the next super-chunk
                    cumulative_sas_line_offset += len(super_chunk_lines)
                    logger.info(f"Finished processing Super-Chunk {i+1}.")


                # 4. Final Assembly & Formatting
                logger.info("Assembling final code from pieces...")
                final_pyspark_code, format_warnings = assemble_and_format_code(final_pyspark_pieces)
                warnings.extend(format_warnings)
                logger.info("Complex Path conversion successful.")

            except Exception as e:
                logger.error(f"Complex Path failed: {e}", exc_info=True)
                warnings.append(f"Complex Path conversion failed: {e}")
                # Attempt to return partially converted code if available
                if final_pyspark_pieces:
                     partial_code, format_warnings = assemble_and_format_code(final_pyspark_pieces)
                     warnings.extend(format_warnings)
                     final_pyspark_code = f"# WARNING: Complex Path failed, returning partial result.\n# Error: {e}\n\n{partial_code}"
                else:
                     final_pyspark_code = f"# ERROR: Complex Path conversion failed entirely: {e}\n# Original SAS:\n# {sas_code.replace(chr(10), chr(10)+'# ')}"


        # --- Response Preparation ---
        end_invocation_time = time.time()
        processing_stats = {
            "total_duration_seconds": round(end_invocation_time - start_invocation_time, 2),
            "llm_calls": token_usage_stats['api_calls'],
            "token_usage": token_usage_stats,
            "llm_call_details": llm_call_counts, # Include breakdown
            "complexity_assessment": complexity,
            "sas_code_lines": sas_code.count('\n') + 1,
            "generated_pyspark_lines": final_pyspark_code.count('\n') + 1
        }

        logger.info(f"Invocation finished. Duration: {processing_stats['total_duration_seconds']:.2f}s. LLM Calls: {processing_stats['llm_calls']}.")

        # Construct final JSON output
        final_output = {
            "status": "success" if not any(w.startswith("ERROR:") or "failed" in w for w in warnings) else "warning",
            "pyspark_code": final_pyspark_code,
            "annotations": final_annotations,
            "warnings": warnings,
            "strategy_used": strategy_used,
            "processing_stats": processing_stats,
            "processing_complete": True # Indicate completion
        }

        # Return response
        return {
            'statusCode': 200,
            'headers': response_headers,
            'body': json.dumps(final_output, ensure_ascii=False, default=str) # Use default=str for safety
        }

    except Exception as e:
        logger.error(f"Unhandled exception in lambda_handler: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': response_headers,
            'body': json.dumps({
                'status': 'error',
                'message': f"Internal server error: {e}",
                'details': str(e)
            }, ensure_ascii=False)
        }

# --- Test Handler ---
def test_handler(sas_code, options=None):
    """ Utility for testing the lambda function locally or from console """
    test_event = {
        "body": json.dumps({
            "sas_code": sas_code,
            "options": options or {}
        })
    }
    # Mock context object
    class MockContext:
        def get_remaining_time_in_millis(self):
            return 300000 # 5 minutes

    result = lambda_handler(test_event, MockContext())
    # Pretty print the JSON body
    try:
        body_json = json.loads(result['body'])
        print(json.dumps(body_json, indent=2))
    except:
        print(result['body']) # Print raw if not JSON
    return result

# Example usage for local testing (if run directly)
if __name__ == '__main__':
    # Ensure Bedrock client is initialized for local tests if needed
    # Note: Local testing might require AWS credentials configured
    if not USE_BEDROCK:
         print("Warning: Bedrock client not initialized. Running in demo mode.")

    print("Running local test with Simple SAS code...")
    test_sas_simple = """
    DATA work.output;
        SET work.input;
        new_var = old_var * 1.1;
        IF category = 'X' THEN delete;
    RUN;

    PROC SORT DATA=work.output OUT=work.sorted;
        BY id;
    RUN;
    """
    test_handler(test_sas_simple)
import json
import re
import logging
import time
import boto3
import os
import hashlib
from functools import lru_cache
from botocore.config import Config
import ast # For basic syntax check

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Bedrock Client Initialization ---
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
boto_config = Config(read_timeout=3600) # Increased timeout

try:
    bedrock_runtime = boto3.client('bedrock-runtime', region_name=AWS_REGION, config=boto_config)
    USE_BEDROCK = True
    logger.info("Successfully initialized Bedrock client")
except Exception as e:
    logger.warning(f"Failed to initialize Bedrock client: {e}. Will run in demo/test mode only.")
    bedrock_runtime = None
    USE_BEDROCK = False

# --- Configuration ---
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-3-sonnet-20240229-v1:0") # Defaulting to Sonnet 3
ANTHROPIC_VERSION = "bedrock-2023-05-31" # Required for Claude 3 models

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.environ.get("RETRY_DELAY_SECONDS", "2"))
RETRY_BACKOFF_FACTOR = float(os.environ.get("RETRY_BACKOFF_FACTOR", "2.0"))
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "4000")) # Default max tokens for Sonnet 3 response
CONTEXT_SIZE_LIMIT = int(os.environ.get("CONTEXT_SIZE_LIMIT", "150000")) # Approx limit before summarization
REFINEMENT_THRESHOLD = float(os.environ.get("REFINEMENT_THRESHOLD", "3.5")) # Confidence score below which refinement is triggered
MAX_REFINEMENT_ATTEMPTS = int(os.environ.get("MAX_REFINEMENT_ATTEMPTS", "1")) # Limit refinement loops

# Complexity Analyzer Thresholds (Configurable via Env Vars)
SIMPLE_MAX_LINES = int(os.environ.get("SIMPLE_MAX_LINES", "500"))
SIMPLE_MAX_MACROS = int(os.environ.get("SIMPLE_MAX_MACROS", "5"))
SIMPLE_MAX_PROCS = int(os.environ.get("SIMPLE_MAX_PROCS", "10"))
# Optional complexity thresholds (can be added if needed)
# SIMPLE_MAX_MACRO_CALLS = int(os.environ.get("SIMPLE_MAX_MACRO_CALLS", "20"))
# Optional complexity thresholds for more fine-grained control
# COMPLEX_PROC_LIST = os.environ.get("COMPLEX_PROC_LIST", "FCMP,LUA,PROTO,OPTMODEL,IML").split(',')
# CHECK_INCLUDES = os.environ.get("CHECK_INCLUDES", "FALSE").upper() == "TRUE"


# Caching Configuration
USE_LLM_CACHING = os.environ.get("USE_LLM_CACHING", "TRUE").upper() == "TRUE"
MAX_CACHE_ENTRIES = int(os.environ.get("MAX_CACHE_ENTRIES", "100"))
llm_response_cache = {}

# Define error handling retry statuses for Bedrock
BEDROCK_RETRY_EXCEPTIONS = [
    'ThrottlingException',
    'ServiceUnavailableException',
    'InternalServerException',
    'ModelTimeoutException',
    'ModelErrorException', # Added based on potential Bedrock errors
    'ValidationException' # Added for input validation issues
]

# Initialize global statistics
token_usage_stats = {
    'total_tokens': 0,
    'input_tokens': 0,
    'output_tokens': 0,
    'api_calls': 0
}
llm_call_counts = {
    'complexity_analysis': 0,
    'simple_conversion': 0,
    'structural_analysis': 0,
    'context_focus': 0, # Not implemented in this version, placeholder
    'chunk_conversion': 0,
    'refinement': 0,
    'summarization': 0
}

# --- Caching ---
def cache_key(prompt, model_id=None):
    """Generate a cache key from the prompt and optional model ID"""
    key_content = f"{prompt}:{model_id or BEDROCK_MODEL_ID}"
    return hashlib.md5(key_content.encode('utf-8')).hexdigest()

def manage_cache_size():
    """Removes oldest entries if cache exceeds max size."""
    if len(llm_response_cache) > MAX_CACHE_ENTRIES:
        # Simple FIFO eviction
        num_to_remove = len(llm_response_cache) - MAX_CACHE_ENTRIES
        keys_to_remove = list(llm_response_cache.keys())[:num_to_remove]
        for key in keys_to_remove:
            del llm_response_cache[key]
        logger.info(f"Cache full. Removed {num_to_remove} oldest entries.")

# --- Bedrock LLM Interaction ---
def _invoke_claude3(prompt, max_tokens=MAX_TOKENS):
    """
    Invokes a Claude 3 model on Bedrock (handles Sonnet/Haiku).
    Handles the specific request/response format for Anthropic models via Bedrock.
    Updates global token usage stats.
    """
    if not USE_BEDROCK or bedrock_runtime is None:
        logger.warning("Bedrock is not available. Returning demo response.")
        # Return a structured demo response matching expected output formats
        return """{"pyspark_code": "# Demo mode - Bedrock not available\nprint('Demo conversion')",
                   "annotations": [{"sas_lines": [1, 1], "pyspark_lines": [1, 1], "note": "Demo mode", "severity": "Info"}],
                   "confidence_score": 5, "refinement_notes": ""}""" # Include fields for all call types

    messages = [{"role": "user", "content": prompt}]
    body = {
        "anthropic_version": ANTHROPIC_VERSION,
        "max_tokens": max_tokens,
        "messages": messages,
        "temperature": float(os.environ.get("LLM_TEMPERATURE", "0.1")), # Lower temp for more deterministic output
        "top_p": float(os.environ.get("LLM_TOP_P", "0.9")),
    }

    try:
        logger.info(f"Invoking {BEDROCK_MODEL_ID} with max_tokens={max_tokens}")
        start_time = time.time()
        response = bedrock_runtime.invoke_model(
            body=json.dumps(body),
            modelId=BEDROCK_MODEL_ID,
            accept='application/json',
            contentType='application/json'
        )
        response_time = time.time() - start_time
        response_body = json.loads(response['body'].read())

        # Track token usage
        input_tokens = response_body.get("usage", {}).get("input_tokens", 0)
        output_tokens = response_body.get("usage", {}).get("output_tokens", 0)
        total_tokens = input_tokens + output_tokens

        logger.info(f"Token usage: {input_tokens} input + {output_tokens} output = {total_tokens} total tokens")
        logger.info(f"Response time: {response_time:.2f}s")

        # Update global stats
        token_usage_stats['total_tokens'] += total_tokens
        token_usage_stats['input_tokens'] += input_tokens
        token_usage_stats['output_tokens'] += output_tokens
        token_usage_stats['api_calls'] += 1

        # Extract content
        if response_body.get("type") == "message" and response_body.get("content"):
            llm_output = response_body["content"][0]["text"]
            return llm_output
        elif response_body.get("error"):
             error_type = response_body["error"].get("type")
             error_message = response_body["error"].get("message")
             logger.error(f"Bedrock API Error: {error_type} - {error_message}")
             # Raise specific exception types if needed for retry logic
             if error_type == 'overloaded_error':
                 raise Exception("Model overloaded") # Or a custom exception
             else:
                 raise Exception(f"Bedrock Error: {error_type} - {error_message}")
        else:
            logger.error(f"Unexpected Bedrock response format: {response_body}")
            raise ValueError("Unexpected response format from Bedrock Claude 3")

    except Exception as e:
        logger.error(f"Bedrock invoke_model failed: {e}", exc_info=True)
        raise # Re-raise to be caught by retry logic

def _call_llm_with_retry(invoke_function, prompt, description="LLM call", llm_call_type_key=None, bypass_cache=False, max_tokens=MAX_TOKENS):
    """
    Wrapper for retry logic around Bedrock invoke_model calls. Includes caching and exponential backoff.
    """
    # Check cache first
    cache_key_val = cache_key(prompt)
    if USE_LLM_CACHING and not bypass_cache and cache_key_val in llm_response_cache:
        logger.info(f"Cache hit for {description}! Using cached response.")
        return llm_response_cache[cache_key_val]

    # Proceed with API call
    last_exception = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            logger.info(f"Attempting {description} (Attempt {attempt + 1}/{MAX_RETRIES + 1})...")
            result = invoke_function(prompt, max_tokens=max_tokens) # Pass max_tokens here

            # Store in cache
            if USE_LLM_CACHING:
                llm_response_cache[cache_key_val] = result
                manage_cache_size() # Check and manage cache size after adding

            # Increment specific LLM call counter if key provided
            if llm_call_type_key and llm_call_type_key in llm_call_counts:
                llm_call_counts[llm_call_type_key] += 1

            return result # Success

        except Exception as e:
            last_exception = e
            error_name = type(e).__name__
            error_str = str(e)
            logger.warning(f"{description} attempt {attempt + 1} failed: {error_name} - {error_str}")

            # Check if retryable
            is_retryable = any(retry_err in error_name for retry_err in BEDROCK_RETRY_EXCEPTIONS) or \
                           any(retry_err in error_str for retry_err in BEDROCK_RETRY_EXCEPTIONS)

            if attempt < MAX_RETRIES and is_retryable:
                delay = RETRY_DELAY_SECONDS * (RETRY_BACKOFF_FACTOR ** attempt)
                logger.info(f"Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"{description} failed after {attempt + 1} attempts.")
                break # Exit loop after final attempt or non-retryable error

    # If loop finished without success, raise the last exception
    raise last_exception or Exception(f"{description} failed after {MAX_RETRIES + 1} attempts.")


# --- Complexity Analysis ---
def assess_complexity(sas_code, thresholds):
    """
    Analyzes SAS code complexity based on heuristics to determine the conversion path.

    Args:
        sas_code (str): The SAS code.
        thresholds (dict): Dictionary containing complexity thresholds like
                           {'SIMPLE_MAX_LINES': 500, 'SIMPLE_MAX_MACROS': 5, ...}.

    Returns:
        str: "Simple" or "Complex".
    """
    logger.info("Assessing SAS code complexity using heuristics...")
    loc = sas_code.count('\n') + 1
    # char_count = len(sas_code) # Less reliable metric on its own

    # Core metrics
    macro_defs = len(re.findall(r'^\s*%MACRO\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))
    proc_calls = len(re.findall(r'^\s*PROC\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))

    # Optional advanced metrics (can be enabled via config/thresholds)
    # macro_calls_approx = len(re.findall(r'%\w+\s*\(', sas_code, re.IGNORECASE)) # Approximate macro calls
    # complex_procs_found = []
    # if 'COMPLEX_PROC_LIST' in thresholds:
    #     complex_proc_pattern = r'^\s*PROC\s+(' + '|'.join(thresholds['COMPLEX_PROC_LIST']) + r')\b'
    #     complex_procs_found = re.findall(complex_proc_pattern, sas_code, re.IGNORECASE | re.MULTILINE)
    # has_includes = False
    # if thresholds.get('CHECK_INCLUDES', False):
    #     has_includes = bool(re.search(r'^\s*%INCLUDE\b', sas_code, re.IGNORECASE | re.MULTILINE))

    # Classification Rule
    is_simple = (
        loc <= thresholds.get('SIMPLE_MAX_LINES', 500) and
        macro_defs <= thresholds.get('SIMPLE_MAX_MACROS', 5) and
        proc_calls <= thresholds.get('SIMPLE_MAX_PROCS', 10) #and
        # Optional checks:
        # macro_calls_approx <= thresholds.get('SIMPLE_MAX_MACRO_CALLS', 20) and
        # not complex_procs_found and
        # not has_includes
    )

    complexity = "Simple" if is_simple else "Complex"
    logger.info(f"Complexity assessed as: {complexity} (LoC: {loc}, Macro Defs: {macro_defs}, PROC Calls: {proc_calls})")
    # logger.debug(f"Optional metrics - Approx Macro Calls: {macro_calls_approx}, Complex PROCs: {complex_procs_found}, Has Includes: {has_includes}")
    return complexity

# --- Code Splitting ---
def deterministic_split(sas_code, max_chunk_size=50000):
    """
    Splits SAS code into super-chunks based on PROC/DATA/MACRO boundaries.
    (Reusing the existing robust implementation)
    """
    logger.info("Performing deterministic splitting into super-chunks...")
    if len(sas_code) < 1000: # Optimization for small code
        logger.info("Code is small, treating as a single chunk.")
        return [sas_code]

    # Regex to find major boundaries (start of line, case-insensitive)
    boundary_pattern = re.compile(r'^\s*(PROC\s+\w+|DATA\s+[\w\.]+|%MACRO\s+[\w\.]+)\b.*?;', re.IGNORECASE | re.MULTILINE | re.DOTALL)
    boundaries = list(boundary_pattern.finditer(sas_code))

    if not boundaries:
        logger.warning("No clear PROC/DATA/MACRO boundaries found. Treating as one chunk.")
        return [sas_code]

    logger.info(f"Found {len(boundaries)} potential split points.")
    super_chunks = []
    start_index = 0
    current_chunk_start_index = 0

    for i, match in enumerate(boundaries):
        split_point = match.start()

        # If this isn't the first boundary and the current chunk exceeds max size, split before this boundary
        if i > 0 and (split_point - current_chunk_start_index > max_chunk_size):
            chunk = sas_code[current_chunk_start_index:start_index] # Split at the *previous* boundary
            if chunk.strip():
                super_chunks.append(chunk)
                logger.info(f"Created super-chunk (size: {len(chunk)} chars)")
                current_chunk_start_index = start_index # Start next chunk from previous boundary

        # Update the potential end of the current logical block
        start_index = match.start()

    # Add the final chunk (from the last boundary start to the end)
    final_chunk = sas_code[current_chunk_start_index:]
    if final_chunk.strip():
         # If the final chunk itself is too large, try an emergency split (e.g., by lines)
        if len(final_chunk) > max_chunk_size * 1.2: # Allow some overshoot
             logger.warning(f"Final chunk is very large ({len(final_chunk)} chars). Performing emergency line split.")
             lines = final_chunk.splitlines()
             temp_chunk = []
             current_len = 0
             for line in lines:
                 line_len = len(line) + 1 # Account for newline
                 if current_len + line_len > max_chunk_size and temp_chunk:
                     super_chunks.append("\n".join(temp_chunk))
                     logger.info(f"Created super-chunk (emergency split, size: {current_len} chars)")
                     temp_chunk = [line]
                     current_len = line_len
                 else:
                     temp_chunk.append(line)
                     current_len += line_len
             if temp_chunk: # Add remaining lines
                 super_chunks.append("\n".join(temp_chunk))
                 logger.info(f"Created super-chunk (emergency split, size: {current_len} chars)")
        else:
            super_chunks.append(final_chunk)
            logger.info(f"Created final super-chunk (size: {len(final_chunk)} chars)")


    logger.info(f"Split SAS code into {len(super_chunks)} super-chunks.")
    return [chunk for chunk in super_chunks if chunk.strip()]


# --- Structural Analysis (Complex Path) ---
def call_llm_for_structural_analysis(super_chunk):
    """Calls LLM to identify logical chunks within a super-chunk."""
    logger.info("Calling LLM for structural analysis...")
    # Optimization: Skip LLM for very small super-chunks
    if len(super_chunk.splitlines()) < 10: # Heuristic threshold
        logger.info("Super-chunk is small, creating a single default logical chunk.")
        return [{
            "type": "UNKNOWN", "name": "small_chunk",
            "start_line": 1, "end_line": len(super_chunk.splitlines()),
            "inputs": [], "outputs": []
        }]

    prompt = f"""Analyze the following SAS code block. Identify all distinct logical units (DATA steps, PROC steps, defined %MACROs).
Output ONLY a valid JSON list where each item represents a unit and includes:
{{
  "type": "DATA" | "PROC" | "MACRO",
  "name": "step/proc/macro_name", // Best guess for the name
  "start_line": <integer>, // Starting line number (1-based) within this super_chunk
  "end_line": <integer>, // Ending line number (1-based) within this super_chunk
  "inputs": ["dataset_or_macro_used"], // List of likely input dataset/macro names used
  "outputs": ["dataset_or_macro_created"] // List of likely output dataset/macro names created
}}
Ensure the output is ONLY the JSON list, starting with '[' and ending with ']'. Be precise with line numbers.

SAS Code Block:
```sas
{super_chunk}
```"""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Structural Analysis", llm_call_type_key='structural_analysis')
    try:
        # Clean potential markdown fences
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        logical_chunks = json.loads(cleaned_response)
        if isinstance(logical_chunks, list):
            # Basic validation
            for chunk in logical_chunks:
                if not all(k in chunk for k in ["type", "name", "start_line", "end_line", "inputs", "outputs"]):
                    raise ValueError("Invalid chunk structure in structural analysis response.")
            logger.info(f"LLM identified {len(logical_chunks)} logical chunks.")
            # Handle empty list case
            if not logical_chunks:
                 logger.warning("LLM returned empty chunk list, creating default chunk for the super-chunk.")
                 return [{
                    "type": "UNKNOWN", "name": "default_chunk",
                    "start_line": 1, "end_line": len(super_chunk.splitlines()),
                    "inputs": [], "outputs": []
                 }]
            return logical_chunks
        else:
            raise ValueError("LLM response for structural analysis was not a JSON list.")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON for structural analysis: {e}\nResponse: {response_text}")
        # Fallback: treat the whole super-chunk as one logical chunk
        return [{"type": "UNKNOWN", "name": "fallback_chunk", "start_line": 1, "end_line": len(super_chunk.splitlines()), "inputs": [], "outputs": []}]
    except Exception as e:
        logger.error(f"Error processing structural analysis: {e}\nResponse: {response_text}")
        raise # Re-raise other errors


# --- Context Management (Complex Path) ---
# Context Focusing - Placeholder for future optimization
# def call_llm_for_context_focus(overall_context, logical_chunk_code): ...

def update_overall_context(overall_context, logical_chunk, pyspark_output, annotations):
    """
    Updates the overall context with outputs from a processed logical chunk.
    Focuses on schema and macro definitions.
    """
    logger.debug(f"Updating overall context after processing chunk: {logical_chunk.get('name')}")
    if "schema_info" not in overall_context: overall_context["schema_info"] = {}
    if "macros_defined" not in overall_context: overall_context["macros_defined"] = {}
    if "processed_chunk_outputs" not in overall_context: overall_context["processed_chunk_outputs"] = {}

    chunk_type = logical_chunk.get('type', '').upper()
    chunk_name = logical_chunk.get('name', 'unnamed')

    # Store outputs (datasets/macros created)
    outputs = logical_chunk.get('outputs', [])
    overall_context["processed_chunk_outputs"][chunk_name] = outputs

    for output_name in outputs:
        if chunk_type == 'MACRO':
            # Basic macro definition tracking
            overall_context["macros_defined"][output_name] = {
                "status": "defined",
                "source_chunk": chunk_name,
                "parameters": [], # TODO: Could try to extract params from code/annotations
                "body_preview": pyspark_output[:200] # Store a preview
            }
            logger.debug(f"Added/Updated macro definition: {output_name}")
        elif chunk_type in ['DATA', 'PROC']:
            # Basic schema tracking - could be enhanced by LLM extracting schema from code/annotations
            # For now, just mark the dataset as created
            overall_context["schema_info"][output_name] = {
                "status": "created/modified",
                "source_chunk": chunk_name,
                "derived_from": logical_chunk.get('inputs', []),
                "columns": ["unknown"] # Placeholder - needs schema extraction logic
            }
            logger.debug(f"Added/Updated schema info for dataset: {output_name}")

    # Check context size and summarize if needed
    try:
        current_context_size = len(json.dumps(overall_context))
        logger.debug(f"Current estimated context size: {current_context_size} chars.")
        if current_context_size > CONTEXT_SIZE_LIMIT:
            logger.warning(f"Context size ({current_context_size}) exceeds limit ({CONTEXT_SIZE_LIMIT}). Attempting summarization...")
            overall_context = call_llm_for_summarization(overall_context)
            new_size = len(json.dumps(overall_context))
            logger.info(f"Context size after summarization: {new_size} chars.")
    except TypeError as e:
        logger.warning(f"Could not estimate context size or summarize due to content: {e}")
    except Exception as e:
         logger.error(f"Error during context size check/summarization: {e}")


    return overall_context

def call_llm_for_summarization(overall_context):
    """Calls LLM to summarize the overall context if it gets too large."""
    logger.info("Calling LLM for context summarization...")
    try:
        context_to_summarize_json = json.dumps(overall_context, indent=2)
    except TypeError:
         logger.error("Cannot serialize context for summarization. Skipping.")
         return overall_context # Return original if cannot serialize

    prompt = f"""Summarize the key information in the provided context JSON concisely, preserving essential details for downstream SAS to PySpark conversion.
Focus on:
1. Dataset schemas (column names, types if available) for the *most recently* created/modified datasets.
2. All defined macros and their parameters/previews.
3. Key dataset lineage (outputs of recent chunks).

Output ONLY a valid JSON object with the same structure as the input, but summarized. Remove older schema entries or less critical details if necessary to reduce size.

Context JSON to Summarize:
```json
{context_to_summarize_json}
```

Output ONLY the summarized JSON object."""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Context Summarization", llm_call_type_key='summarization')
    try:
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        summarized_context = json.loads(cleaned_response)
        if isinstance(summarized_context, dict):
            logger.info("Context summarization successful.")
            return summarized_context
        else:
            raise ValueError("LLM response for summarization was not a JSON object.")
    except Exception as e:
        logger.error(f"Failed to process summarization response: {e}. Returning original context.")
        return overall_context # Fallback to original context on error


# --- Code Conversion (Simple and Complex Paths) ---
def call_llm_for_conversion(code_to_convert, context, is_simple_path=False, chunk_info=None):
    """
    Calls LLM to convert SAS code (either full script or a chunk) to PySpark.
    Returns pyspark_code, annotations, and confidence score.
    """
    logger.info(f"Calling LLM for conversion ({'Simple Path' if is_simple_path else 'Chunk: '+chunk_info.get('name', 'N/A')})...")
    try:
        context_json = json.dumps(context, indent=2, default=str) # Use default=str for non-serializable items
    except TypeError as e:
        logger.warning(f"Could not serialize context for conversion: {e}. Using empty context.")
        context_json = "{}"

    path_specific_instructions = ""
    if is_simple_path:
        path_specific_instructions = "You are converting the entire SAS script in one go."
        llm_call_key = 'simple_conversion'
    else:
        path_specific_instructions = f"You are converting a logical chunk identified as '{chunk_info.get('name', 'N/A')}' (type: {chunk_info.get('type', 'N/A')}). Focus on this chunk, using the provided context for dependencies."
        llm_call_key = 'chunk_conversion'

    prompt = f"""Convert the following SAS code {'snippet' if not is_simple_path else 'script'} to equivalent PySpark code.

{path_specific_instructions}

Context (use this for dependencies like schemas, macros):
```json
{context_json}
```

SAS Code to Convert:
```sas
{code_to_convert}
```

Conversion Guidelines:
- Maintain semantic equivalence.
- Use idiomatic PySpark (DataFrame API).
- Include necessary imports (placeholders are fine, they will be consolidated later).
- Add comments explaining the conversion logic, especially for complex parts or assumptions. Reference original SAS line numbers if possible.
- Handle SAS-specific features (e.g., macro variables, data step logic, PROCs) appropriately.
- If a direct conversion isn't possible, implement the closest equivalent and add a 'Warning' annotation.
- Assess your confidence in the conversion quality for this specific piece of code.

Output ONLY a valid JSON object with the following structure:
{{
  "pyspark_code": "...",
  "annotations": [
    {{
      "sas_lines": [<start_line_int>, <end_line_int>], // Relative to the input SAS code snippet/script
      "pyspark_lines": [<start_line_int>, <end_line_int>], // Relative to the generated PySpark code
      "note": "Explanation...",
      "severity": "Info" | "Warning"
    }}
  ],
  "confidence_score": <float 1.0-5.0> // Your confidence in this specific conversion's accuracy
}}
Ensure the output is ONLY the JSON object.
"""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Conversion", llm_call_type_key=llm_call_key)
    try:
        # Clean potential markdown fences and control characters
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        cleaned_response = ''.join(c for c in cleaned_response if c.isprintable() or c in '\n\r\t') # Keep whitespace

        result = json.loads(cleaned_response)

        # Validate structure
        if isinstance(result, dict) and \
           all(k in result for k in ["pyspark_code", "annotations", "confidence_score"]) and \
           isinstance(result["pyspark_code"], str) and \
           isinstance(result["annotations"], list) and \
           isinstance(result["confidence_score"], (int, float)):

            # Basic validation of annotations
            valid_annotations = []
            for ann in result["annotations"]:
                if isinstance(ann, dict) and all(k in ann for k in ["sas_lines", "pyspark_lines", "note", "severity"]):
                    valid_annotations.append(ann)
                else:
                     logger.warning(f"Skipping invalid annotation structure: {ann}")
            result["annotations"] = valid_annotations

            logger.info(f"LLM conversion successful. Confidence: {result['confidence_score']:.1f}/5.0")
            return result["pyspark_code"], result["annotations"], float(result["confidence_score"])
        else:
            raise ValueError("LLM response for conversion has invalid structure.")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON for conversion: {e}\nResponse: {response_text}")
        # Fallback: return error code, annotation, and low confidence
        error_code = f"# ERROR: Failed to parse LLM conversion response.\n# Original SAS:\n# {code_to_convert.replace(chr(10), chr(10)+'# ')}"
        error_ann = [{'sas_lines': [1,1], 'pyspark_lines': [1,1], 'note': f'LLM response was not valid JSON: {e}', 'severity': 'Warning'}]
        return error_code, error_ann, 1.0 # Low confidence
    except Exception as e:
        logger.error(f"Error processing conversion response: {e}\nResponse: {response_text}")
        error_code = f"# ERROR: Exception processing conversion response: {e}\n# Original SAS:\n# {code_to_convert.replace(chr(10), chr(10)+'# ')}"
        error_ann = [{'sas_lines': [1,1], 'pyspark_lines': [1,1], 'note': f'Exception during conversion processing: {e}', 'severity': 'Warning'}]
        return error_code, error_ann, 1.0 # Low confidence


# --- Code Refinement (Complex Path) ---
def call_llm_for_refinement(pyspark_section, context, previous_notes=None):
    """Calls LLM to refine a PySpark code section and assess confidence."""
    logger.info("Calling LLM for refinement...")
    try:
        context_json = json.dumps(context, indent=2, default=str)
    except TypeError:
        context_json = "{}" # Send empty if serialization fails

    refinement_focus = "Focus on correctness against SAS semantics, Spark efficiency, PEP 8 style, and clarity."
    if previous_notes:
        refinement_focus = f"Confidence was low previously. Focus on addressing these notes:\n'''\n{previous_notes}\n'''\nAlso perform a general review."

    prompt = f"""Review and refine the following PySpark code section.
{refinement_focus}

Context (for reference):
```json
{context_json}
```

PySpark Code Section to Refine:
```python
{pyspark_section}
```

Output ONLY a valid JSON object with:
{{
  "refined_code": "...", // The improved PySpark code
  "confidence_score": <float 1.0-5.0>, // Updated confidence score
  "refinement_notes": "..." // Any *new* or *remaining* issues or suggestions
}}
Ensure the output is ONLY the JSON object.
"""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Refinement", llm_call_type_key='refinement')
    try:
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        cleaned_response = ''.join(c for c in cleaned_response if c.isprintable() or c in '\n\r\t')

        result = json.loads(cleaned_response)

        # Validate structure
        if isinstance(result, dict) and \
           all(k in result for k in ["refined_code", "confidence_score", "refinement_notes"]) and \
           isinstance(result["refined_code"], str) and \
           isinstance(result["confidence_score"], (int, float)) and \
           isinstance(result["refinement_notes"], str):
            logger.info(f"LLM refinement successful. New Confidence: {result['confidence_score']:.1f}/5.0")
            return result["refined_code"], float(result["confidence_score"]), result["refinement_notes"]
        else:
            raise ValueError("LLM response for refinement has invalid structure.")
    except Exception as e:
        logger.error(f"Failed to process refinement response: {e}. Returning original code and low confidence.")
        # Fallback: return original code, low confidence, error note
        return pyspark_section, 1.0, f"Error processing refinement response: {e}"


# --- Code Assembly & Formatting ---
def assemble_and_format_code(pyspark_pieces):
    """Joins PySpark code pieces, moves imports, and formats."""
    logger.info("Assembling and formatting final PySpark code...")
    full_code = "\n\n# --- Autogenerated Chunk Separator ---\n\n".join(pyspark_pieces)

    # Extract and consolidate imports
    imports = set()
    body_lines = []
    import_pattern = re.compile(r'^\s*(import\s+[\w\.,\s]+|from\s+\w+(\.\w+)*\s+import\s+[\w\*\.,\s\(\)]+)')

    for line in full_code.splitlines():
        match = import_pattern.match(line)
        if match:
            # Normalize and add unique imports
            import_statement = match.group(0).strip()
            imports.add(import_statement)
        else:
            body_lines.append(line)

    # Sort imports (basic alphabetical sort)
    sorted_imports = sorted(list(imports))
    final_code = "\n".join(sorted_imports) + "\n\n# --- End Imports ---\n\n" + "\n".join(body_lines)

    # Apply basic linting/formatting
    formatted_code, warnings = basic_lint_check(final_code)
    return formatted_code, warnings

def basic_lint_check(pyspark_code):
    """Performs basic syntax check and formatting."""
    logger.info("Performing basic lint check and formatting...")
    warnings = []
    formatted_code = pyspark_code # Start with original code

    # 1. Syntax Check using ast
    try:
        ast.parse(formatted_code)
        logger.info("Basic Python syntax check passed (ast.parse).")
    except SyntaxError as e:
        logger.error(f"Syntax error found: {e}")
        warnings.append(f"Syntax Error at line ~{e.lineno}: {e.msg}")
        # Don't attempt further formatting if basic syntax fails
        return formatted_code, warnings
    except Exception as e:
        logger.error(f"Error during AST syntax check: {e}")
        warnings.append(f"AST Check Error: {e}")

    # 2. Basic Formatting (if syntax is ok)
    try:
        # Attempt formatting with black if available
        try:
            import black
            mode = black.Mode(line_length=100) # Configurable line length
            formatted_code = black.format_str(formatted_code, mode=mode)
            logger.info("Applied 'black' formatting.")
        except ImportError:
            logger.warning("Optional formatter 'black' not installed. Skipping black formatting.")
        except Exception as e:
            logger.warning(f"Could not apply 'black' formatting: {e}")

        # Add other simple formatting rules here if needed (e.g., whitespace cleanup)

    except Exception as e:
        logger.error(f"Error during basic formatting: {e}")
        warnings.append(f"Formatting Error: {e}")
        # Return the last valid state of the code if formatting fails
        return pyspark_code, warnings

    return formatted_code, warnings


# --- Annotation Line Number Remapping ---
def remap_annotations(annotations, chunk_abs_start_line, chunk_pyspark_start_line):
    """Adjusts line numbers in annotations to be absolute within the final file."""
    remapped = []
    for ann in annotations:
        try:
            # Adjust SAS lines (relative to chunk -> absolute in original SAS)
            sas_start = ann['sas_lines'][0] + chunk_abs_start_line - 1
            sas_end = ann['sas_lines'][1] + chunk_abs_start_line - 1

            # Adjust PySpark lines (relative to chunk code -> absolute in final PySpark)
            # Note: This assumes chunks are simply concatenated. More complex assembly might need different logic.
            pyspark_start = ann['pyspark_lines'][0] + chunk_pyspark_start_line - 1
            pyspark_end = ann['pyspark_lines'][1] + chunk_pyspark_start_line - 1

            remapped.append({
                **ann, # Copy other fields
                "sas_lines": [sas_start, sas_end],
                "pyspark_lines": [pyspark_start, pyspark_end]
            })
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"Skipping annotation due to remapping error ({e}): {ann}")
    return remapped


# --- Main Lambda Handler ---
def lambda_handler(event, context):
    """
    Main AWS Lambda handler function. Orchestrates SAS to PySpark conversion using Adaptive Strategy.
    """
    start_invocation_time = time.time()
    logger.info("Lambda invocation started.")

    # Reset global stats for this invocation
    global token_usage_stats, llm_call_counts
    token_usage_stats = {'total_tokens': 0, 'input_tokens': 0, 'output_tokens': 0, 'api_calls': 0}
    llm_call_counts = {k: 0 for k in llm_call_counts}

    # Clear cache periodically if enabled
    if USE_LLM_CACHING and token_usage_stats['api_calls'] % 10 == 0: # Heuristic: clear every 10 calls
         manage_cache_size()

    # CORS Headers
    response_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'OPTIONS,POST',
        'Content-Type': 'application/json; charset=utf-8'
    }

    # Handle OPTIONS preflight request
    if event.get('httpMethod') == 'OPTIONS':
        logger.info("Handling OPTIONS preflight request.")
        return {'statusCode': 200, 'headers': response_headers, 'body': json.dumps('OK')}

    # --- Demo Mode Check ---
    if not USE_BEDROCK:
        logger.warning("Running in DEMO mode - Bedrock client not initialized.")
        # Provide a minimal demo response
        demo_pyspark = "# DEMO MODE - Bedrock not available\nprint('This is a demo conversion.')"
        demo_annotations = [{"sas_lines": [1, 1], "pyspark_lines": [1, 1], "note": "Running in demo mode.", "severity": "Info"}]
        return {
            'statusCode': 200,
            'headers': response_headers,
            'body': json.dumps({
                "status": "success",
                "pyspark_code": demo_pyspark,
                "annotations": demo_annotations,
                "warnings": ["Running in demo mode without Bedrock access."],
                "strategy_used": "Demo",
                "processing_stats": {"total_duration_seconds": 0.1, "llm_calls": 0, "token_usage": token_usage_stats, "llm_call_details": llm_call_counts},
                "processing_complete": True
            }, ensure_ascii=False)
        }

    # --- Request Parsing ---
    try:
        logger.info("Parsing request event...")
        # Handle potential API Gateway proxy structure
        if 'body' in event:
            try:
                body = json.loads(event.get('body', '{}'))
            except json.JSONDecodeError:
                 logger.error("Invalid JSON in request body.")
                 return {'statusCode': 400, 'headers': response_headers, 'body': json.dumps({'error': 'Invalid JSON body'})}
        else:
             body = event # Assume direct invocation

        sas_code = body.get('sas_code')
        options = body.get('options', {}) # e.g., {"add_detailed_comments": false}

        if not sas_code:
            logger.error("Missing 'sas_code' in request.")
            return {'statusCode': 400, 'headers': response_headers, 'body': json.dumps({'error': "Request must contain 'sas_code'"})}

        logger.info(f"Received SAS code ({len(sas_code)} chars). Options: {options}")

        # --- Adaptive Strategy Orchestration ---
        warnings = []
        final_pyspark_code = ""
        final_annotations = []
        strategy_used = "Unknown"

        # 1. Assess Complexity
        complexity_thresholds = {
            'SIMPLE_MAX_LINES': SIMPLE_MAX_LINES,
            'SIMPLE_MAX_MACROS': SIMPLE_MAX_MACROS,
            'SIMPLE_MAX_PROCS': SIMPLE_MAX_PROCS
        }
        complexity = assess_complexity(sas_code, complexity_thresholds)
        logger.info(f"Complexity assessed as '{complexity}'.")

        # --- Simple Path ---
        if complexity == "Simple":
            strategy_used = "Simple Direct"
            logger.info("Executing Simple Conversion Path...")
            try:
                pyspark_code, annotations, confidence = call_llm_for_conversion(
                    sas_code,
                    {"conversion_options": options}, # Pass options as context
                    is_simple_path=True
                )
                # Simple path doesn't remap annotations as lines are absolute
                final_annotations.extend(annotations)
                # Format the single block of code
                final_pyspark_code, format_warnings = basic_lint_check(pyspark_code)
                warnings.extend(format_warnings)
                logger.info("Simple Path conversion successful.")
            except Exception as e:
                logger.error(f"Simple Path failed: {e}", exc_info=True)
                warnings.append(f"Simple Path conversion failed: {e}")
                final_pyspark_code = f"# ERROR: Simple Path conversion failed: {e}\n# Original SAS:\n# {sas_code.replace(chr(10), chr(10)+'# ')}"


        # --- Complex Path ---
        else: # complexity == "Complex"
            strategy_used = "Complex Multi-Stage"
            logger.info("Executing Complex Conversion Path...")
            overall_context = {"conversion_options": options} # Initialize context with options
            final_pyspark_pieces = []
            cumulative_sas_line_offset = 0
            cumulative_pyspark_line_offset = 0 # Track PySpark lines for annotation remapping

            try:
                # 2. Split into Super-Chunks
                super_chunks = deterministic_split(sas_code)

                # 3. Process Super-Chunks Sequentially
                for i, super_chunk in enumerate(super_chunks):
                    logger.info(f"Processing Super-Chunk {i+1}/{len(super_chunks)}...")
                    super_chunk_start_line_abs = cumulative_sas_line_offset + 1
                    super_chunk_lines = super_chunk.splitlines()
                    pyspark_outputs_in_sc = {} # Track outputs within this super-chunk

                    # 3a. Structural Analysis (LLM)
                    logical_chunks = call_llm_for_structural_analysis(super_chunk)

                    # 3b. Process Logical Chunks Sequentially
                    for j, logical_chunk in enumerate(logical_chunks):
                        logger.info(f"  Processing Logical Chunk {j+1}/{len(logical_chunks)}: {logical_chunk.get('name')} ({logical_chunk.get('type')})")

                        # Extract code for this logical chunk
                        chunk_start_line_rel = logical_chunk.get('start_line', 1)
                        chunk_end_line_rel = logical_chunk.get('end_line', len(super_chunk_lines))
                        # Adjust for 0-based indexing and slice exclusivity
                        logical_chunk_code = "\n".join(super_chunk_lines[chunk_start_line_rel-1 : chunk_end_line_rel])

                        if not logical_chunk_code.strip():
                            logger.warning(f"  Logical chunk {logical_chunk.get('name')} is empty. Skipping.")
                            continue

                        # Calculate absolute SAS line numbers for this chunk
                        chunk_abs_start_line = super_chunk_start_line_abs + chunk_start_line_rel - 1
                        # chunk_abs_end_line = super_chunk_start_line_abs + chunk_end_line_rel - 1 # Less critical

                        # Prepare context (Simplified: pass overall context; TODO: implement focusing)
                        focused_context = overall_context

                        # 3c. Convert Chunk (LLM)
                        pyspark_code_chunk, annotations_chunk, confidence = call_llm_for_conversion(
                            logical_chunk_code,
                            focused_context,
                            is_simple_path=False,
                            chunk_info=logical_chunk
                        )

                        # 3d. Conditional Refinement (LLM)
                        refinement_attempts = 0
                        current_notes = None
                        while confidence < REFINEMENT_THRESHOLD and refinement_attempts < MAX_REFINEMENT_ATTEMPTS:
                            logger.warning(f"  Confidence ({confidence:.1f}) below threshold ({REFINEMENT_THRESHOLD}). Refining chunk...")
                            refinement_attempts += 1
                            pyspark_code_chunk, confidence, current_notes = call_llm_for_refinement(
                                pyspark_code_chunk,
                                focused_context,
                                previous_notes=current_notes
                            )
                            logger.info(f"  Refinement attempt {refinement_attempts} complete. New confidence: {confidence:.1f}")
                            if current_notes:
                                # Add refinement notes as an annotation
                                annotations_chunk.append({
                                    "sas_lines": [chunk_start_line_rel, chunk_end_line_rel], # Relative SAS lines
                                    "pyspark_lines": [1, len(pyspark_code_chunk.splitlines())], # Relative PySpark lines
                                    "note": f"Refinement Notes (Attempt {refinement_attempts}): {current_notes}",
                                    "severity": "Info"
                                })

                        # Calculate PySpark start line for remapping (based on *current* total lines)
                        current_pyspark_start_line = cumulative_pyspark_line_offset + 1

                        # 3e. Remap Annotation Lines
                        remapped_chunk_annotations = remap_annotations(
                            annotations_chunk,
                            chunk_abs_start_line,
                            current_pyspark_start_line
                        )
                        final_annotations.extend(remapped_chunk_annotations)

                        # 3f. Store result & Update PySpark line offset
                        final_pyspark_pieces.append(pyspark_code_chunk)
                        cumulative_pyspark_line_offset += len(pyspark_code_chunk.splitlines()) + 2 # Account for separators

                        # 3g. Update Overall Context
                        # Pass the *original* logical chunk info and the *final* (potentially refined) code
                        overall_context = update_overall_context(
                            overall_context,
                            logical_chunk,
                            pyspark_code_chunk,
                            annotations_chunk # Pass original annotations before remapping
                        )
                        # Store outputs for potential schema inference later
                        for output_name in logical_chunk.get('outputs', []):
                             pyspark_outputs_in_sc[output_name] = {"code_preview": pyspark_code_chunk[:200]}


                    # Update cumulative SAS line offset for the next super-chunk
                    cumulative_sas_line_offset += len(super_chunk_lines)
                    logger.info(f"Finished processing Super-Chunk {i+1}.")


                # 4. Final Assembly & Formatting
                logger.info("Assembling final code from pieces...")
                final_pyspark_code, format_warnings = assemble_and_format_code(final_pyspark_pieces)
                warnings.extend(format_warnings)
                logger.info("Complex Path conversion successful.")

            except Exception as e:
                logger.error(f"Complex Path failed: {e}", exc_info=True)
                warnings.append(f"Complex Path conversion failed: {e}")
                # Attempt to return partially converted code if available
                if final_pyspark_pieces:
                     partial_code, format_warnings = assemble_and_format_code(final_pyspark_pieces)
                     warnings.extend(format_warnings)
                     final_pyspark_code = f"# WARNING: Complex Path failed, returning partial result.\n# Error: {e}\n\n{partial_code}"
                else:
                     final_pyspark_code = f"# ERROR: Complex Path conversion failed entirely: {e}\n# Original SAS:\n# {sas_code.replace(chr(10), chr(10)+'# ')}"


        # --- Response Preparation ---
        end_invocation_time = time.time()
        processing_stats = {
            "total_duration_seconds": round(end_invocation_time - start_invocation_time, 2),
            "llm_calls": token_usage_stats['api_calls'],
            "token_usage": token_usage_stats,
            "llm_call_details": llm_call_counts, # Include breakdown
            "complexity_assessment": complexity,
            "sas_code_lines": sas_code.count('\n') + 1,
            "generated_pyspark_lines": final_pyspark_code.count('\n') + 1
        }

        logger.info(f"Invocation finished. Duration: {processing_stats['total_duration_seconds']:.2f}s. LLM Calls: {processing_stats['llm_calls']}.")

        # Construct final JSON output
        final_output = {
            "status": "success" if not any(w.startswith("ERROR:") or "failed" in w for w in warnings) else "warning",
            "pyspark_code": final_pyspark_code,
            "annotations": final_annotations,
            "warnings": warnings,
            "strategy_used": strategy_used,
            "processing_stats": processing_stats,
            "processing_complete": True # Indicate completion
        }

        # Return response
        return {
            'statusCode': 200,
            'headers': response_headers,
            'body': json.dumps(final_output, ensure_ascii=False, default=str) # Use default=str for safety
        }

    except Exception as e:
        logger.error(f"Unhandled exception in lambda_handler: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': response_headers,
            'body': json.dumps({
                'status': 'error',
                'message': f"Internal server error: {e}",
                'details': str(e)
            }, ensure_ascii=False)
        }

# --- Test Handler ---
def test_handler(sas_code, options=None):
    """ Utility for testing the lambda function locally or from console """
    test_event = {
        "body": json.dumps({
            "sas_code": sas_code,
            "options": options or {}
        })
    }
    # Mock context object
    class MockContext:
        def get_remaining_time_in_millis(self):
            return 300000 # 5 minutes

    result = lambda_handler(test_event, MockContext())
    # Pretty print the JSON body
    try:
        body_json = json.loads(result['body'])
        print(json.dumps(body_json, indent=2))
    except:
        print(result['body']) # Print raw if not JSON
    return result

# Example usage for local testing (if run directly)
if __name__ == '__main__':
    # Ensure Bedrock client is initialized for local tests if needed
    # Note: Local testing might require AWS credentials configured
    if not USE_BEDROCK:
         print("Warning: Bedrock client not initialized. Running in demo mode.")

    print("Running local test with Simple SAS code...")
import json
import re
import logging
import time
import boto3
import os
import hashlib
from functools import lru_cache
from botocore.config import Config
import ast # For basic syntax check

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Bedrock Client Initialization ---
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
boto_config = Config(read_timeout=3600) # Increased timeout

try:
    bedrock_runtime = boto3.client('bedrock-runtime', region_name=AWS_REGION, config=boto_config)
    USE_BEDROCK = True
    logger.info("Successfully initialized Bedrock client")
except Exception as e:
    logger.warning(f"Failed to initialize Bedrock client: {e}. Will run in demo/test mode only.")
    bedrock_runtime = None
    USE_BEDROCK = False

# --- Configuration ---
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-3-sonnet-20240229-v1:0") # Defaulting to Sonnet 3
ANTHROPIC_VERSION = "bedrock-2023-05-31" # Required for Claude 3 models

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.environ.get("RETRY_DELAY_SECONDS", "2"))
RETRY_BACKOFF_FACTOR = float(os.environ.get("RETRY_BACKOFF_FACTOR", "2.0"))
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "4000")) # Default max tokens for Sonnet 3 response
CONTEXT_SIZE_LIMIT = int(os.environ.get("CONTEXT_SIZE_LIMIT", "150000")) # Approx limit before summarization
REFINEMENT_THRESHOLD = float(os.environ.get("REFINEMENT_THRESHOLD", "3.5")) # Confidence score below which refinement is triggered
MAX_REFINEMENT_ATTEMPTS = int(os.environ.get("MAX_REFINEMENT_ATTEMPTS", "1")) # Limit refinement loops

# Complexity Analyzer Thresholds (Configurable via Env Vars)
SIMPLE_MAX_LINES = int(os.environ.get("SIMPLE_MAX_LINES", "500"))
SIMPLE_MAX_MACROS = int(os.environ.get("SIMPLE_MAX_MACROS", "5"))
SIMPLE_MAX_PROCS = int(os.environ.get("SIMPLE_MAX_PROCS", "10"))
# Optional complexity thresholds (can be added if needed)
# SIMPLE_MAX_MACRO_CALLS = int(os.environ.get("SIMPLE_MAX_MACRO_CALLS", "20"))
# Optional complexity thresholds for more fine-grained control
# COMPLEX_PROC_LIST = os.environ.get("COMPLEX_PROC_LIST", "FCMP,LUA,PROTO,OPTMODEL,IML").split(',')
# CHECK_INCLUDES = os.environ.get("CHECK_INCLUDES", "FALSE").upper() == "TRUE"


# Caching Configuration
USE_LLM_CACHING = os.environ.get("USE_LLM_CACHING", "TRUE").upper() == "TRUE"
MAX_CACHE_ENTRIES = int(os.environ.get("MAX_CACHE_ENTRIES", "100"))
llm_response_cache = {}

# Define error handling retry statuses for Bedrock
BEDROCK_RETRY_EXCEPTIONS = [
    'ThrottlingException',
    'ServiceUnavailableException',
    'InternalServerException',
    'ModelTimeoutException',
    'ModelErrorException', # Added based on potential Bedrock errors
    'ValidationException' # Added for input validation issues
]

# Initialize global statistics
token_usage_stats = {
    'total_tokens': 0,
    'input_tokens': 0,
    'output_tokens': 0,
    'api_calls': 0
}
llm_call_counts = {
    'complexity_analysis': 0,
    'simple_conversion': 0,
    'structural_analysis': 0,
    'context_focus': 0, # Not implemented in this version, placeholder
    'chunk_conversion': 0,
    'refinement': 0,
    'summarization': 0
}

# --- Caching ---
def cache_key(prompt, model_id=None):
    """Generate a cache key from the prompt and optional model ID"""
    key_content = f"{prompt}:{model_id or BEDROCK_MODEL_ID}"
    return hashlib.md5(key_content.encode('utf-8')).hexdigest()

def manage_cache_size():
    """Removes oldest entries if cache exceeds max size."""
    if len(llm_response_cache) > MAX_CACHE_ENTRIES:
        # Simple FIFO eviction
        num_to_remove = len(llm_response_cache) - MAX_CACHE_ENTRIES
        keys_to_remove = list(llm_response_cache.keys())[:num_to_remove]
        for key in keys_to_remove:
            del llm_response_cache[key]
        logger.info(f"Cache full. Removed {num_to_remove} oldest entries.")

# --- Bedrock LLM Interaction ---
def _invoke_claude3(prompt, max_tokens=MAX_TOKENS):
    """
    Invokes a Claude 3 model on Bedrock (handles Sonnet/Haiku).
    Handles the specific request/response format for Anthropic models via Bedrock.
    Updates global token usage stats.
    """
    if not USE_BEDROCK or bedrock_runtime is None:
        logger.warning("Bedrock is not available. Returning demo response.")
        # Return a structured demo response matching expected output formats
        return """{"pyspark_code": "# Demo mode - Bedrock not available\nprint('Demo conversion')",
                   "annotations": [{"sas_lines": [1, 1], "pyspark_lines": [1, 1], "note": "Demo mode", "severity": "Info"}],
                   "confidence_score": 5, "refinement_notes": ""}""" # Include fields for all call types

    messages = [{"role": "user", "content": prompt}]
    body = {
        "anthropic_version": ANTHROPIC_VERSION,
        "max_tokens": max_tokens,
        "messages": messages,
        "temperature": float(os.environ.get("LLM_TEMPERATURE", "0.1")), # Lower temp for more deterministic output
        "top_p": float(os.environ.get("LLM_TOP_P", "0.9")),
    }

    try:
        logger.info(f"Invoking {BEDROCK_MODEL_ID} with max_tokens={max_tokens}")
        start_time = time.time()
        response = bedrock_runtime.invoke_model(
            body=json.dumps(body),
            modelId=BEDROCK_MODEL_ID,
            accept='application/json',
            contentType='application/json'
        )
        response_time = time.time() - start_time
        response_body = json.loads(response['body'].read())

        # Track token usage
        input_tokens = response_body.get("usage", {}).get("input_tokens", 0)
        output_tokens = response_body.get("usage", {}).get("output_tokens", 0)
        total_tokens = input_tokens + output_tokens

        logger.info(f"Token usage: {input_tokens} input + {output_tokens} output = {total_tokens} total tokens")
        logger.info(f"Response time: {response_time:.2f}s")

        # Update global stats
        token_usage_stats['total_tokens'] += total_tokens
        token_usage_stats['input_tokens'] += input_tokens
        token_usage_stats['output_tokens'] += output_tokens
        token_usage_stats['api_calls'] += 1

        # Extract content
        if response_body.get("type") == "message" and response_body.get("content"):
            llm_output = response_body["content"][0]["text"]
            return llm_output
        elif response_body.get("error"):
             error_type = response_body["error"].get("type")
             error_message = response_body["error"].get("message")
             logger.error(f"Bedrock API Error: {error_type} - {error_message}")
             # Raise specific exception types if needed for retry logic
             if error_type == 'overloaded_error':
                 raise Exception("Model overloaded") # Or a custom exception
             else:
                 raise Exception(f"Bedrock Error: {error_type} - {error_message}")
        else:
            logger.error(f"Unexpected Bedrock response format: {response_body}")
            raise ValueError("Unexpected response format from Bedrock Claude 3")

    except Exception as e:
        logger.error(f"Bedrock invoke_model failed: {e}", exc_info=True)
        raise # Re-raise to be caught by retry logic

def _call_llm_with_retry(invoke_function, prompt, description="LLM call", llm_call_type_key=None, bypass_cache=False, max_tokens=MAX_TOKENS):
    """
    Wrapper for retry logic around Bedrock invoke_model calls. Includes caching and exponential backoff.
    """
    # Check cache first
    cache_key_val = cache_key(prompt)
    if USE_LLM_CACHING and not bypass_cache and cache_key_val in llm_response_cache:
        logger.info(f"Cache hit for {description}! Using cached response.")
        return llm_response_cache[cache_key_val]

    # Proceed with API call
    last_exception = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            logger.info(f"Attempting {description} (Attempt {attempt + 1}/{MAX_RETRIES + 1})...")
            result = invoke_function(prompt, max_tokens=max_tokens) # Pass max_tokens here

            # Store in cache
            if USE_LLM_CACHING:
                llm_response_cache[cache_key_val] = result
                manage_cache_size() # Check and manage cache size after adding

            # Increment specific LLM call counter if key provided
            if llm_call_type_key and llm_call_type_key in llm_call_counts:
                llm_call_counts[llm_call_type_key] += 1

            return result # Success

        except Exception as e:
            last_exception = e
            error_name = type(e).__name__
            error_str = str(e)
            logger.warning(f"{description} attempt {attempt + 1} failed: {error_name} - {error_str}")

            # Check if retryable
            is_retryable = any(retry_err in error_name for retry_err in BEDROCK_RETRY_EXCEPTIONS) or \
                           any(retry_err in error_str for retry_err in BEDROCK_RETRY_EXCEPTIONS)

            if attempt < MAX_RETRIES and is_retryable:
                delay = RETRY_DELAY_SECONDS * (RETRY_BACKOFF_FACTOR ** attempt)
                logger.info(f"Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"{description} failed after {attempt + 1} attempts.")
                break # Exit loop after final attempt or non-retryable error

    # If loop finished without success, raise the last exception
    raise last_exception or Exception(f"{description} failed after {MAX_RETRIES + 1} attempts.")


# --- Complexity Analysis ---
def assess_complexity(sas_code, thresholds):
    """
    Analyzes SAS code complexity based on heuristics to determine the conversion path.

    Args:
        sas_code (str): The SAS code.
        thresholds (dict): Dictionary containing complexity thresholds like
                           {'SIMPLE_MAX_LINES': 500, 'SIMPLE_MAX_MACROS': 5, ...}.

    Returns:
        str: "Simple" or "Complex".
    """
    logger.info("Assessing SAS code complexity using heuristics...")
    loc = sas_code.count('\n') + 1
    # char_count = len(sas_code) # Less reliable metric on its own

    # Core metrics
    macro_defs = len(re.findall(r'^\s*%MACRO\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))
    proc_calls = len(re.findall(r'^\s*PROC\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))

    # Optional advanced metrics (can be enabled via config/thresholds)
    # macro_calls_approx = len(re.findall(r'%\w+\s*\(', sas_code, re.IGNORECASE)) # Approximate macro calls
    # complex_procs_found = []
    # if 'COMPLEX_PROC_LIST' in thresholds:
    #     complex_proc_pattern = r'^\s*PROC\s+(' + '|'.join(thresholds['COMPLEX_PROC_LIST']) + r')\b'
    #     complex_procs_found = re.findall(complex_proc_pattern, sas_code, re.IGNORECASE | re.MULTILINE)
    # has_includes = False
    # if thresholds.get('CHECK_INCLUDES', False):
    #     has_includes = bool(re.search(r'^\s*%INCLUDE\b', sas_code, re.IGNORECASE | re.MULTILINE))

    # Classification Rule
    is_simple = (
        loc <= thresholds.get('SIMPLE_MAX_LINES', 500) and
        macro_defs <= thresholds.get('SIMPLE_MAX_MACROS', 5) and
        proc_calls <= thresholds.get('SIMPLE_MAX_PROCS', 10) #and
        # Optional checks:
        # macro_calls_approx <= thresholds.get('SIMPLE_MAX_MACRO_CALLS', 20) and
        # not complex_procs_found and
        # not has_includes
    )

    complexity = "Simple" if is_simple else "Complex"
    logger.info(f"Complexity assessed as: {complexity} (LoC: {loc}, Macro Defs: {macro_defs}, PROC Calls: {proc_calls})")
    # logger.debug(f"Optional metrics - Approx Macro Calls: {macro_calls_approx}, Complex PROCs: {complex_procs_found}, Has Includes: {has_includes}")
    return complexity

# --- Code Splitting ---
def deterministic_split(sas_code, max_chunk_size=50000):
    """
    Splits SAS code into super-chunks based on PROC/DATA/MACRO boundaries.
    (Reusing the existing robust implementation)
    """
    logger.info("Performing deterministic splitting into super-chunks...")
    if len(sas_code) < 1000: # Optimization for small code
        logger.info("Code is small, treating as a single chunk.")
        return [sas_code]

    # Regex to find major boundaries (start of line, case-insensitive)
    boundary_pattern = re.compile(r'^\s*(PROC\s+\w+|DATA\s+[\w\.]+|%MACRO\s+[\w\.]+)\b.*?;', re.IGNORECASE | re.MULTILINE | re.DOTALL)
    boundaries = list(boundary_pattern.finditer(sas_code))

    if not boundaries:
        logger.warning("No clear PROC/DATA/MACRO boundaries found. Treating as one chunk.")
        return [sas_code]

    logger.info(f"Found {len(boundaries)} potential split points.")
    super_chunks = []
    start_index = 0
    current_chunk_start_index = 0

    for i, match in enumerate(boundaries):
        split_point = match.start()

        # If this isn't the first boundary and the current chunk exceeds max size, split before this boundary
        if i > 0 and (split_point - current_chunk_start_index > max_chunk_size):
            chunk = sas_code[current_chunk_start_index:start_index] # Split at the *previous* boundary
            if chunk.strip():
                super_chunks.append(chunk)
                logger.info(f"Created super-chunk (size: {len(chunk)} chars)")
                current_chunk_start_index = start_index # Start next chunk from previous boundary

        # Update the potential end of the current logical block
        start_index = match.start()

    # Add the final chunk (from the last boundary start to the end)
    final_chunk = sas_code[current_chunk_start_index:]
    if final_chunk.strip():
         # If the final chunk itself is too large, try an emergency split (e.g., by lines)
        if len(final_chunk) > max_chunk_size * 1.2: # Allow some overshoot
             logger.warning(f"Final chunk is very large ({len(final_chunk)} chars). Performing emergency line split.")
             lines = final_chunk.splitlines()
             temp_chunk = []
             current_len = 0
             for line in lines:
                 line_len = len(line) + 1 # Account for newline
                 if current_len + line_len > max_chunk_size and temp_chunk:
                     super_chunks.append("\n".join(temp_chunk))
                     logger.info(f"Created super-chunk (emergency split, size: {current_len} chars)")
                     temp_chunk = [line]
                     current_len = line_len
                 else:
                     temp_chunk.append(line)
                     current_len += line_len
             if temp_chunk: # Add remaining lines
                 super_chunks.append("\n".join(temp_chunk))
                 logger.info(f"Created super-chunk (emergency split, size: {current_len} chars)")
        else:
            super_chunks.append(final_chunk)
            logger.info(f"Created final super-chunk (size: {len(final_chunk)} chars)")


    logger.info(f"Split SAS code into {len(super_chunks)} super-chunks.")
    return [chunk for chunk in super_chunks if chunk.strip()]


# --- Structural Analysis (Complex Path) ---
def call_llm_for_structural_analysis(super_chunk):
    """Calls LLM to identify logical chunks within a super-chunk."""
    logger.info("Calling LLM for structural analysis...")
    # Optimization: Skip LLM for very small super-chunks
    if len(super_chunk.splitlines()) < 10: # Heuristic threshold
        logger.info("Super-chunk is small, creating a single default logical chunk.")
        return [{
            "type": "UNKNOWN", "name": "small_chunk",
            "start_line": 1, "end_line": len(super_chunk.splitlines()),
            "inputs": [], "outputs": []
        }]

    prompt = f"""Analyze the following SAS code block. Identify all distinct logical units (DATA steps, PROC steps, defined %MACROs).
Output ONLY a valid JSON list where each item represents a unit and includes:
{{
  "type": "DATA" | "PROC" | "MACRO",
  "name": "step/proc/macro_name", // Best guess for the name
  "start_line": <integer>, // Starting line number (1-based) within this super_chunk
  "end_line": <integer>, // Ending line number (1-based) within this super_chunk
  "inputs": ["dataset_or_macro_used"], // List of likely input dataset/macro names used
  "outputs": ["dataset_or_macro_created"] // List of likely output dataset/macro names created
}}
Ensure the output is ONLY the JSON list, starting with '[' and ending with ']'. Be precise with line numbers.

SAS Code Block:
```sas
{super_chunk}
```"""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Structural Analysis", llm_call_type_key='structural_analysis')
    try:
        # Clean potential markdown fences
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        logical_chunks = json.loads(cleaned_response)
        if isinstance(logical_chunks, list):
            # Basic validation
            for chunk in logical_chunks:
                if not all(k in chunk for k in ["type", "name", "start_line", "end_line", "inputs", "outputs"]):
                    raise ValueError("Invalid chunk structure in structural analysis response.")
            logger.info(f"LLM identified {len(logical_chunks)} logical chunks.")
            # Handle empty list case
            if not logical_chunks:
                 logger.warning("LLM returned empty chunk list, creating default chunk for the super-chunk.")
                 return [{
                    "type": "UNKNOWN", "name": "default_chunk",
                    "start_line": 1, "end_line": len(super_chunk.splitlines()),
                    "inputs": [], "outputs": []
                 }]
            return logical_chunks
        else:
            raise ValueError("LLM response for structural analysis was not a JSON list.")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON for structural analysis: {e}\nResponse: {response_text}")
        # Fallback: treat the whole super-chunk as one logical chunk
        return [{"type": "UNKNOWN", "name": "fallback_chunk", "start_line": 1, "end_line": len(super_chunk.splitlines()), "inputs": [], "outputs": []}]
    except Exception as e:
        logger.error(f"Error processing structural analysis: {e}\nResponse: {response_text}")
        raise # Re-raise other errors


# --- Context Management (Complex Path) ---
# Context Focusing - Placeholder for future optimization
# def call_llm_for_context_focus(overall_context, logical_chunk_code): ...

def update_overall_context(overall_context, logical_chunk, pyspark_output, annotations):
    """
    Updates the overall context with outputs from a processed logical chunk.
    Focuses on schema and macro definitions.
    """
    logger.debug(f"Updating overall context after processing chunk: {logical_chunk.get('name')}")
    if "schema_info" not in overall_context: overall_context["schema_info"] = {}
    if "macros_defined" not in overall_context: overall_context["macros_defined"] = {}
    if "processed_chunk_outputs" not in overall_context: overall_context["processed_chunk_outputs"] = {}

    chunk_type = logical_chunk.get('type', '').upper()
    chunk_name = logical_chunk.get('name', 'unnamed')

    # Store outputs (datasets/macros created)
    outputs = logical_chunk.get('outputs', [])
    overall_context["processed_chunk_outputs"][chunk_name] = outputs

    for output_name in outputs:
        if chunk_type == 'MACRO':
            # Basic macro definition tracking
            overall_context["macros_defined"][output_name] = {
                "status": "defined",
                "source_chunk": chunk_name,
                "parameters": [], # TODO: Could try to extract params from code/annotations
                "body_preview": pyspark_output[:200] # Store a preview
            }
            logger.debug(f"Added/Updated macro definition: {output_name}")
        elif chunk_type in ['DATA', 'PROC']:
            # Basic schema tracking - could be enhanced by LLM extracting schema from code/annotations
            # For now, just mark the dataset as created
            overall_context["schema_info"][output_name] = {
                "status": "created/modified",
                "source_chunk": chunk_name,
                "derived_from": logical_chunk.get('inputs', []),
                "columns": ["unknown"] # Placeholder - needs schema extraction logic
            }
            logger.debug(f"Added/Updated schema info for dataset: {output_name}")

    # Check context size and summarize if needed
    try:
        current_context_size = len(json.dumps(overall_context))
        logger.debug(f"Current estimated context size: {current_context_size} chars.")
        if current_context_size > CONTEXT_SIZE_LIMIT:
            logger.warning(f"Context size ({current_context_size}) exceeds limit ({CONTEXT_SIZE_LIMIT}). Attempting summarization...")
            overall_context = call_llm_for_summarization(overall_context)
            new_size = len(json.dumps(overall_context))
            logger.info(f"Context size after summarization: {new_size} chars.")
    except TypeError as e:
        logger.warning(f"Could not estimate context size or summarize due to content: {e}")
    except Exception as e:
         logger.error(f"Error during context size check/summarization: {e}")


    return overall_context

def call_llm_for_summarization(overall_context):
    """Calls LLM to summarize the overall context if it gets too large."""
    logger.info("Calling LLM for context summarization...")
    try:
        context_to_summarize_json = json.dumps(overall_context, indent=2)
    except TypeError:
         logger.error("Cannot serialize context for summarization. Skipping.")
         return overall_context # Return original if cannot serialize

    prompt = f"""Summarize the key information in the provided context JSON concisely, preserving essential details for downstream SAS to PySpark conversion.
Focus on:
1. Dataset schemas (column names, types if available) for the *most recently* created/modified datasets.
2. All defined macros and their parameters/previews.
3. Key dataset lineage (outputs of recent chunks).

Output ONLY a valid JSON object with the same structure as the input, but summarized. Remove older schema entries or less critical details if necessary to reduce size.

Context JSON to Summarize:
```json
{context_to_summarize_json}
```

Output ONLY the summarized JSON object."""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Context Summarization", llm_call_type_key='summarization')
    try:
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        summarized_context = json.loads(cleaned_response)
        if isinstance(summarized_context, dict):
            logger.info("Context summarization successful.")
            return summarized_context
        else:
            raise ValueError("LLM response for summarization was not a JSON object.")
    except Exception as e:
        logger.error(f"Failed to process summarization response: {e}. Returning original context.")
        return overall_context # Fallback to original context on error


# --- Code Conversion (Simple and Complex Paths) ---
def call_llm_for_conversion(code_to_convert, context, is_simple_path=False, chunk_info=None):
    """
    Calls LLM to convert SAS code (either full script or a chunk) to PySpark.
    Returns pyspark_code, annotations, and confidence score.
    """
    logger.info(f"Calling LLM for conversion ({'Simple Path' if is_simple_path else 'Chunk: '+chunk_info.get('name', 'N/A')})...")
    try:
        context_json = json.dumps(context, indent=2, default=str) # Use default=str for non-serializable items
    except TypeError as e:
        logger.warning(f"Could not serialize context for conversion: {e}. Using empty context.")
        context_json = "{}"

    path_specific_instructions = ""
    if is_simple_path:
        path_specific_instructions = "You are converting the entire SAS script in one go."
        llm_call_key = 'simple_conversion'
    else:
        path_specific_instructions = f"You are converting a logical chunk identified as '{chunk_info.get('name', 'N/A')}' (type: {chunk_info.get('type', 'N/A')}). Focus on this chunk, using the provided context for dependencies."
        llm_call_key = 'chunk_conversion'

    prompt = f"""Convert the following SAS code {'snippet' if not is_simple_path else 'script'} to equivalent PySpark code.

{path_specific_instructions}

Context (use this for dependencies like schemas, macros):
```json
{context_json}
```

SAS Code to Convert:
```sas
{code_to_convert}
```

Conversion Guidelines:
- Maintain semantic equivalence.
- Use idiomatic PySpark (DataFrame API).
- Include necessary imports (placeholders are fine, they will be consolidated later).
- Add comments explaining the conversion logic, especially for complex parts or assumptions. Reference original SAS line numbers if possible.
- Handle SAS-specific features (e.g., macro variables, data step logic, PROCs) appropriately.
- If a direct conversion isn't possible, implement the closest equivalent and add a 'Warning' annotation.
- Assess your confidence in the conversion quality for this specific piece of code.

Output ONLY a valid JSON object with the following structure:
{{
  "pyspark_code": "...",
  "annotations": [
    {{
      "sas_lines": [<start_line_int>, <end_line_int>], // Relative to the input SAS code snippet/script
      "pyspark_lines": [<start_line_int>, <end_line_int>], // Relative to the generated PySpark code
      "note": "Explanation...",
      "severity": "Info" | "Warning"
    }}
  ],
  "confidence_score": <float 1.0-5.0> // Your confidence in this specific conversion's accuracy
}}
Ensure the output is ONLY the JSON object.
"""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Conversion", llm_call_type_key=llm_call_key)
    try:
        # Clean potential markdown fences and control characters
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        cleaned_response = ''.join(c for c in cleaned_response if c.isprintable() or c in '\n\r\t') # Keep whitespace

        result = json.loads(cleaned_response)

        # Validate structure
        if isinstance(result, dict) and \
           all(k in result for k in ["pyspark_code", "annotations", "confidence_score"]) and \
           isinstance(result["pyspark_code"], str) and \
           isinstance(result["annotations"], list) and \
           isinstance(result["confidence_score"], (int, float)):

            # Basic validation of annotations
            valid_annotations = []
            for ann in result["annotations"]:
                if isinstance(ann, dict) and all(k in ann for k in ["sas_lines", "pyspark_lines", "note", "severity"]):
                    valid_annotations.append(ann)
                else:
                     logger.warning(f"Skipping invalid annotation structure: {ann}")
            result["annotations"] = valid_annotations

            logger.info(f"LLM conversion successful. Confidence: {result['confidence_score']:.1f}/5.0")
            return result["pyspark_code"], result["annotations"], float(result["confidence_score"])
        else:
            raise ValueError("LLM response for conversion has invalid structure.")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON for conversion: {e}\nResponse: {response_text}")
        # Fallback: return error code, annotation, and low confidence
        error_code = f"# ERROR: Failed to parse LLM conversion response.\n# Original SAS:\n# {code_to_convert.replace(chr(10), chr(10)+'# ')}"
        error_ann = [{'sas_lines': [1,1], 'pyspark_lines': [1,1], 'note': f'LLM response was not valid JSON: {e}', 'severity': 'Warning'}]
        return error_code, error_ann, 1.0 # Low confidence
    except Exception as e:
        logger.error(f"Error processing conversion response: {e}\nResponse: {response_text}")
        error_code = f"# ERROR: Exception processing conversion response: {e}\n# Original SAS:\n# {code_to_convert.replace(chr(10), chr(10)+'# ')}"
        error_ann = [{'sas_lines': [1,1], 'pyspark_lines': [1,1], 'note': f'Exception during conversion processing: {e}', 'severity': 'Warning'}]
        return error_code, error_ann, 1.0 # Low confidence


# --- Code Refinement (Complex Path) ---
def call_llm_for_refinement(pyspark_section, context, previous_notes=None):
    """Calls LLM to refine a PySpark code section and assess confidence."""
    logger.info("Calling LLM for refinement...")
    try:
        context_json = json.dumps(context, indent=2, default=str)
    except TypeError:
        context_json = "{}" # Send empty if serialization fails

    refinement_focus = "Focus on correctness against SAS semantics, Spark efficiency, PEP 8 style, and clarity."
    if previous_notes:
        refinement_focus = f"Confidence was low previously. Focus on addressing these notes:\n'''\n{previous_notes}\n'''\nAlso perform a general review."

    prompt = f"""Review and refine the following PySpark code section.
{refinement_focus}

Context (for reference):
```json
{context_json}
```

PySpark Code Section to Refine:
```python
{pyspark_section}
```

Output ONLY a valid JSON object with:
{{
  "refined_code": "...", // The improved PySpark code
  "confidence_score": <float 1.0-5.0>, // Updated confidence score
  "refinement_notes": "..." // Any *new* or *remaining* issues or suggestions
}}
Ensure the output is ONLY the JSON object.
"""

    response_text = _call_llm_with_retry(_invoke_claude3, prompt, description="LLM Refinement", llm_call_type_key='refinement')
    try:
        cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
        cleaned_response = ''.join(c for c in cleaned_response if c.isprintable() or c in '\n\r\t')

        result = json.loads(cleaned_response)

        # Validate structure
        if isinstance(result, dict) and \
           all(k in result for k in ["refined_code", "confidence_score", "refinement_notes"]) and \
           isinstance(result["refined_code"], str) and \
           isinstance(result["confidence_score"], (int, float)) and \
           isinstance(result["refinement_notes"], str):
            logger.info(f"LLM refinement successful. New Confidence: {result['confidence_score']:.1f}/5.0")
            return result["refined_code"], float(result["confidence_score"]), result["refinement_notes"]
        else:
            raise ValueError("LLM response for refinement has invalid structure.")
    except Exception as e:
        logger.error(f"Failed to process refinement response: {e}. Returning original code and low confidence.")
        # Fallback: return original code, low confidence, error note
        return pyspark_section, 1.0, f"Error processing refinement response: {e}"


# --- Code Assembly & Formatting ---
def assemble_and_format_code(pyspark_pieces):
    """Joins PySpark code pieces, moves imports, and formats."""
    logger.info("Assembling and formatting final PySpark code...")
    full_code = "\n\n# --- Autogenerated Chunk Separator ---\n\n".join(pyspark_pieces)

    # Extract and consolidate imports
    imports = set()
    body_lines = []
    import_pattern = re.compile(r'^\s*(import\s+[\w\.,\s]+|from\s+\w+(\.\w+)*\s+import\s+[\w\*\.,\s\(\)]+)')

    for line in full_code.splitlines():
        match = import_pattern.match(line)
        if match:
            # Normalize and add unique imports
            import_statement = match.group(0).strip()
            imports.add(import_statement)
        else:
            body_lines.append(line)

    # Sort imports (basic alphabetical sort)
    sorted_imports = sorted(list(imports))
    final_code = "\n".join(sorted_imports) + "\n\n# --- End Imports ---\n\n" + "\n".join(body_lines)

    # Apply basic linting/formatting
    formatted_code, warnings = basic_lint_check(final_code)
    return formatted_code, warnings

def basic_lint_check(pyspark_code):
    """Performs basic syntax check and formatting."""
    logger.info("Performing basic lint check and formatting...")
    warnings = []
    formatted_code = pyspark_code # Start with original code

    # 1. Syntax Check using ast
    try:
        ast.parse(formatted_code)
        logger.info("Basic Python syntax check passed (ast.parse).")
    except SyntaxError as e:
        logger.error(f"Syntax error found: {e}")
        warnings.append(f"Syntax Error at line ~{e.lineno}: {e.msg}")
        # Don't attempt further formatting if basic syntax fails
        return formatted_code, warnings
    except Exception as e:
        logger.error(f"Error during AST syntax check: {e}")
        warnings.append(f"AST Check Error: {e}")

    # 2. Basic Formatting (if syntax is ok)
    try:
        # Attempt formatting with black if available
        try:
            import black
            mode = black.Mode(line_length=100) # Configurable line length
            formatted_code = black.format_str(formatted_code, mode=mode)
            logger.info("Applied 'black' formatting.")
        except ImportError:
            logger.warning("Optional formatter 'black' not installed. Skipping black formatting.")
        except Exception as e:
            logger.warning(f"Could not apply 'black' formatting: {e}")

        # Add other simple formatting rules here if needed (e.g., whitespace cleanup)

    except Exception as e:
        logger.error(f"Error during basic formatting: {e}")
        warnings.append(f"Formatting Error: {e}")
        # Return the last valid state of the code if formatting fails
        return pyspark_code, warnings

    return formatted_code, warnings


# --- Annotation Line Number Remapping ---
def remap_annotations(annotations, chunk_abs_start_line, chunk_pyspark_start_line):
    """Adjusts line numbers in annotations to be absolute within the final file."""
    remapped = []
    for ann in annotations:
        try:
            # Adjust SAS lines (relative to chunk -> absolute in original SAS)
            sas_start = ann['sas_lines'][0] + chunk_abs_start_line - 1
            sas_end = ann['sas_lines'][1] + chunk_abs_start_line - 1

            # Adjust PySpark lines (relative to chunk code -> absolute in final PySpark)
            # Note: This assumes chunks are simply concatenated. More complex assembly might need different logic.
            pyspark_start = ann['pyspark_lines'][0] + chunk_pyspark_start_line - 1
            pyspark_end = ann['pyspark_lines'][1] + chunk_pyspark_start_line - 1

            remapped.append({
                **ann, # Copy other fields
                "sas_lines": [sas_start, sas_end],
                "pyspark_lines": [pyspark_start, pyspark_end]
            })
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"Skipping annotation due to remapping error ({e}): {ann}")
    return remapped


# --- Main Lambda Handler ---
def lambda_handler(event, context):
    """
    Main AWS Lambda handler function. Orchestrates SAS to PySpark conversion using Adaptive Strategy.
    """
    start_invocation_time = time.time()
    logger.info("Lambda invocation started.")

    # Reset global stats for this invocation
    global token_usage_stats, llm_call_counts
    token_usage_stats = {'total_tokens': 0, 'input_tokens': 0, 'output_tokens': 0, 'api_calls': 0}
    llm_call_counts = {k: 0 for k in llm_call_counts}

    # Clear cache periodically if enabled
    if USE_LLM_CACHING and token_usage_stats['api_calls'] % 10 == 0: # Heuristic: clear every 10 calls
         manage_cache_size()

    # CORS Headers
    response_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'OPTIONS,POST',
        'Content-Type': 'application/json; charset=utf-8'
    }

    # Handle OPTIONS preflight request
    if event.get('httpMethod') == 'OPTIONS':
        logger.info("Handling OPTIONS preflight request.")
        return {'statusCode': 200, 'headers': response_headers, 'body': json.dumps('OK')}

    # --- Demo Mode Check ---
    if not USE_BEDROCK:
        logger.warning("Running in DEMO mode - Bedrock client not initialized.")
        # Provide a minimal demo response
        demo_pyspark = "# DEMO MODE - Bedrock not available\nprint('This is a demo conversion.')"
        demo_annotations = [{"sas_lines": [1, 1], "pyspark_lines": [1, 1], "note": "Running in demo mode.", "severity": "Info"}]
        return {
            'statusCode': 200,
            'headers': response_headers,
            'body': json.dumps({
                "status": "success",
                "pyspark_code": demo_pyspark,
                "annotations": demo_annotations,
                "warnings": ["Running in demo mode without Bedrock access."],
                "strategy_used": "Demo",
                "processing_stats": {"total_duration_seconds": 0.1, "llm_calls": 0, "token_usage": token_usage_stats, "llm_call_details": llm_call_counts},
                "processing_complete": True
            }, ensure_ascii=False)
        }

    # --- Request Parsing ---
    try:
        logger.info("Parsing request event...")
        # Handle potential API Gateway proxy structure
        if 'body' in event:
            try:
                body = json.loads(event.get('body', '{}'))
            except json.JSONDecodeError:
                 logger.error("Invalid JSON in request body.")
                 return {'statusCode': 400, 'headers': response_headers, 'body': json.dumps({'error': 'Invalid JSON body'})}
        else:
             body = event # Assume direct invocation

        sas_code = body.get('sas_code')
        options = body.get('options', {}) # e.g., {"add_detailed_comments": false}

        if not sas_code:
            logger.error("Missing 'sas_code' in request.")
            return {'statusCode': 400, 'headers': response_headers, 'body': json.dumps({'error': "Request must contain 'sas_code'"})}

        logger.info(f"Received SAS code ({len(sas_code)} chars). Options: {options}")

        # --- Adaptive Strategy Orchestration ---
        warnings = []
        final_pyspark_code = ""
        final_annotations = []
        strategy_used = "Unknown"

        # 1. Assess Complexity
        complexity_thresholds = {
            'SIMPLE_MAX_LINES': SIMPLE_MAX_LINES,
            'SIMPLE_MAX_MACROS': SIMPLE_MAX_MACROS,
            'SIMPLE_MAX_PROCS': SIMPLE_MAX_PROCS
        }
        complexity = assess_complexity(sas_code, complexity_thresholds)
        logger.info(f"Complexity assessed as '{complexity}'.")

        # --- Simple Path ---
        if complexity == "Simple":
            strategy_used = "Simple Direct"
            logger.info("Executing Simple Conversion Path...")
            try:
                pyspark_code, annotations, confidence = call_llm_for_conversion(
                    sas_code,
                    {"conversion_options": options}, # Pass options as context
                    is_simple_path=True
                )
                # Simple path doesn't remap annotations as lines are absolute
                final_annotations.extend(annotations)
                # Format the single block of code
                final_pyspark_code, format_warnings = basic_lint_check(pyspark_code)
                warnings.extend(format_warnings)
                logger.info("Simple Path conversion successful.")
            except Exception as e:
                logger.error(f"Simple Path failed: {e}", exc_info=True)
                warnings.append(f"Simple Path conversion failed: {e}")
                final_pyspark_code = f"# ERROR: Simple Path conversion failed: {e}\n# Original SAS:\n# {sas_code.replace(chr(10), chr(10)+'# ')}"


        # --- Complex Path ---
        else: # complexity == "Complex"
            strategy_used = "Complex Multi-Stage"
            logger.info("Executing Complex Conversion Path...")
            overall_context = {"conversion_options": options} # Initialize context with options
            final_pyspark_pieces = []
            cumulative_sas_line_offset = 0
            cumulative_pyspark_line_offset = 0 # Track PySpark lines for annotation remapping

            try:
                # 2. Split into Super-Chunks
                super_chunks = deterministic_split(sas_code)

                # 3. Process Super-Chunks Sequentially
                for i, super_chunk in enumerate(super_chunks):
                    logger.info(f"Processing Super-Chunk {i+1}/{len(super_chunks)}...")
                    super_chunk_start_line_abs = cumulative_sas_line_offset + 1
                    super_chunk_lines = super_chunk.splitlines()
                    pyspark_outputs_in_sc = {} # Track outputs within this super-chunk

                    # 3a. Structural Analysis (LLM)
                    logical_chunks = call_llm_for_structural_analysis(super_chunk)

                    # 3b. Process Logical Chunks Sequentially
                    for j, logical_chunk in enumerate(logical_chunks):
                        logger.info(f"  Processing Logical Chunk {j+1}/{len(logical_chunks)}: {logical_chunk.get('name')} ({logical_chunk.get('type')})")

                        # Extract code for this logical chunk
                        chunk_start_line_rel = logical_chunk.get('start_line', 1)
                        chunk_end_line_rel = logical_chunk.get('end_line', len(super_chunk_lines))
                        # Adjust for 0-based indexing and slice exclusivity
                        logical_chunk_code = "\n".join(super_chunk_lines[chunk_start_line_rel-1 : chunk_end_line_rel])

                        if not logical_chunk_code.strip():
                            logger.warning(f"  Logical chunk {logical_chunk.get('name')} is empty. Skipping.")
                            continue

                        # Calculate absolute SAS line numbers for this chunk
                        chunk_abs_start_line = super_chunk_start_line_abs + chunk_start_line_rel - 1
                        # chunk_abs_end_line = super_chunk_start_line_abs + chunk_end_line_rel - 1 # Less critical

                        # Prepare context (Simplified: pass overall context; TODO: implement focusing)
                        focused_context = overall_context

                        # 3c. Convert Chunk (LLM)
                        pyspark_code_chunk, annotations_chunk, confidence = call_llm_for_conversion(
                            logical_chunk_code,
                            focused_context,
                            is_simple_path=False,
                            chunk_info=logical_chunk
                        )

                        # 3d. Conditional Refinement (LLM)
                        refinement_attempts = 0
                        current_notes = None
                        while confidence < REFINEMENT_THRESHOLD and refinement_attempts < MAX_REFINEMENT_ATTEMPTS:
                            logger.warning(f"  Confidence ({confidence:.1f}) below threshold ({REFINEMENT_THRESHOLD}). Refining chunk...")
                            refinement_attempts += 1
                            pyspark_code_chunk, confidence, current_notes = call_llm_for_refinement(
                                pyspark_code_chunk,
                                focused_context,
                                previous_notes=current_notes
                            )
                            logger.info(f"  Refinement attempt {refinement_attempts} complete. New confidence: {confidence:.1f}")
                            if current_notes:
                                # Add refinement notes as an annotation
                                annotations_chunk.append({
                                    "sas_lines": [chunk_start_line_rel, chunk_end_line_rel], # Relative SAS lines
                                    "pyspark_lines": [1, len(pyspark_code_chunk.splitlines())], # Relative PySpark lines
                                    "note": f"Refinement Notes (Attempt {refinement_attempts}): {current_notes}",
                                    "severity": "Info"
                                })

                        # Calculate PySpark start line for remapping (based on *current* total lines)
                        current_pyspark_start_line = cumulative_pyspark_line_offset + 1

                        # 3e. Remap Annotation Lines
                        remapped_chunk_annotations = remap_annotations(
                            annotations_chunk,
                            chunk_abs_start_line,
                            current_pyspark_start_line
                        )
                        final_annotations.extend(remapped_chunk_annotations)

                        # 3f. Store result & Update PySpark line offset
                        final_pyspark_pieces.append(pyspark_code_chunk)
                        cumulative_pyspark_line_offset += len(pyspark_code_chunk.splitlines()) + 2 # Account for separators

                        # 3g. Update Overall Context
                        # Pass the *original* logical chunk info and the *final* (potentially refined) code
                        overall_context = update_overall_context(
                            overall_context,
                            logical_chunk,
                            pyspark_code_chunk,
                            annotations_chunk # Pass original annotations before remapping
                        )
                        # Store outputs for potential schema inference later
                        for output_name in logical_chunk.get('outputs', []):
                             pyspark_outputs_in_sc[output_name] = {"code_preview": pyspark_code_chunk[:200]}


                    # Update cumulative SAS line offset for the next super-chunk
                    cumulative_sas_line_offset += len(super_chunk_lines)
                    logger.info(f"Finished processing Super-Chunk {i+1}.")


                # 4. Final Assembly & Formatting
                logger.info("Assembling final code from pieces...")
                final_pyspark_code, format_warnings = assemble_and_format_code(final_pyspark_pieces)
                warnings.extend(format_warnings)
                logger.info("Complex Path conversion successful.")

            except Exception as e:
                logger.error(f"Complex Path failed: {e}", exc_info=True)
                warnings.append(f"Complex Path conversion failed: {e}")
                # Attempt to return partially converted code if available
                if final_pyspark_pieces:
                     partial_code, format_warnings = assemble_and_format_code(final_pyspark_pieces)
                     warnings.extend(format_warnings)
                     final_pyspark_code = f"# WARNING: Complex Path failed, returning partial result.\n# Error: {e}\n\n{partial_code}"
                else:
                     final_pyspark_code = f"# ERROR: Complex Path conversion failed entirely: {e}\n# Original SAS:\n# {sas_code.replace(chr(10), chr(10)+'# ')}"


        # --- Response Preparation ---
        end_invocation_time = time.time()
        processing_stats = {
            "total_duration_seconds": round(end_invocation_time - start_invocation_time, 2),
            "llm_calls": token_usage_stats['api_calls'],
            "token_usage": token_usage_stats,
            "llm_call_details": llm_call_counts, # Include breakdown
            "complexity_assessment": complexity,
            "sas_code_lines": sas_code.count('\n') + 1,
            "generated_pyspark_lines": final_pyspark_code.count('\n') + 1
        }

        logger.info(f"Invocation finished. Duration: {processing_stats['total_duration_seconds']:.2f}s. LLM Calls: {processing_stats['llm_calls']}.")

        # Construct final JSON output
        final_output = {
            "status": "success" if not any(w.startswith("ERROR:") or "failed" in w for w in warnings) else "warning",
            "pyspark_code": final_pyspark_code,
            "annotations": final_annotations,
            "warnings": warnings,
            "strategy_used": strategy_used,
            "processing_stats": processing_stats,
            "processing_complete": True # Indicate completion
        }

        # Return response
        return {
            'statusCode': 200,
            'headers': response_headers,
            'body': json.dumps(final_output, ensure_ascii=False, default=str) # Use default=str for safety
        }

    except Exception as e:
        logger.error(f"Unhandled exception in lambda_handler: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': response_headers,
            'body': json.dumps({
                'status': 'error',
                'message': f"Internal server error: {e}",
                'details': str(e)
            }, ensure_ascii=False)
        }

# --- Test Handler ---
def test_handler(sas_code, options=None):
    """ Utility for testing the lambda function locally or from console """
    test_event = {
        "body": json.dumps({
            "sas_code": sas_code,
            "options": options or {}
        })
    }
    # Mock context object
    class MockContext:
        def get_remaining_time_in_millis(self):
            return 300000 # 5 minutes

    result = lambda_handler(test_event, MockContext())
    # Pretty print the JSON body
    try:
        body_json = json.loads(result['body'])
        print(json.dumps(body_json, indent=2))
    except:
        print(result['body']) # Print raw if not JSON
    return result

# Example usage for local testing (if run directly)
if __name__ == '__main__':
    # Ensure Bedrock client is initialized for local tests if needed
    # Note: Local testing might require AWS credentials configured
    if not USE_BEDROCK:
         print("Warning: Bedrock client not initialized. Running in demo mode.")

    print("Running local test with Simple SAS code...")
    test_sas_simple = """
    DATA work.output;
        SET work.input;
        new_var = old_var * 1.1;
        IF category = 'X' THEN delete;
    RUN;

    PROC SORT DATA=work.output OUT=work.sorted;
        BY id;
    RUN;
    """
    test_handler(test_sas_simpleimport json
import re
import logging
import time # Add for retry delay
import boto3 # Import boto3 for AWS SDK
import os # To potentially get model ID from environment variables
import hashlib
from functools import lru_cache
from botocore.config import Config # Import Config from botocore.config
# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Bedrock Client Initialization ---
# Get AWS region from environment variable or use default
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
# Create a custom configuration with extended timeout
boto_config = Config(read_timeout=3600)

# Initialize with error handling
try:
    bedrock_runtime = boto3.client('bedrock-runtime', region_name=AWS_REGION, config=boto_config)
    # Test Bedrock access by retrieving model list
    USE_BEDROCK = True
    logger.info("Successfully initialized Bedrock client")
except Exception as e:
    logger.warning(f"Failed to initialize Bedrock client: {e}. Will run in demo/test mode only.")
    bedrock_runtime = None
    USE_BEDROCK = False

# Define the model ID from environment variables with fallback to Claude 3.7 Sonnet
# Note: Claude 3.7 Sonnet requires using an inference profile ID
# Example: "us.anthropic.claude-3-7-sonnet-20250219-v1:0" 
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-3-7-sonnet-20250219-v1:0")
ANTHROPIC_VERSION = "bedrock-2023-05-31"

# Configurable parameters
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.environ.get("RETRY_DELAY_SECONDS", "2"))
RETRY_BACKOFF_FACTOR = float(os.environ.get("RETRY_BACKOFF_FACTOR", "2.0"))  # Exponential backoff factor
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "65536"))  # Default to 64K tokens (Claude 3.7 supports up to 128K)
MAX_EXTREME_TOKENS = int(os.environ.get("MAX_EXTREME_TOKENS", "120000"))  # Maximum 120K tokens for extreme cases
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "4.0"))
MAX_REFINEMENT_ATTEMPTS = int(os.environ.get("MAX_REFINEMENT_ATTEMPTS", "2"))
CONTEXT_SIZE_LIMIT = int(os.environ.get("CONTEXT_SIZE_LIMIT", "200000"))  # Increased context limit
VERY_LARGE_CODE_THRESHOLD = int(os.environ.get("VERY_LARGE_CODE_THRESHOLD", "10000"))
EXTREME_CODE_THRESHOLD = int(os.environ.get("EXTREME_CODE_THRESHOLD", "50000"))  # Threshold for extreme mode
USE_EXTENDED_THINKING = os.environ.get("USE_EXTENDED_THINKING", "TRUE").upper() == "TRUE"

# Define error handling retry statuses for Bedrock
BEDROCK_RETRY_EXCEPTIONS = [
    'ThrottlingException',
    'ServiceUnavailableException',
    'InternalServerException',
    'ModelTimeoutException'
]

# Initialize token usage statistics
token_usage_stats = {
    'total_tokens': 0,
    'input_tokens': 0,
    'output_tokens': 0,
    'api_calls': 0
}

# Initialize cache for LLM responses
llm_response_cache = {}
USE_LLM_CACHING = os.environ.get("USE_LLM_CACHING", "TRUE").upper() == "TRUE"
MAX_CACHE_ENTRIES = int(os.environ.get("MAX_CACHE_ENTRIES", "100"))

def cache_key(prompt, model_id=None):
    """Generate a cache key from the prompt and optional model ID"""
    key_content = f"{prompt}:{model_id or BEDROCK_MODEL_ID}"
    return hashlib.md5(key_content.encode('utf-8')).hexdigest()

def _call_llm_with_retry(bedrock_invoke_function, prompt, description="LLM call", bypass_cache=False):
    """
    Wrapper for retry logic specifically around Bedrock invoke_model calls.
    Now includes caching to avoid repeated identical calls and exponential backoff for retries.
    
    :param bedrock_invoke_function: The function that performs the bedrock_runtime.invoke_model call.
    :param prompt: The prompt to send to the model.
    :param description: A description of the LLM call for logging.
    :param bypass_cache: If True, always call the LLM even if result is cached.
    :return: The parsed response from the LLM.
    """
    # Check cache first if enabled
    if USE_LLM_CACHING and not bypass_cache:
        key = cache_key(prompt)
        if key in llm_response_cache:
            logger.info(f"Cache hit for {description}! Using cached response.")
            return llm_response_cache[key]
    
    # Not in cache or bypass_cache is True, proceed with API call
    for attempt in range(MAX_RETRIES + 1):
        try:
            logger.info(f"Attempting {description} (Attempt {attempt + 1}/{MAX_RETRIES + 1})...")
            result = bedrock_invoke_function(prompt)
            
            # Store in cache if caching is enabled
            if USE_LLM_CACHING and not bypass_cache:
                key = cache_key(prompt)
                llm_response_cache[key] = result
                
                # Manage cache size
                if len(llm_response_cache) > MAX_CACHE_ENTRIES:
                    # Simple strategy: remove a random entry when full
                    import random
                    random_key = random.choice(list(llm_response_cache.keys()))
                    del llm_response_cache[random_key]
                    logger.info(f"Cache full, removed random entry to make space.")
            
            return result # Success
        except Exception as e:
            error_name = type(e).__name__
            logger.warning(f"{description} attempt {attempt + 1} failed: {e}")
            
            # Check if this is a retryable exception
            if attempt < MAX_RETRIES and (error_name in BEDROCK_RETRY_EXCEPTIONS or 'ThrottlingException' in str(e)):
                # Calculate delay with exponential backoff
                delay = RETRY_DELAY_SECONDS * (RETRY_BACKOFF_FACTOR ** attempt)
                logger.info(f"Retrying in {delay:.1f} seconds with exponential backoff...")
                time.sleep(delay)
            else:
                logger.error(f"{description} failed after {attempt + 1} attempts.")
                raise # Re-raise the exception after final attempt

# --- Specific LLM Interaction Functions using Bedrock ---

def _invoke_claude37_sonnet(prompt, message_history=None):
    """
    Invokes the Claude 3.7 Sonnet model on Bedrock.
    Handles the specific request/response format for Anthropic models.
    
    Parameters:
    - prompt: The prompt to send to the model
    - message_history: Optional previous messages for context
    
    Returns:
    - The text response from the model
    """
    # Check if Bedrock is available
    if not USE_BEDROCK or bedrock_runtime is None:
        logger.warning("Bedrock is not available. Returning demo response.")
        # Return a simple demo response to allow testing without Bedrock
        return """{"pyspark_code": "# Demo mode - Bedrock not available\\n\\nfrom pyspark.sql.functions import col, lit\\n\\n# This is a demo conversion since Bedrock is not available\\ndef demo_function():\\n    print('This is a demo conversion')\\n    \\n# Simulated dataframe operations\\noutput_df = input_df.filter(col('category') == 'A')\\noutput_df = output_df.withColumn('new_var', col('old_var') * 2)\\n",
        "annotations": [{"sas_lines": [1, 5], "pyspark_lines": [1, 10], "note": "Demo conversion - Bedrock is not available", "severity": "Info"}]}"""
    
    # Set up messages format
    if message_history:
        messages = message_history
        # Add the new user message
        messages.append({"role": "user", "content": prompt})
    else:
        messages = [{"role": "user", "content": prompt}]
    
    # Use strategy-specific token limit if available, otherwise use global default
    token_limit = MAX_TOKENS_PER_STRATEGY if 'MAX_TOKENS_PER_STRATEGY' in globals() else MAX_TOKENS
    
    # Configure request body with Claude 3.7 specific parameters
    body = {
        "anthropic_version": ANTHROPIC_VERSION,
        "max_tokens": token_limit,
        "messages": messages,
        "temperature": float(os.environ.get("LLM_TEMPERATURE", "0.7")),
        "top_p": float(os.environ.get("LLM_TOP_P", "0.9")),
    }

    try:
        logger.info(f"Invoking Claude 3.7 Sonnet with max_tokens={token_limit}")
        start_time = time.time()
        try:
            response = bedrock_runtime.invoke_model(
                body=json.dumps(body),
                modelId=BEDROCK_MODEL_ID,
                accept='application/json',
                contentType='application/json'
            )
        except Exception as invoke_error:
            error_msg = str(invoke_error)
            if "ValidationException" in error_msg and "on-demand throughput isn't supported" in error_msg:
                logger.error(f"Model error: Claude 3.7 Sonnet requires using an inference profile ID.")
                logger.error(f"Use the inference profile ID: 'us.anthropic.claude-3-7-sonnet-20250219-v1:0'")
                logger.error(f"Set this in the BEDROCK_MODEL_ID environment variable or update the default value in the code.")
            raise invoke_error
            
        response_time = time.time() - start_time
        response_body = json.loads(response['body'].read())
        
        # Track token usage for monitoring and billing
        input_tokens = response_body.get("usage", {}).get("input_tokens", 0)
        output_tokens = response_body.get("usage", {}).get("output_tokens", 0)
        total_tokens = input_tokens + output_tokens
        
        logger.info(f"Token usage: {input_tokens} input + {output_tokens} output = {total_tokens} total tokens")
        logger.info(f"Response time: {response_time:.2f}s")
        
        # Update global token usage stats if they exist
        if 'token_usage_stats' in globals():
            token_usage_stats['total_tokens'] += total_tokens
            token_usage_stats['input_tokens'] += input_tokens
            token_usage_stats['output_tokens'] += output_tokens
            token_usage_stats['api_calls'] += 1

        # Extract content from the response (specific to Claude 3.7 Messages API)
        if response_body.get("type") == "message" and response_body.get("content"):
            # Assuming the primary response is in the first content block
            llm_output = response_body["content"][0]["text"]
            return llm_output
        else:
            logger.error(f"Unexpected Bedrock response format: {response_body}")
            raise ValueError("Unexpected response format from Bedrock Claude 3.7")

    except Exception as e:
        logger.error(f"Bedrock invoke_model failed: {e}", exc_info=True)
        raise # Re-raise to be caught by retry logic or handler

def call_llm_for_strategy(sas_sample):
    """Calls Bedrock Claude 3.7 Sonnet to determine the conversion strategy."""
    prompt = f"""Analyze the complexity of the provided SAS code sample:
```sas
{sas_sample}
```
Consider macro usage intensity, nesting depth, data step complexity (implicit loops, merges, arrays), PROC types used, and apparent dependencies.
Recommend one strategy based on your analysis:
- 'Simple Direct': For very straightforward code with minimal dependencies or macros.
- 'Standard Multi-Pass': For moderately complex code with standard procedures and some dependencies.
- 'Deep Macro Analysis': For code heavily reliant on complex macros or intricate dependencies requiring deeper analysis.

Explain your reasoning briefly. Output ONLY the recommended strategy string (e.g., 'Standard Multi-Pass')."""

    def invoke_strategy(p):
        response_text = _invoke_claude37_sonnet(p)
        # Basic validation - check if response is one of the expected strategies
        strategies = ['Simple Direct', 'Standard Multi-Pass', 'Deep Macro Analysis']
        for strategy in strategies:
            if strategy in response_text:
                strategy_name = strategy
                logger.info(f"Bedrock recommended strategy: {strategy_name}")
                return strategy_name
                
        logger.warning(f"LLM returned unexpected strategy format: '{response_text}'. Defaulting to 'Standard Multi-Pass'.")
        # Fallback to standard approach
        return 'Standard Multi-Pass'

    return _call_llm_with_retry(invoke_strategy, prompt, description="LLM Strategy Advice")


# --- Structural Analysis ---
def call_llm_for_structural_analysis(super_chunk):
    """Calls Bedrock Claude 3.7 Sonnet to identify logical chunks in a super-chunk."""
    # For very small chunks, use a simplified approach
    if len(super_chunk.splitlines()) < 50:
        logger.info("Chunk is very small, using simplified structural analysis")
        # Create a basic logical chunk for the entire content
        simple_chunk = {
            "type": "UNKNOWN",  # Will be resolved during conversion
            "name": "small_code_chunk",
            "start_line": 1,
            "end_line": len(super_chunk.splitlines()),
            "inputs": [],  # Empty inputs initially, will be inferred during conversion
            "outputs": []  # Empty outputs initially, will be inferred during conversion
        }
        return [simple_chunk]

    prompt = f"""Analyze the following SAS code block. Identify all distinct logical units (DATA steps, PROC steps, defined %MACROs).
Output ONLY a valid JSON list where each item represents a unit and includes:
{{
  "type": "DATA" | "PROC" | "MACRO",
  "name": "step/proc/macro_name", // Best guess for the name
  "start_line": <integer>, // Starting line number (1-based) within this super_chunk
  "end_line": <integer>, // Ending line number (1-based) within this super_chunk
  "inputs": ["dataset_or_macro_used"], // List of likely input dataset/macro names
  "outputs": ["dataset_or_macro_created"] // List of likely output dataset/macro names
}}
Ensure the output is ONLY the JSON list, starting with '[' and ending with ']'.

SAS Code Block:
```sas
{super_chunk}
```"""

    def invoke_structural_analysis(p):
        response_text = _invoke_claude37_sonnet(p)
        try:
            # Attempt to parse the response as JSON
            # Clean potential markdown code fences if LLM includes them
            cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
            logical_chunks = json.loads(cleaned_response)
            if isinstance(logical_chunks, list):
                logger.info(f"Bedrock identified {len(logical_chunks)} logical chunks.")
                # Basic validation of structure could be added here
                
                # If the LLM returned an empty list, create a default chunk for the entire code
                if len(logical_chunks) == 0:
                    logger.warning("LLM returned empty chunk list, creating default chunk")
                    default_chunk = {
                        "type": "UNKNOWN",
                        "name": "default_chunk",
                        "start_line": 1,
                        "end_line": len(super_chunk.splitlines()),
                        "inputs": [],
                        "outputs": []
                    }
                    return [default_chunk]
                    
                return logical_chunks
            else:
                logger.error(f"LLM structural analysis returned valid JSON but not a list: {cleaned_response}")
                raise ValueError("LLM response for structural analysis was not a JSON list.")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON response for structural analysis: {e}\nResponse text: {response_text}")
            raise ValueError(f"LLM response was not valid JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing structural analysis response: {e}\nResponse text: {response_text}")
            raise # Re-raise other unexpected errors

    return _call_llm_with_retry(invoke_structural_analysis, prompt, description="LLM Structural Analysis")


# --- Context Focusing ---
def call_llm_for_context_focus(overall_context, logical_chunk_code):
    """Calls Bedrock Claude 3.7 Sonnet to extract focused context for a logical chunk."""
    # Convert overall_context to JSON string for the prompt
    try:
        overall_context_json = json.dumps(overall_context, indent=2)
    except TypeError as e:
        logger.error(f"Could not serialize overall_context to JSON: {e}")
        # Handle error appropriately - maybe send an empty context or raise
        overall_context_json = "{}"

    prompt = f"""Review the Overall Context (JSON below) and the upcoming SAS code snippet.
Extract and return ONLY the minimal subset of the Overall Context (as valid JSON) that is essential for accurately converting the upcoming SAS code snippet.
Include relevant dataset schemas, macro definitions, and key variable states mentioned or likely required by the SAS code.
If no specific context is needed, return an empty JSON object {{}}.

Overall Context:
```json
{overall_context_json}
```

Upcoming SAS Code Snippet:
```sas
{logical_chunk_code}
```

Output ONLY the focused context as a valid JSON object."""

    def invoke_context_focus(p):
        response_text = _invoke_claude37_sonnet(p)
        try:
            # Clean potential markdown code fences
            cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
            focused_context = json.loads(cleaned_response)
            if isinstance(focused_context, dict):
                logger.info("Bedrock returned focused context.")
                return focused_context
            else:
                logger.error(f"LLM context focus returned valid JSON but not an object: {cleaned_response}")
                raise ValueError("LLM response for context focus was not a JSON object.")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON response for context focus: {e}\nResponse text: {response_text}")
            # Decide on fallback: return empty context or raise error
            logger.warning("Falling back to empty focused context due to JSON decode error.")
            return {}
        except Exception as e:
            logger.error(f"Error processing context focus response: {e}\nResponse text: {response_text}")
            raise # Re-raise other unexpected errors

    return _call_llm_with_retry(invoke_context_focus, prompt, description="LLM Context Focusing")


# --- Conversion & Annotation ---
def call_llm_for_conversion(logical_chunk_code, focused_context, chunk_type=None, strategy=None):
    """
    Calls Bedrock Claude 3.7 Sonnet to convert SAS chunk and generate annotations.
    Uses enhanced prompting for reasoned, accurate conversions.
    
    Parameters:
    - logical_chunk_code: The SAS code to convert
    - focused_context: The focused context for this chunk
    - chunk_type: The type of chunk (DATA, PROC, MACRO)
    - strategy: The conversion strategy to use
    
    Returns:
    - pyspark_code: The converted PySpark code
    - annotations: List of annotations with explanations
    """
    try:
        focused_context_json = json.dumps(focused_context, indent=2)
    except TypeError as e:
        logger.error(f"Could not serialize focused_context to JSON for conversion: {e}")
        focused_context_json = "{}"  # Send empty context if serialization fails

    # Customize prompt based on chunk type and strategy
    chunk_specific_guidance = ""
    if chunk_type == "DATA":
        chunk_specific_guidance = """
For DATA steps, follow these specific conversion patterns:
- SAS DATA step becomes a sequence of PySpark DataFrame operations
- SAS SET statement becomes DataFrame read or reference: `df = spark.table("table_name")` or variable reference
- SAS WHERE clause becomes DataFrame.filter(): `df = df.filter("condition")`
- SAS assignment statements become DataFrame.withColumn(): `df = df.withColumn("new_col", expr("calculation"))`
- SAS IF/THEN/ELSE becomes when().otherwise(): `df = df.withColumn("new_col", when(condition, then_value).otherwise(else_value))`
- SAS BY-group processing requires Window functions: `Window.partitionBy("col1").orderBy("col2")`
- SAS implicit loops require careful handling - may use join, crossJoin, or explode() as appropriate
"""
    elif chunk_type == "PROC":
        chunk_specific_guidance = """
For PROC steps, use these exact mappings:
- PROC SORT: `df = df.orderBy("col1", "col2")` or `df = df.orderBy(col("col1").desc())`
- PROC SQL: transform to either DataFrame operations or spark.sql()
  - SELECT becomes df.select()
  - WHERE becomes df.filter()
  - GROUP BY becomes df.groupBy().agg()
  - JOIN becomes df.join()
  - ORDER BY becomes df.orderBy()
- PROC MEANS/SUMMARY: `df.groupBy("group_cols").agg(mean("value_col"), sum("value_col"))`
- PROC TRANSPOSE: Based on specific case, may use pivot/unpivot or more complex reshaping logic
- PROC PRINT: Can be approximated with `df.show()` or similar display functions
"""
    elif chunk_type == "MACRO":
        chunk_specific_guidance = """
For MACRO definitions, implement as Python functions with exact parameter mapping:
- SAS %MACRO name(param1, param2) becomes `def name(param1, param2):`
- SAS macro variables &var become function parameters
- SAS %GLOBAL variables become function parameters or globals in specific cases
- SAS macro conditional logic (%IF/%THEN/%ELSE) becomes Python if/else
- SAS %DO loops become Python for/while loops
- Add docstring explaining the macro's purpose and parameters
"""
    
    # Add strategy-specific guidance with concrete examples
    strategy_guidance = ""
    if strategy == "Deep Macro Analysis":
        strategy_guidance = """
For Deep Macro Analysis, follow this systematic approach:
1. Identify all macro references and their dependencies
2. Map each macro parameter to its usage pattern
3. For macro calls like %macro_name(param1=value1), create Python function calls: `macro_name(param1=value1)`
4. For macro variable references like &variable, use the corresponding Python variable
5. For complex macro expansions, trace through the actual logic flow step by step
"""
    elif strategy == "Simple Direct":
        strategy_guidance = """
For Simple Direct conversion, prioritize readability:
1. Maintain a direct 1:1 mapping between SAS and PySpark operations where possible
2. Choose the most straightforward PySpark equivalent for each SAS operation
3. Use descriptive variable names that match the original SAS code
4. Avoid overly complex optimizations that might obscure the original logic
"""
    
    # Code formatting instructions
    code_formatting_guidance = """
Ensure proper Python formatting in your generated code:
- Use consistent 4-space indentation
- Follow PEP 8 style guidelines
- Use meaningful variable names that match the intent of the original SAS variables
- Add appropriate blank lines between logical sections
- Use proper method chaining for PySpark operations (with each method on a new line)
- Include type hints where appropriate
- Make sure all imports are at the top of the file
- For multi-line statements, ensure proper line breaks and alignment
- Ensure proper brackets/parentheses closure
"""

    # Add detailed comments if requested by the user
    comment_guidance = ""
    if focused_context.get("conversion_options", {}).get("add_detailed_comments", True):
        comment_guidance = """
Add detailed comments explaining:
- The original SAS logic being converted (with direct references to SAS statements by line number)
- Any assumptions made during conversion
- Complex transformations and why they were chosen
- Potential performance considerations

Format your comments clearly:
```python
# Original SAS: DATA step creates new dataset with filtering
# PySpark equivalent: Create DataFrame with filter operation
```
"""

    # Check for immediate optimizations option
    optimization_guidance = ""
    if focused_context.get("conversion_options", {}).get("apply_immediate_optimizations", True):
        optimization_guidance = """
Apply these specific PySpark optimizations:
- Use broadcast joins for small-large table joins: `df1.join(broadcast(df2), "key")`
- Cache DataFrames appropriately: `df_frequent = df.cache()` for reused DataFrames
- Apply column pruning: `df.select("only_needed_columns")`
- Combine multiple filters: `df.filter("condition1 AND condition2")` instead of sequential filters
- Use built-in functions instead of UDFs: `df.withColumn("new_col", expr("FUNCTION(col)"))` 
- For aggregations, use efficient built-in functions: `df.groupBy().agg(sum("col"), avg("col"))`
"""

    # Reasoning and anti-hallucination instructions
    reasoning_guidance = """
Follow this exact conversion process to prevent hallucination:
1. First, carefully parse and understand each SAS statement's purpose
2. Map each SAS feature to a specific, known PySpark equivalent
3. ONLY use documented PySpark functions and methods that actually exist
4. If you're uncertain about a conversion, flag it in an annotation with severity "Warning"
5. If a direct mapping is unclear, present multiple alternatives as annotations
6. Be explicit about any assumptions or ambiguities in the SAS code
7. Double-check variable references to ensure they exist in the appropriate scope
8. If a SAS feature has no direct equivalent, create a detailed annotation explaining the limitation

Importantly:
- Do NOT invent non-existent PySpark functions or syntax
- If unsure of exact syntax, provide a functional equivalent with an annotation
- Explicitly indicate any limitations or edge cases in annotations
"""

    # Data type handling instructions
    datatype_guidance = """
Use these specific data type mappings:
- SAS character -> StringType()
- SAS numeric -> DoubleType() for floating point, IntegerType()/LongType() for integers
- SAS date -> DateType()
- SAS datetime -> TimestampType()
- SAS formats -> Use appropriate PySpark functions like date_format(), format_number(), etc.

Handle missing values correctly:
- SAS missing numeric (.) -> PySpark null
- SAS missing character ('') -> PySpark null or empty string depending on context
"""

    # Comprehensive examples for common patterns
    examples_guidance = """
Here are concrete examples of common SAS to PySpark conversion patterns:

1. SAS DATA step with WHERE:
```sas
DATA output;
  SET input;
  WHERE age > 25 AND state = 'CA';
  new_var = var1 * 2;
RUN;
```

PySpark equivalent:
```python
from pyspark.sql.functions import col, lit

# Filter and transform data
output_df = input_df.filter((col("age") > 25) & (col("state") == "CA"))
output_df = output_df.withColumn("new_var", col("var1") * 2)
```

2. SAS PROC SQL:
```sas
PROC SQL;
  CREATE TABLE output AS
  SELECT a.id, a.name, b.value
  FROM table1 a
  LEFT JOIN table2 b
  ON a.id = b.id
  WHERE a.date > '01JAN2020'd
  ORDER BY a.id;
QUIT;
```

PySpark equivalent:
```python
from pyspark.sql.functions import col
from datetime import datetime

# Define date for comparison
date_threshold = datetime.strptime("2020-01-01", "%Y-%m-%d")

# Create SQL join with ordering
output_df = table1_df.alias("a").join(
    table2_df.alias("b"),
    col("a.id") == col("b.id"),
    "left"
).filter(col("a.date") > lit(date_threshold)) \
 .select(col("a.id"), col("a.name"), col("b.value")) \
 .orderBy("a.id")
```

3. SAS Macro:
```sas
%MACRO process_data(input_ds, filter_var, filter_val);
  DATA output;
    SET &input_ds;
    WHERE &filter_var > &filter_val;
  RUN;
%MEND process_data;
```

PySpark equivalent:
```python
def process_data(input_df, filter_var, filter_val):
    '''
    Process data by filtering on the specified variable and value
    
    Args:
        input_df: Input DataFrame
        filter_var: Column name to filter on
        filter_val: Value to compare against
        
    Returns:
        Filtered DataFrame
    '''
    return input_df.filter(col(filter_var) > filter_val)
```
"""

    prompt = f"""As an expert SAS and PySpark developer, I need you to convert the following SAS code into equivalent PySpark code. 

The SAS code appears to be a {chunk_type if chunk_type else "code segment"}.

Here is the focused context to consider during conversion:
```json
{focused_context_json}
```

{chunk_specific_guidance}

{strategy_guidance}

{reasoning_guidance}

{code_formatting_guidance}

{datatype_guidance}

{comment_guidance}

{optimization_guidance}

{examples_guidance}

Convert the code with these principles:
- Maintain semantic equivalence to the original SAS code
- Use the most appropriate PySpark methods and functions
- Ensure the resulting code is syntactically correct
- Preserve the original processing logic and data flow
- Handle SAS-specific features with their closest PySpark equivalents
- Consider the focused context when making conversion decisions
- Be explicit about any limitations or assumptions in annotations

Output ONLY a valid JSON object with the following structure:
{{
  "pyspark_code": "...",
  "annotations": [
    {{
      "sas_lines": [<start_line_int>, <end_line_int>], 
      "pyspark_lines": [<start_line_int>, <end_line_int>], 
      "note": "Explanation for this conversion, including any assumptions or limitations",
      "severity": "Info" | "Warning" 
    }}
  ]
}}

The SAS code to convert is:
```sas
{logical_chunk_code}
```"""

    def invoke_conversion(p):
        response_text = _invoke_claude37_sonnet(p)
        try:
            # First, clean up any common issues that could break JSON parsing
            clean_response = response_text
            
            # Find and fix triple quotes (""") which break JSON
            clean_response = re.sub(r'"""(.*?)"""', r'"\1"', clean_response, flags=re.DOTALL)
            
            # Replace any control characters that break JSON parsing
            control_chars = [chr(x) for x in range(32) if x not in [9, 10, 13]]  # Tab, LF, CR are ok
            for char in control_chars:
                clean_response = clean_response.replace(char, '')
            
            # Clean potential markdown code fences
            cleaned_response = re.sub(r'^```json\s*|\s*```$', '', clean_response, flags=re.MULTILINE).strip()
            
            # Check if the model has wrapped JSON in an explanation (common issue)
            json_match = re.search(r'({[\s\S]*?"pyspark_code"[\s\S]*?"annotations"[\s\S]*?})', cleaned_response)
            if json_match:
                # Extract just the JSON object from the text
                potential_json = json_match.group(1)
                logger.info("Extracted potential JSON object from response text")
                try:
                    # Try parsing the extracted JSON
                    result = json.loads(potential_json)
                    if 'pyspark_code' in result and 'annotations' in result:
                        logger.info("Successfully extracted valid JSON from response text")
                        cleaned_response = potential_json
                except:
                    # If extraction fails, continue with the original cleaned response
                    logger.warning("Extracted text was not valid JSON, continuing with original response")
            
            # Try to parse the JSON response
            try:
                result = json.loads(cleaned_response)
            except json.JSONDecodeError as json_err:
                # Handle common cases where the model returns multiple JSON objects
                # in the same response or has extra text around the JSON
                try:
                    # Try to find any JSON objects in the response
                    potential_jsons = re.findall(r'({[\s\S]*?"pyspark_code"[\s\S]*?"annotations"[\s\S]*?})', cleaned_response)
                    if potential_jsons:
                        # Try each potential JSON match
                        for json_str in potential_jsons:
                            try:
                                result = json.loads(json_str)
                                if 'pyspark_code' in result and 'annotations' in result:
                                    logger.info("Found valid JSON object within response")
                                    break
                            except:
                                continue
                        else:
                            # If we get here, none of the matches worked
                            raise json_err
                    else:
                        raise json_err
                except:
                    # If all extraction attempts fail, re-raise the original error
                    raise json_err

            # Validate structure
            if isinstance(result, dict) and \
               'pyspark_code' in result and isinstance(result['pyspark_code'], str) and \
               'annotations' in result and isinstance(result['annotations'], list):
                # Clean the pyspark_code in case it contains triple quotes
                if '"""' in result['pyspark_code']:
                    # Replace triple quotes with regular quotes in the code, preserving multiline strings
                    code_content = result['pyspark_code'].strip('"""')
                    result['pyspark_code'] = code_content
                
                # Further validate annotations structure
                valid_annotations = []
                for ann in result['annotations']:
                    if isinstance(ann, dict) and 'sas_lines' in ann and 'note' in ann:
                        valid_annotations.append(ann)
                    else:
                        logger.warning(f"Invalid annotation structure: {ann}")
                
                # Replace with validated annotations
                result['annotations'] = valid_annotations
                logger.info(f"Bedrock returned converted PySpark ({len(result['pyspark_code'])} chars) and {len(valid_annotations)} annotations.")
                
                # Check for potential hallucinations in the code
                hallucination_indicators = [
                    (r'spark\.\w+\(', 'Potentially undefined Spark functions'),
                    (r'import\s+\w+\s+as\s+\w+\s+from', 'Invalid Python import syntax'),
                    (r'df\.[a-zA-Z]+\s+\(', 'Incorrect method call syntax'),
                    (r'pyspark\.sql\.functions\.[a-zA-Z]+\([^)]*\)\.[a-zA-Z]+\(', 'Incorrect function chaining')
                ]
                
                for pattern, message in hallucination_indicators:
                    if re.search(pattern, result['pyspark_code']):
                        warning_annotation = {
                            'sas_lines': [1, 1],
                            'pyspark_lines': [1, 1],
                            'note': f"Potential code issue detected: {message}. Please verify this code carefully.",
                            'severity': 'Warning'
                        }
                        result['annotations'].append(warning_annotation)
                        logger.warning(f"Potential hallucination detected: {message}")
                
                return result['pyspark_code'], result['annotations']
            else:
                logger.error(f"LLM conversion response has invalid structure: {cleaned_response}")
                raise ValueError("LLM response for conversion has invalid structure.")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON response for conversion: {e}\nResponse text: {response_text}")
            
            # Try one last approach - return everything after the last code fence if it exists
            code_fences = re.findall(r'```(?:python|json)?([^`]+)```', response_text)
            if code_fences:
                last_code = code_fences[-1].strip()
                logger.info(f"Extracted code from the last code fence as fallback")
                error_pyspark = f"# Extracted from LLM response (JSON parsing failed)\n\n{last_code}"
                error_annotation = [{'sas_lines': [1,1], 'pyspark_lines': [1,1], 'note': f'Extracted code from response as fallback (JSON parsing failed)', 'severity': 'Warning'}]
                return error_pyspark, error_annotation
            
            # Fallback: Return error placeholder and an annotation
            error_pyspark = f"# ERROR: Failed to parse LLM conversion response.\n# Original SAS code preserved as comment for reference:\n# {logical_chunk_code.replace(chr(10), chr(10)+'# ')}"
            error_annotation = [{'sas_lines': [1,1], 'pyspark_lines': [1,1], 'note': f'LLM response was not valid JSON: {e}', 'severity': 'Warning'}]
            return error_pyspark, error_annotation
        except Exception as e:
            logger.error(f"Error processing conversion response: {e}\nResponse text: {response_text}")
            # Fallback: Return error placeholder and an annotation
            error_pyspark = f"# ERROR: Exception processing conversion response.\n# Original SAS code preserved as comment for reference:\n# {logical_chunk_code.replace(chr(10), chr(10)+'# ')}"
            error_annotation = [{'sas_lines': [1,1], 'pyspark_lines': [1,1], 'note': f'Exception during conversion: {e}', 'severity': 'Warning'}]
            return error_pyspark, error_annotation

    # Call the LLM with retry
    return _call_llm_with_retry(invoke_conversion, prompt, description="LLM Conversion & Annotation")


# --- Refinement ---
def call_llm_for_refinement(pyspark_section, refinement_notes=None):
    """Calls Bedrock Claude 3.7 Sonnet to refine PySpark code and get confidence."""

    initial_prompt_instruction = """Review and refine the following PySpark code section for correctness against SAS semantics, Spark efficiency (avoid anti-patterns like unnecessary collects, use broadcast joins where appropriate), and Python style (PEP 8). Add/improve comments where clarity is needed.
Provide an overall confidence score (integer 1-5, where 1=Very Low, 5=Very High) for this section's conversion quality.
List any specific areas of uncertainty or suggestions for further improvement as refinement notes.

Output ONLY a valid JSON object with the following structure:
{{
  "refined_code": "...",
  "confidence_score": <integer 1-5>,
  "refinement_notes": "..." // String containing notes, or empty string if none
}}
Ensure the output is ONLY the JSON object, starting with '{{' and ending with '}}'.
"""

    follow_up_prompt_instruction = f"""Confidence was low in the previous review. Please focus on refining the specific areas mentioned in the previous notes below, in addition to general review.
Previous Refinement Notes:
'''
{refinement_notes}
'''

Review and refine the following PySpark code section again, paying close attention to the previous notes.
Provide an updated overall confidence score (integer 1-5) and any *new* or *remaining* refinement notes.

Output ONLY a valid JSON object with the same structure as before:
{{
  "refined_code": "...",
  "confidence_score": <integer 1-5>,
  "refinement_notes": "..."
}}
Ensure the output is ONLY the JSON object, starting with '{{' and ending with '}}'.
"""

    prompt = f"""{(follow_up_prompt_instruction if refinement_notes else initial_prompt_instruction)}

PySpark Code Section to Refine:
```python
{pyspark_section}
```"""

    def invoke_refinement(p):
        response_text = _invoke_claude37_sonnet(p)
        try:
            # Fix common JSON issues
            clean_response = response_text
            
            # Find and fix triple quotes (""") which break JSON
            clean_response = re.sub(r'"""(.*?)"""', r'"\1"', clean_response, flags=re.DOTALL)
            
            # Remove control characters that break JSON parsing
            control_chars = [chr(x) for x in range(32) if x not in [9, 10, 13]]  # Tab, LF, CR are ok
            for char in control_chars:
                clean_response = clean_response.replace(char, '')
            
            # Clean potential markdown code fences
            cleaned_response = re.sub(r'^```json\s*|\s*```$', '', clean_response, flags=re.MULTILINE).strip()
            
            # Remove any extra curly braces ({{ }}) which are common errors
            if cleaned_response.startswith("{{"):
                cleaned_response = cleaned_response[1:]
            if cleaned_response.endswith("}}"):
                cleaned_response = cleaned_response[:-1]
            
            # Check if the model has wrapped JSON in an explanation
            json_match = re.search(r'({[\s\S]*?"refined_code"[\s\S]*?"confidence_score"[\s\S]*?})', cleaned_response)
            if json_match:
                # Extract just the JSON object from the text
                potential_json = json_match.group(1)
                logger.info("Extracted potential JSON object from refinement response")
                try:
                    # Try parsing the extracted JSON
                    result = json.loads(potential_json)
                    if 'refined_code' in result and 'confidence_score' in result:
                        logger.info("Successfully extracted valid JSON from refinement response")
                        cleaned_response = potential_json
                except:
                    # If extraction fails, continue with the original cleaned response
                    logger.warning("Extracted text was not valid JSON, continuing with original response")
                    
            # Try to parse the JSON response
            result = json.loads(cleaned_response)

            # Validate structure and types
            if isinstance(result, dict) and \
               'refined_code' in result and isinstance(result['refined_code'], str) and \
               'confidence_score' in result and isinstance(result['confidence_score'], (int, float)) and 1 <= result['confidence_score'] <= 5 and \
               'refinement_notes' in result and isinstance(result['refinement_notes'], str):
                logger.info(f"Bedrock returned refinement. Confidence: {result['confidence_score']}/5")
                return result['refined_code'], result['confidence_score'], result['refinement_notes']
            else:
                logger.error(f"LLM refinement response has invalid structure or values: {cleaned_response}")
                raise ValueError("LLM response for refinement has invalid structure or values.")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON response for refinement: {e}\nResponse text: {response_text}")
            
            # Try to extract code from code blocks as fallback
            code_fences = re.findall(r'```(?:python|json)?([^`]+)```', response_text)
            if code_fences:
                last_code = code_fences[-1].strip()
                logger.info("Extracted code from code fence as fallback for refinement")
                return last_code, 3, "Code extracted from response after JSON parsing failed"
                
            # Fallback: Return original code, low confidence, and error note
            return pyspark_section, 1, f"Error: Failed to parse LLM refinement response JSON - {e}"
        except Exception as e:
            logger.error(f"Error processing refinement response: {e}\nResponse text: {response_text}")
            # Fallback: Return original code, low confidence, and error note
            return pyspark_section, 1, f"Error: Exception during refinement processing - {e}"

    # Pass the prompt to the retry wrapper
    return _call_llm_with_retry(invoke_refinement, prompt, description="LLM Refinement")


# --- Summarization ---
def call_llm_for_summarization(overall_context):
    """
    Calls Bedrock Claude 3.7 Sonnet to summarize large context.
    Focuses on preserving essential schema and dependency information.
    Handles errors gracefully with partial context preservation.
    """
    try:
        # First try to identify the most critical parts of the context
        critical_context = extract_critical_context(overall_context)
        
        # Only summarize if the context is large enough to warrant it
        if len(json.dumps(overall_context)) < CONTEXT_SIZE_LIMIT * 0.5:
            logger.info("Context is relatively small, skipping summarization.")
            return overall_context
            
        # Serialize the context, handling potential errors
        try:
            overall_context_json = json.dumps(overall_context, indent=2)
        except TypeError as e:
            logger.error(f"Could not serialize overall_context to JSON for summarization: {e}")
            # If we can't serialize the entire context, try to salvage critical parts
            return critical_context
            
        prompt = f"""Summarize the key information in the provided context JSON concisely, preserving essential details for downstream SAS to PySpark conversion.
Focus on these critical elements:
1. Dataset schemas (column names, types if available)
2. Most recently defined macros and their parameters
3. Important dataset lineage information (which datasets are derived from others)
4. Most recent step outputs that will likely be needed in subsequent steps

Output ONLY a valid JSON object with the same structure as the input, but with less detail in non-critical areas.
Remove redundant information and consolidate repetitive patterns.

Context JSON to Summarize:
```json
{overall_context_json}
```

Output ONLY the summarized JSON object."""

        def invoke_summarization(p):
            response_text = _invoke_claude37_sonnet(p)
            try:
                # Clean potential markdown code fences
                cleaned_response = re.sub(r'^```json\s*|\s*```$', '', response_text, flags=re.MULTILINE).strip()
                summarized_context = json.loads(cleaned_response)
                if isinstance(summarized_context, dict):
                    logger.info("Bedrock returned summarized context.")
                    
                    # Ensure critical parts are preserved
                    for key in critical_context:
                        if key not in summarized_context:
                            summarized_context[key] = critical_context[key]
                            
                    return summarized_context
                else:
                    logger.error(f"LLM summarization returned valid JSON but not an object: {cleaned_response}")
                    return critical_context  # Fall back to critical context
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON response for summarization: {e}\nResponse text: {response_text}")
                return critical_context  # Fall back to critical context
            except Exception as e:
                logger.error(f"Error processing summarization response: {e}\nResponse text: {response_text}")
                return critical_context  # Fall back to critical context

        # Pass the prompt to the retry wrapper
        return _call_llm_with_retry(invoke_summarization, prompt, description="LLM Context Summarization")
    
    except Exception as e:
        logger.error(f"Exception during context summarization: {e}")
        # In case of any error, return the original context or an empty context
        # as a last resort fallback
        try:
            return overall_context
        except:
            return {"error": "Context summarization failed completely", "fallback": True}


def extract_critical_context(context):
    """
    Extract the most critical elements from the overall context.
    Used as a fallback when summarization fails.
    """
    critical = {}
    
    try:
        # Try to preserve schema info for the most recently created/modified datasets
        if "schema_info" in context:
            # Take the 5 most recently referenced schemas or all if fewer
            schemas = list(context["schema_info"].items())
            critical["schema_info"] = dict(schemas[-5:] if len(schemas) > 5 else schemas)
        
        # Preserve all macro definitions as they're often critical
        if "macros_defined" in context:
            critical["macros_defined"] = context["macros_defined"]
        
        # Preserve the list of processed chunks for reference
        if "processed_chunks" in context:
            # Just keep the last few processed chunks to maintain continuity
            chunks = context["processed_chunks"]
            critical["processed_chunks"] = chunks[-10:] if len(chunks) > 10 else chunks
            
    except Exception as e:
        logger.error(f"Error extracting critical context: {e}")
        # Provide a minimal fallback context
        return {"error": "Failed to extract critical context", "schema_info": {}, "macros_defined": {}}
        
    return critical


# --- Helper Functions ---

def deterministic_split(sas_code, max_chunk_size=50000):
    """
    Splits SAS code into super-chunks based on PROC/DATA/MACRO boundaries.
    Uses regex to find logical boundaries and splits only at those boundaries.
    Aims for chunks under max_chunk_size (approximate character count).
    
    Returns a list of super-chunks (strings).
    """
    logger.info("Performing deterministic pre-processing and splitting...")
    
    # For very small code, just return as a single chunk without processing
    if len(sas_code) < 1000:
        logger.info(f"Code is very small ({len(sas_code)} chars), treating as single chunk")
        return [sas_code]
    
    # Regular expression to find major section boundaries in SAS code
    # This pattern looks for PROC, DATA, or %MACRO at the start of a line (case-insensitive)
    # and captures the entire statement
    boundary_pattern = re.compile(r'^\s*(PROC\s+\w+|DATA\s+[\w\.]+|%MACRO\s+[\w\.]+).*?;', re.IGNORECASE | re.MULTILINE)
    
    # Find all matches
    boundaries = list(boundary_pattern.finditer(sas_code))
    if not boundaries:
        logger.warning("No clear PROC/DATA/MACRO boundaries found. Treating as one chunk.")
        return [sas_code]
    
    logger.info(f"Found {len(boundaries)} potential split points in SAS code.")
    
    # Initialize super-chunks
    super_chunks = []
    start_index = 0
    current_size = 0
    
    for i, match in enumerate(boundaries):
        # Skip the first match as it defines the start of the first chunk
        if i == 0:
            start_index = match.start()
            current_size = 0
            continue
            
        # Calculate size of chunk if we were to split here
        chunk_size = match.start() - start_index
        
        # If adding this section would exceed max_chunk_size, split here
        if current_size + chunk_size > max_chunk_size and current_size > 0:
            chunk = sas_code[start_index:match.start()]
            super_chunks.append(chunk)
            logger.info(f"Created super-chunk of size {len(chunk)} chars")
            
            # Start a new chunk
            start_index = match.start()
            current_size = 0
        else:
            # Continue accumulating
            current_size += chunk_size
    
    # Add the final chunk
    final_chunk = sas_code[start_index:]
    if final_chunk.strip():
        super_chunks.append(final_chunk)
        logger.info(f"Created final super-chunk of size {len(final_chunk)} chars")
    
    # Special handling for very large chunks that couldn't be split
    # This handles cases where a single PROC/DATA/MACRO is larger than max_chunk_size
    revised_chunks = []
    for chunk in super_chunks:
        if len(chunk) > max_chunk_size * 1.5:  # If significantly larger than target
            logger.warning(f"Found very large chunk ({len(chunk)} chars). Attempting emergency split.")
            
            # Try to split at RUN; statements if possible
            run_pattern = re.compile(r'^\s*RUN\s*;', re.IGNORECASE | re.MULTILINE)
            run_matches = list(run_pattern.finditer(chunk))
            
            if run_matches:
                # Split at RUN; statements to keep chunks manageable
                chunk_start = 0
                for run_match in run_matches:
                    # Only split if this would make a reasonably sized chunk
                    if run_match.end() - chunk_start > max_chunk_size * 0.25:  # Ensure minimum chunk size
                        revised_chunks.append(chunk[chunk_start:run_match.end()])
                        chunk_start = run_match.end()
                
                # Add any remaining portion
                if chunk_start < len(chunk):
                    revised_chunks.append(chunk[chunk_start:])
            else:
                # If no RUN; statements, split at a reasonable line boundary as last resort
                lines = chunk.splitlines()
                chunk_lines = []
                current_chunk = []
                
                for line in lines:
                    current_chunk.append(line)
                    # Accumulate lines until we approach max size
                    if sum(len(l) for l in current_chunk) > max_chunk_size:
                        chunk_lines.append('\n'.join(current_chunk))
                        current_chunk = []
                
                # Add any remaining lines
                if current_chunk:
                    chunk_lines.append('\n'.join(current_chunk))
                
                revised_chunks.extend(chunk_lines)
        else:
            # Chunk is a reasonable size, keep as is
            revised_chunks.append(chunk)
    
    logger.info(f"Split SAS code into {len(revised_chunks)} super-chunks")
    
    # Filter out any empty chunks
    return [chunk for chunk in revised_chunks if chunk.strip()]

def map_context_for_chunks(logical_chunks, overall_context):
    """
    Maps context requirements for logical chunks.
    Analyzes dependencies and data lineage to determine what context is needed.
    """
    logger.info("Mapping context for logical chunks...")
    context_map = {}
    
    # Track all datasets/macros referenced in all chunks for dependency analysis
    all_inputs = set()
    all_outputs = set()
    chunk_references = {}
    
    # First pass - collect all references
    for i, chunk in enumerate(logical_chunks):
        chunk_id = f"chunk_{i}_{chunk.get('name', 'unnamed')}"
        inputs = set(chunk.get('inputs', []))
        outputs = set(chunk.get('outputs', []))
        
        all_inputs.update(inputs)
        all_outputs.update(outputs)
        
        chunk_references[chunk_id] = {
            'inputs': inputs,
            'outputs': outputs,
            'type': chunk.get('type', 'UNKNOWN'),
            'position': i  # Store position for dependency ordering
        }
    
    # Second pass - map dependencies between chunks
    for chunk_id, refs in chunk_references.items():
        # Direct dependencies - inputs that are outputs of previous chunks
        dependencies = []
        for other_id, other_refs in chunk_references.items():
            if other_id != chunk_id and refs['position'] > other_refs['position']:
                # If this chunk uses outputs from an earlier chunk, it depends on it
                if refs['inputs'].intersection(other_refs['outputs']):
                    dependencies.append(other_id)
        
        # Map required context from overall context
        required_context = {}
        
        # Include schema information for inputs if available
        for input_name in refs['inputs']:
            if input_name in overall_context.get('schema_info', {}):
                if 'schema_info' not in required_context:
                    required_context['schema_info'] = {}
                required_context['schema_info'][input_name] = overall_context['schema_info'][input_name]
        
        # Include macro definitions if this chunk uses macros
        for input_name in refs['inputs']:
            if input_name in overall_context.get('macros_defined', {}):
                if 'macros_defined' not in required_context:
                    required_context['macros_defined'] = {}
                required_context['macros_defined'][input_name] = overall_context['macros_defined'][input_name]
        
        # Store the context map for this chunk
        context_map[chunk_id] = {
            'required_inputs': list(refs['inputs']),
            'dependencies': dependencies,
            'required_context': required_context
        }
    
    logger.info(f"Context mapping complete. Mapped {len(context_map)} chunks with dependencies.")
    return context_map

def update_overall_context(overall_context, logical_chunks_in_sc, pyspark_outputs_in_sc):
    """
    Updates the overall context with outputs from the processed super-chunk.
    Tracks dataset schemas, macro definitions, and other contextual information.
    """
    logger.info("Updating overall context...")
    if "schema_info" not in overall_context:
        overall_context["schema_info"] = {}
    if "macros_defined" not in overall_context:
        overall_context["macros_defined"] = {}
    if "processed_chunks" not in overall_context:
        overall_context["processed_chunks"] = []
    
    # Add information about the processed chunks
    chunk_names = [f"{chunk.get('type')}:{chunk.get('name', 'unnamed')}" for chunk in logical_chunks_in_sc]
    overall_context["processed_chunks"].extend(chunk_names)
    
    # Track dataset lineage across chunks
    for chunk in logical_chunks_in_sc:
        chunk_type = chunk.get('type', '').upper()
        chunk_name = chunk.get('name', 'unnamed')
        
        # Process outputs based on chunk type
        for output in chunk.get('outputs', []):
            if chunk_type == 'MACRO':
                # Store macro definition information
                overall_context["macros_defined"][output] = {
                    "status": "defined",
                    "source_chunk": chunk_name,
                    "inputs": chunk.get('inputs', []),
                    "defined_in_sc": True
                }
            else:  # DATA or PROC
                # Infer basic schema information for dataset outputs
                # In a real implementation, we would extract this from the conversion results
                input_schemas = []
                for input_name in chunk.get('inputs', []):
                    if input_name in overall_context["schema_info"]:
                        input_schemas.append(overall_context["schema_info"][input_name])
                
                # If we have pyspark outputs with schema information, use that
                if output in pyspark_outputs_in_sc:
                    schema_info = pyspark_outputs_in_sc[output]
                else:
                    # Create a placeholder schema based on inputs or create a new one
                    columns = []
                    if input_schemas:
                        # Combine columns from input schemas as a best guess
                        for schema in input_schemas:
                            if "columns" in schema:
                                columns.extend(schema["columns"])
                        # Remove duplicates
                        columns = list(set(columns))
                    else:
                        # No input schemas, create placeholder
                        columns = ["unknown_column_1", "unknown_column_2"]
                    
                    schema_info = {
                        "columns": columns,
                        "source_chunk": chunk_name,
                        "derived_from": chunk.get('inputs', [])
                    }
                
                overall_context["schema_info"][output] = schema_info

    # Check context size and summarize if needed
    try:
        current_context_size = len(json.dumps(overall_context))
    except TypeError:
        current_context_size = 0  # Handle potential serialization errors

    logger.info(f"Current estimated context size: {current_context_size} chars.")

    if current_context_size > CONTEXT_SIZE_LIMIT:
        logger.warning(f"Overall context size ({current_context_size}) exceeds limit ({CONTEXT_SIZE_LIMIT}). Attempting summarization...")
        overall_context = call_llm_for_summarization(overall_context)
        try:
            new_size = len(json.dumps(overall_context))
            logger.info(f"Context size after summarization: {new_size} chars.")
        except TypeError:
            logger.warning("Could not estimate size after summarization due to content.")

    logger.info("Overall context update complete.")
    return overall_context

def basic_lint_check(pyspark_code):
    """
    Performs basic checks and formatting for PySpark code.
    Ensures proper indentation and Python syntax.
    """
    logger.info("Performing basic lint/syntax check and formatting...")
    warnings = []
    
    # Try to use ast.parse to check for syntax errors
    try:
        import ast
        ast.parse(pyspark_code)
        logger.info("Basic syntax check passed (using ast.parse).")
    except ImportError:
        logger.warning("`ast` module not available for syntax check.")
        warnings.append("Could not perform full syntax check (`ast` unavailable).")
    except SyntaxError as e:
        logger.error(f"Syntax error found: {e}")
        warnings.append(f"Syntax Error: {e}")
    except Exception as e:
        logger.error(f"Error during lint check: {e}")
        warnings.append(f"Lint Check Error: {e}")
        
    # Basic formatting enhancements
    try:
        # Consistent imports grouping at the top
        lines = pyspark_code.splitlines()
        formatted_lines = []
        
        # Create proper spacing around major code blocks
        current_block = ""
        for line in lines:
            # Add spacing around major blocks or after multi-line statements
            if (line.strip() and not line.startswith(" ") and current_block and 
                not current_block.startswith("import") and not line.startswith("import")):
                formatted_lines.append("")  # Add blank line
            
            formatted_lines.append(line)
            current_block = line.strip() if line.strip() else current_block
        
        # Enhance PySpark method chaining format
        improved_lines = []
        i = 0
        while i < len(formatted_lines):
            line = formatted_lines[i]
            
            # Check for PySpark method chains with backslash continuation
            if '\\' in line and any(method in line for method in ['.filter(', '.select(', '.withColumn(', '.join(', '.groupBy(']):
                # Start a new chain
                chain_lines = [line.rstrip('\\').rstrip()]
                j = i + 1
                
                # Collect all lines in this chain
                while j < len(formatted_lines) and (
                    formatted_lines[j].strip().startswith('.') or 
                    '\\' in formatted_lines[j]
                ):
                    chain_part = formatted_lines[j].strip()
                    if chain_part.endswith('\\'):
                        chain_part = chain_part.rstrip('\\').rstrip()
                    chain_lines.append(chain_part)
                    j += 1
                
                # Reformat the chain with proper indentation
                if len(chain_lines) > 1:
                    # First line stays as is
                    improved_lines.append(chain_lines[0])
                    
                    # Format the rest with consistent indentation
                    for part in chain_lines[1:]:
                        # Ensure method chains start with . and have consistent spacing
                        if not part.startswith('.') and not part.strip() == '':
                            part = '.' + part.lstrip()
                        improved_lines.append('    ' + part)
                    
                    i = j - 1  # Skip the lines we've processed
                else:
                    improved_lines.append(line)
            else:
                improved_lines.append(line)
            
            i += 1
        
        # Rejoin the formatted code
        formatted_code = "\n".join(improved_lines)
        
        # Fix common chain formatting issues
        # Change one-line chains with multiple methods to multi-line format
        for method in ['.filter(', '.select(', '.withColumn(', '.join(', '.groupBy(']:
            pattern = r'(\w+)(' + re.escape(method) + r'[^\n.]*?)\.(\w+\()'
            formatted_code = re.sub(pattern, r'\1\2\n    .\3', formatted_code)
        
        # Try to use black for code formatting if available
        try:
            import black
            mode = black.Mode(line_length=100)
            formatted_code = black.format_str(formatted_code, mode=mode)
            logger.info("Applied black formatting to PySpark code.")
        except (ImportError, Exception) as e:
            logger.warning(f"Could not use black for code formatting: {e}")
            # Black not available, continue with basic formatted code
        
        return formatted_code, warnings
    except Exception as e:
        logger.error(f"Error during code formatting: {e}")
        warnings.append(f"Formatting Error: {e}")
        # Return original code if formatting fails
        return pyspark_code, warnings

# --- Main Lambda Handler ---

def lambda_handler(event, context):
    """
    Main AWS Lambda handler function.
    Orchestrates the multi-pass SAS to PySpark conversion.
    Handles API Gateway requests from the React UI.
    """
    # Reset token usage stats for this invocation
    global token_usage_stats
    token_usage_stats = {
        'total_tokens': 0,
        'input_tokens': 0,
        'output_tokens': 0,
        'api_calls': 0
    }
    
    # Performance optimization: clear cache if it's getting too large
    if USE_LLM_CACHING and len(llm_response_cache) > MAX_CACHE_ENTRIES * 0.9:
        llm_response_cache.clear()
        logger.info("Cleared LLM response cache to prevent memory issues.")
    
    # Check if Bedrock is available, if not, run in test mode
    if not USE_BEDROCK:
        logger.warning("Running in test/demo mode - Bedrock is not available")
        
        # Create a simple demo response for testing
        demo_response = {
            "status": "success",
            "pyspark_code": "# Demo mode - Bedrock not available\n\nfrom pyspark.sql.functions import col, lit\n\n# This is a demo conversion since Bedrock is not available\ndef demo_function():\n    print('This is a demo conversion')\n    \n# Simulated dataframe operations\noutput_df = input_df.filter(col('category') == 'A')\noutput_df = output_df.withColumn('new_var', col('old_var') * 2)",
            "annotations": [{"sas_lines": [1, 5], "pyspark_lines": [1, 10], "note": "Demo conversion - Bedrock is not available", "severity": "Info"}],
            "warnings": ["Running in demo mode without Bedrock access"],
            "strategy_used": "Demo",
            "processing_stats": {
                "demo_mode": True,
                "total_duration_seconds": 0.1
            },
            "processing_complete": True
        }
        
        # Prepare response with CORS headers
        response_headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'OPTIONS,POST',
            'Content-Type': 'application/json; charset=utf-8'
        }
        
        # Handle preflight OPTIONS request
        if event.get('httpMethod') == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': response_headers,
                'body': json.dumps({'message': 'CORS preflight request successful'})
            }
            
        # Get SAS code from event to include in demo response
        try:
            if 'body' in event:
                body = json.loads(event.get('body', '{}'))
                sas_code = body.get('sas_code', '')
                if sas_code:
                    # Add the original SAS code as a comment in the demo response
                    demo_response["pyspark_code"] += f"\n\n# Original SAS code (demo mode):\n# " + sas_code.replace("\n", "\n# ")
        except:
            pass
        
        return {
            'statusCode': 200,
            'headers': response_headers,
            'body': json.dumps(demo_response, ensure_ascii=False)
        }
    
    # Get Lambda timeout from context and set safety margin (in seconds)
    LAMBDA_TIMEOUT = context.get_remaining_time_in_millis() / 1000 if context else 900  # Default 15 min
    SAFETY_MARGIN = float(os.environ.get("TIMEOUT_SAFETY_MARGIN", "30"))  # Default 30 seconds
    timeout_deadline = time.time() + LAMBDA_TIMEOUT - SAFETY_MARGIN
    
    # Prepare response with CORS headers for browser access
    response_headers = {
        'Access-Control-Allow-Origin': '*',  # Allow requests from any origin
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'OPTIONS,POST',
        'Content-Type': 'application/json; charset=utf-8'  # Explicitly set UTF-8 encoding
    }
    
    # Handle preflight OPTIONS request
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': response_headers,
            'body': json.dumps({'message': 'CORS preflight request successful'})
        }
    
    try:
        # Log the event but redact sensitive information
        if isinstance(event, dict):
            event_log = event.copy()
            if 'body' in event_log:
                try:
                    body_json = json.loads(event_log['body'])
                    if 'sas_code' in body_json:
                        char_count = len(body_json['sas_code'])
                        body_json['sas_code'] = f"[SAS CODE: {char_count} chars]"
                    event_log['body'] = json.dumps(body_json)
                except:
                    event_log['body'] = "[UNPARSEABLE BODY]"
            logger.info(f"Received event: {json.dumps(event_log)}")
        else:
            logger.info("Received non-dict event")
            
        logger.info(f"Lambda timeout: {LAMBDA_TIMEOUT:.1f}s, using safety margin of {SAFETY_MARGIN}s")

        # Extract SAS code from the event payload
        # Initialize variables
        body = {}
        sas_code = None
        options = {}
        
        # Check if this is a multipart/form-data request (file upload)
        content_type = None
        if 'headers' in event:
            headers = event.get('headers', {})
            if headers:
                content_type = headers.get('content-type', headers.get('Content-Type', ''))
        
        if content_type and 'multipart/form-data' in content_type:
            logger.info("Processing multipart/form-data request (file upload)")
            
            # Parse multipart/form-data content
            try:
                # Check if the body is already decoded or needs to be decoded from base64
                if event.get('isBase64Encoded', False):
                    import base64
                    decoded_body = base64.b64decode(event['body'])
                else:
                    decoded_body = event['body']
                
                # Extract boundary from content type
                boundary = None
                for part in content_type.split(';'):
                    if 'boundary=' in part:
                        boundary = part.split('boundary=')[1].strip()
                
                if not boundary:
                    raise ValueError("Boundary not found in multipart/form-data content type")
                
                # Parse multipart form data to extract file content
                if isinstance(decoded_body, str):
                    parts = decoded_body.split('--' + boundary)
                else:
                    # If it's bytes, decode to string first
                    parts = decoded_body.decode('utf-8', errors='replace').split('--' + boundary)
                
                for part in parts:
                    if 'filename=' in part and 'Content-Type' in part:
                        # This part contains a file
                        # Extract filename
                        filename_match = re.search(r'filename=\"(.+?)\"', part)
                        if filename_match and filename_match.group(1).lower().endswith('.sas'):
                            # Found a SAS file, extract its content
                            content_sections = part.split('\r\n\r\n', 1)
                            if len(content_sections) > 1:
                                file_content = content_sections[1].rsplit('\r\n', 1)[0]
                                sas_code = file_content
                                logger.info(f"Extracted SAS file content: {len(sas_code)} characters")
                    elif 'name="options"' in part:
                        # This part contains options JSON
                        content_sections = part.split('\r\n\r\n', 1)
                        if len(content_sections) > 1:
                            options_content = content_sections[1].rsplit('\r\n', 1)[0]
                            try:
                                options = json.loads(options_content)
                                logger.info("Successfully parsed options from form data")
                            except json.JSONDecodeError:
                                logger.warning(f"Could not parse options JSON: {options_content}")
            
            except Exception as e:
                logger.error(f"Error processing multipart/form-data: {e}", exc_info=True)
                return {
                    'statusCode': 400,
                    'headers': response_headers,
                    'body': json.dumps({'error': f"Error processing file upload: {str(e)}"})
                }
        
        # If not found in multipart form, check regular JSON body
        if not sas_code:
            # Check for different request formats (direct API Gateway or Lambda proxy)
            if 'body' in event:
                try:
                    body = json.loads(event.get('body', '{}'))
                except json.JSONDecodeError:
                    logger.error("Invalid JSON in request body")
                    return {
                        'statusCode': 400,
                        'headers': response_headers,
                        'body': json.dumps({'error': "Invalid JSON format in request body"})
                    }
            else:
                # Assume direct invocation with event as the body
                body = event

            sas_code = body.get('sas_code')
            options = body.get('options', {})

        if not sas_code:
            logger.error("No 'sas_code' found in the request (neither as JSON field nor as file upload).")
            return {
                'statusCode': 400,
                'headers': response_headers,
                'body': json.dumps({'error': "Request must contain 'sas_code' field or a SAS file upload"})
            }
            
        # Process options
        expected_token_count = options.get('expected_token_count', 0)
        force_extreme_mode = options.get('extreme_mode', False)
        skip_refinement = options.get('skip_refinement', False)  # New option to skip refinement for speed
        minimal_annotations = options.get('minimal_annotations', False)  # New option to reduce annotation detail
        skip_context_focusing = options.get('skip_context_focusing', False)  # Skip focusing step for small files
        
        # Performance optimization: Determine file size 
        sas_code_size = len(sas_code)
        sas_code_lines = sas_code.count('\n') + 1
        
        # Determine if we should use extreme mode based on code size or explicit flag
        use_extreme_mode = force_extreme_mode or sas_code_size > EXTREME_CODE_THRESHOLD or expected_token_count > 100000
        
        # For small files, use a simpler approach with fewer LLM calls
        use_simplified_approach = sas_code_size < 5000 and sas_code_lines < 200 and not force_extreme_mode
        
        if use_extreme_mode:
            logger.info("EXTREME MODE ACTIVATED: Using full 128K token context window")
            global MAX_TOKENS
            MAX_TOKENS = MAX_EXTREME_TOKENS  # Override with maximum token limit
        
        # Basic size logging
        logger.info(f"SAS code size: {sas_code_size} chars, {sas_code_lines} lines, expected tokens: {expected_token_count}")
        if sas_code_size > VERY_LARGE_CODE_THRESHOLD:
            logger.warning(f"Large input detected ({sas_code_size} chars). Adjusting processing strategy.")
        
        # --- Initialize State ---
        state = {
            "sas_super_chunks": [],
            "overall_context": {},
            "refined_pyspark_sections": [],
            "annotations": [],
            "strategy": None,
            "warnings": [],
            "processing_stats": {
                "start_time": time.time(),
                "chunks_processed": 0,
                "llm_calls": 0,
                "refinement_attempts": 0,
                "extreme_mode": use_extreme_mode,
                "simplified_approach": use_simplified_approach
            },
            "partial_processing": False
        }
        logger.info("Initialized state.")

        # --- Pass 0a: Deterministic Pre-processing & Splitting ---
        disable_splitting = options.get('disable_splitting', False) or use_simplified_approach
        if disable_splitting or sas_code_size <= VERY_LARGE_CODE_THRESHOLD:
            state["sas_super_chunks"] = [sas_code]  # Treat as one super-chunk
        else:
            # For extreme mode, allow larger chunk sizes
            max_chunk_size = 100000 if use_extreme_mode else 50000
            state["sas_super_chunks"] = deterministic_split(sas_code, max_chunk_size=max_chunk_size)

        # --- Pass 0b: LLM Strategy Advisor ---
        # Performance optimization: Skip strategy detection for very small files or use default for simplified approach
        if use_simplified_approach:
            state["strategy"] = "Simple Direct"
            logger.info(f"Using simplified approach with 'Simple Direct' strategy for small file")
        else:
            # Use strategy override if provided
            strategy_override = options.get('strategy_override')
            if strategy_override in ['Simple Direct', 'Standard Multi-Pass', 'Deep Macro Analysis']:
                state["strategy"] = strategy_override
                logger.info(f"Using strategy override: {state['strategy']}")
            else:
                # Use the entire code for strategy selection if it's very small
                if sas_code_size < 5000:
                    sas_sample = sas_code
                else:
                    sas_sample = state["sas_super_chunks"][0][:3000]  # Use first 3000 chars of first chunk as sample
                    
                state["strategy"] = call_llm_for_strategy(sas_sample)
                logger.info(f"Using detected strategy: {state['strategy']}")
                state["processing_stats"]["llm_calls"] += 1
        
        # Apply strategy-specific adjustments
        strategy_config = configure_strategy(state["strategy"], use_extreme_mode)
        global MAX_TOKENS_PER_STRATEGY
        MAX_TOKENS_PER_STRATEGY = strategy_config["max_tokens"]
        CONFIDENCE_THRESHOLD = strategy_config["confidence_threshold"]
        MAX_REFINEMENT_ATTEMPTS = strategy_config["max_refinement_attempts"]
        
        # Check if we need to reorder chunks for priority processing
        # Skip this for simplified approach
        if not use_simplified_approach and len(state["sas_super_chunks"]) > 1:
            prioritized_chunks = prioritize_chunks(state["sas_super_chunks"], state["strategy"])
        else:
            prioritized_chunks = state["sas_super_chunks"]

        # --- Loop Through Super-Chunks ---
        for i, super_chunk in enumerate(prioritized_chunks):
            # Check for timeout before processing each super-chunk
            if time.time() > timeout_deadline:
                logger.warning(f"Approaching Lambda timeout. Processed {i}/{len(prioritized_chunks)} super-chunks. Returning partial results.")
                state["warnings"].append(f"Processing incomplete: Lambda timeout approaching after processing {i}/{len(prioritized_chunks)} chunks.")
                state["partial_processing"] = True
                break
                
            logger.info(f"--- Processing Super-Chunk {i+1}/{len(prioritized_chunks)} ---")
            sc_pyspark_chunks = []
            sc_logical_chunks = []  # Store logical chunks for this SC

            try:
                # Performance optimization: Simplified approach for small files
                if use_simplified_approach:
                    # For very small files, treat the entire content as one logical chunk
                    simple_chunk = {
                        "type": "UNKNOWN",
                        "name": "single_chunk",
                        "start_line": 1,
                        "end_line": sas_code_lines,
                        "inputs": [],
                        "outputs": []
                    }
                    sc_logical_chunks = [simple_chunk]
                else:
                    # --- Pass 1: LLM - Structural Analysis (within SC) ---
                    sc_logical_chunks = call_llm_for_structural_analysis(super_chunk)
                    state["processing_stats"]["llm_calls"] += 1
                
                if not sc_logical_chunks:
                    logger.warning(f"No logical chunks identified in Super-Chunk {i+1}. Skipping.")
                    state["warnings"].append(f"No logical chunks found in Super-Chunk {i+1}.")
                    continue

                # --- Pass 2: LLM/Code - Context Mapping (within SC) ---
                # Skip detailed context mapping for simplified approach
                if use_simplified_approach:
                    sc_chunk_context_map = {"chunk_0_single_chunk": {"required_inputs": [], "dependencies": [], "required_context": {}}}
                else:
                    sc_chunk_context_map = map_context_for_chunks(sc_logical_chunks, state["overall_context"])

                # --- Loop Through Logical Chunks within Super-Chunk ---
                for j, logical_chunk in enumerate(sc_logical_chunks):
                    # Check for timeout before each logical chunk
                    if time.time() > timeout_deadline:
                        logger.warning(f"Approaching Lambda timeout during chunk processing. Stopping at logical chunk {j+1}/{len(sc_logical_chunks)}.")
                        state["warnings"].append(f"Processing incomplete: Lambda timeout approaching during super-chunk {i+1}, logical chunk {j+1}.")
                        state["partial_processing"] = True
                        break
                        
                    logger.info(f"--- Processing Logical Chunk {j+1}/{len(sc_logical_chunks)} ({logical_chunk.get('type', '')} '{logical_chunk.get('name', 'unnamed')}') ---")

                    # Extract the code for the current logical chunk
                    # Line numbers from LLM are 1-based and inclusive
                    start_line = logical_chunk.get('start_line', 1) - 1  # Adjust to 0-based index
                    end_line = logical_chunk.get('end_line', start_line + 1)
                    
                    # Safety check for line numbers
                    max_line = len(super_chunk.splitlines())
                    if start_line >= max_line or end_line > max_line:
                        logger.warning(f"Invalid line numbers in logical chunk: start={start_line+1}, end={end_line}, max={max_line}")
                        start_line = min(start_line, max_line-1)
                        end_line = min(end_line, max_line)
                        state["warnings"].append(f"Invalid line numbers detected in chunk {j+1}. Adjusted to valid range.")
                    
                    logical_chunk_code_lines = super_chunk.splitlines()[start_line:end_line]
                    logical_chunk_code = "\n".join(logical_chunk_code_lines)

                    if not logical_chunk_code.strip():
                        logger.warning(f"Logical chunk {j+1} has empty code. Skipping.")
                        continue

                    try:
                        # --- Pass 2.5: LLM - Dynamic Context Focusing ---
                        # Performance optimization: Skip context focusing for simplified approach or if disabled
                        focused_context = state["overall_context"]
                        if not (use_simplified_approach or skip_context_focusing or 
                               (state["strategy"] == "Simple Direct" and len(json.dumps(state["overall_context"])) < 1000)):
                            focused_context = call_llm_for_context_focus(state["overall_context"], logical_chunk_code)
                            state["processing_stats"]["llm_calls"] += 1

                        # --- Pass 3: LLM - Focused Conversion & Annotation ---
                        add_detailed_comments = options.get('add_detailed_comments', not minimal_annotations)
                        apply_immediate_optimizations = options.get('apply_immediate_optimizations', True)
                        
                        # Add these options to the prompt context
                        conversion_context = {
                            **focused_context,
                            "conversion_options": {
                                "add_detailed_comments": add_detailed_comments,
                                "apply_immediate_optimizations": apply_immediate_optimizations,
                                "minimal_annotations": minimal_annotations
                            }
                        }
                        
                        pyspark_chunk, chunk_annotations = call_llm_for_conversion(
                            logical_chunk_code, 
                            conversion_context, 
                            logical_chunk.get('type'), 
                            state["strategy"]
                        )
                        state["processing_stats"]["llm_calls"] += 1
                        
                        sc_pyspark_chunks.append(pyspark_chunk)
                        state["annotations"].extend(chunk_annotations)

                    except Exception as chunk_err:
                        logger.error(f"Error processing logical chunk {j+1}: {chunk_err}", exc_info=True)
                        state["warnings"].append(f"Failed to process logical chunk {j+1} ('{logical_chunk.get('name', 'unnamed')}'): {chunk_err}")
                        # Add a placeholder for the failed chunk
                        sc_pyspark_chunks.append(f"# ERROR: Failed to convert logical chunk {j+1}\n")
                
                # Check if we exited the loop early due to timeout
                if state["partial_processing"]:
                    break

                # --- Pass 4a: LLM - Confidence-Driven Refinement (for the SC) ---
                assembled_sc_pyspark = "\n\n".join(sc_pyspark_chunks)
                if assembled_sc_pyspark.strip():
                    # Performance optimization: Skip refinement for Simple Direct strategy or if explicitly disabled
                    if state["strategy"] == "Simple Direct" or use_simplified_approach or skip_refinement:
                        # Simple conversion - one pass only
                        refined_section = assembled_sc_pyspark
                    else:
                        # Apply strategy-specific refinement
                        refinement_notes = None
                        refined_section = assembled_sc_pyspark
                        confidence = 0
                        
                        # Performance optimization: Reduce max refinement attempts for small files
                        max_attempts = 1 if sas_code_size < 10000 else MAX_REFINEMENT_ATTEMPTS

                        for attempt in range(max_attempts):
                            # Check for timeout before refinement
                            if time.time() > timeout_deadline:
                                logger.warning(f"Approaching Lambda timeout during refinement. Stopping after attempt {attempt}.")
                                state["warnings"].append(f"Refinement incomplete: Lambda timeout approaching during super-chunk {i+1} refinement.")
                                state["partial_processing"] = True
                                break
                                
                            logger.info(f"Refinement attempt {attempt+1}/{max_attempts} for Super-Chunk {i+1}")
                            refined_section, confidence, refinement_notes = call_llm_for_refinement(refined_section, refinement_notes)
                            state["processing_stats"]["llm_calls"] += 1
                            state["processing_stats"]["refinement_attempts"] += 1
                            
                            if confidence >= CONFIDENCE_THRESHOLD:
                                logger.info(f"Confidence threshold ({CONFIDENCE_THRESHOLD}) met.")
                                break
                            else:
                                logger.warning(f"Confidence {confidence} below threshold ({CONFIDENCE_THRESHOLD}). Will attempt refinement again if possible.")
                        else:  # Runs if loop completes without break
                            logger.warning(f"Maximum refinement attempts reached for Super-Chunk {i+1}. Final confidence: {confidence}")
                            state["warnings"].append(f"Low confidence ({confidence}/5) for Super-Chunk {i+1} after refinement attempts.")

                    state["refined_pyspark_sections"].append(refined_section)
                else:
                    logger.warning(f"No PySpark code generated for Super-Chunk {i+1} to refine.")

                # --- Update & Summarize Overall Context ---
                # Performance optimization: Skip context updates for simplified approach
                if not use_simplified_approach:
                    # Pass the logical chunks identified and any outputs 
                    schema_outputs = {}  # In a real implementation, extract this from conversion results
                    state["overall_context"] = update_overall_context(state["overall_context"], sc_logical_chunks, schema_outputs)
                
                # Update stats
                state["processing_stats"]["chunks_processed"] += 1
                
                # Check if we should stop processing due to timeout after this chunk
                if time.time() > timeout_deadline:
                    logger.warning(f"Approaching Lambda timeout after super-chunk {i+1}. Stopping.")
                    state["warnings"].append(f"Processing incomplete: Lambda timeout approaching after processing {i+1}/{len(prioritized_chunks)} chunks.")
                    state["partial_processing"] = True
                    break

            except Exception as sc_err:
                logger.error(f"Error processing Super-Chunk {i+1}: {sc_err}", exc_info=True)
                state["warnings"].append(f"Failed to process Super-Chunk {i+1}: {sc_err}")
                # Add a placeholder error section
                state["refined_pyspark_sections"].append(f"# ERROR: Failed to process Super-Chunk {i+1}\n")

        # --- Pass 4b: Final Assembly & Annotation Aggregation ---
        logger.info("Starting final assembly and annotation aggregation...")
        final_pyspark_code = "\n\n# --- End of Super-Chunk ---\n\n".join(state["refined_pyspark_sections"])

        # Enhanced linting and formatting for better code quality
        try:
            formatted_pyspark_code, lint_warnings = basic_lint_check(final_pyspark_code)
            if formatted_pyspark_code:
                final_pyspark_code = formatted_pyspark_code
            state["warnings"].extend(lint_warnings)
        except Exception as format_err:
            logger.error(f"Error during code formatting: {format_err}", exc_info=True)
            state["warnings"].append(f"Code formatting error: {format_err}")

        # Apply final formatting improvements that weren't caught earlier
        try:
            # Ensure import statements are at the top
            import_pattern = re.compile(r'^import\s+.*$|^from\s+.*\s+import\s+.*$', re.MULTILINE)
            import_statements = import_pattern.findall(final_pyspark_code)
            
            if import_statements:
                # Remove imports from their current locations
                code_without_imports = import_pattern.sub('', final_pyspark_code)
                
                # Deduplicate imports and sort them
                unique_imports = list(set(import_statements))
                standard_lib_imports = sorted([imp for imp in unique_imports if not imp.startswith('from pyspark') and not imp.startswith('import pyspark')])
                pyspark_imports = sorted([imp for imp in unique_imports if imp.startswith('from pyspark') or imp.startswith('import pyspark')])
                
                # Combine imports in the proper order
                all_imports = standard_lib_imports + [''] + pyspark_imports if (standard_lib_imports and pyspark_imports) else standard_lib_imports + pyspark_imports
                
                # Build the final code with organized imports at the top
                organized_code = '\n'.join(all_imports)
                if organized_code:
                    organized_code += '\n\n'
                organized_code += code_without_imports.strip()
                
                final_pyspark_code = organized_code
        except Exception as e:
            logger.warning(f"Error during import organization: {e}")
            # Keep the original code if import reorganization fails

        # Add processing stats
        state["processing_stats"]["end_time"] = time.time()
        state["processing_stats"]["total_duration_seconds"] = state["processing_stats"]["end_time"] - state["processing_stats"]["start_time"]
        
        # Add partial processing flag and chunk count information
        if state["partial_processing"]:
            state["processing_stats"]["total_chunks"] = len(state["sas_super_chunks"])
            state["processing_stats"]["processed_chunks"] = state["processing_stats"]["chunks_processed"]
            state["processing_stats"]["processing_complete"] = False
        else:
            state["processing_stats"]["processing_complete"] = True
            
        # Add token usage statistics
        state["processing_stats"]["token_usage"] = token_usage_stats
        
        # Add cache stats if caching is enabled
        if USE_LLM_CACHING:
            state["processing_stats"]["cache_stats"] = {
                "cache_enabled": True,
                "cache_entries": len(llm_response_cache)
            }

        # Structure final output
        final_output = {
            "status": "success",
            "pyspark_code": final_pyspark_code,
            "annotations": state["annotations"],
            "warnings": state["warnings"],
            "strategy_used": state["strategy"],
            "processing_stats": state["processing_stats"],
            "processing_complete": not state["partial_processing"]
        }
        logger.info("Final assembly complete.")

        # Ensure proper JSON encoding with UTF-8
        try:
            response_body = json.dumps(final_output, ensure_ascii=False)
        except UnicodeEncodeError as e:
            logger.error(f"Unicode encoding error in response: {e}")
            # Fall back to ASCII encoding if necessary
            response_body = json.dumps(final_output, ensure_ascii=True)
            state["warnings"].append("Warning: Some special characters may be encoded as Unicode escape sequences.")
        except Exception as e:
            logger.error(f"JSON encoding error: {e}")
            return {
                'statusCode': 500,
                'headers': response_headers,
                'body': json.dumps({
                    'status': 'error',
                    'message': 'Failed to encode response',
                    'details': str(e)
                }, ensure_ascii=False)
            }

        return {
            'statusCode': 200,
            'headers': response_headers,
            'body': response_body
        }

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in request body: {e}")
        return {
            'statusCode': 400,
            'headers': response_headers,
            'body': json.dumps({
                'status': 'error',
                'message': f"Invalid JSON format in request body: {e}"
            }, ensure_ascii=False)
        }
    except Exception as e:
        logger.error(f"Unhandled exception in lambda_handler: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': response_headers,
            'body': json.dumps({
                'status': 'error',
                'message': f"Internal server error: {e}", 
                'details': str(e)
            }, ensure_ascii=False)
        }

def configure_strategy(strategy, use_extreme_mode=False):
    """
    Configure parameters based on the selected strategy and code size.
    
    Parameters:
    - strategy: The conversion strategy to use
    - use_extreme_mode: Whether to enable extreme token mode for very large files
    
    Returns:
    - Dictionary with configuration parameters
    """
    # In extreme mode, use the maximum possible tokens
    if use_extreme_mode:
        return {
            "max_tokens": MAX_EXTREME_TOKENS,  # Use 120K token limit
            "confidence_threshold": 4.0,  # Standard confidence threshold
            "max_refinement_attempts": 2 if strategy == "Simple Direct" else 3  # More refinement for complex cases
        }
        
    # Normal mode configuration based on strategy
    if strategy == "Simple Direct":
        return {
            "max_tokens": MAX_TOKENS,  # Default 64K tokens
            "confidence_threshold": 3,  # Lower threshold as we don't expect perfection
            "max_refinement_attempts": 0  # No refinement for simple conversion
        }
    elif strategy == "Deep Macro Analysis":
        return {
            "max_tokens": MAX_TOKENS,  # Default 64K tokens
            "confidence_threshold": 4.5,  # Higher threshold for complex code
            "max_refinement_attempts": 3  # More refinement attempts
        }
    else:  # "Standard Multi-Pass" or fallback
        return {
            "max_tokens": MAX_TOKENS,  # Default 64K tokens
            "confidence_threshold": 4,
            "max_refinement_attempts": 2
        }

def prioritize_chunks(chunks, strategy):
    """
    Reorders chunks for processing priority based on strategy.
    """
    if strategy == "Deep Macro Analysis":
        # For deep analysis, try to process macro definitions first
        prioritized = []
        macro_chunks = []
        other_chunks = []
        
        for chunk in chunks:
            # Simple heuristic - check if this chunk likely contains macro definitions
            if "%MACRO" in chunk.upper() or "MACRO " in chunk.upper():
                macro_chunks.append(chunk)
            else:
                other_chunks.append(chunk)
                
        # Process macros first, then other chunks
        prioritized = macro_chunks + other_chunks
        if len(prioritized) != len(chunks):
            logger.warning(f"Chunk count mismatch during prioritization. Using original ordering.")
            return chunks
            
        logger.info(f"Reordered chunks for {strategy}: {len(macro_chunks)} macro chunks first, {len(other_chunks)} other chunks after.")
        return prioritized
    
    # For other strategies, keep original order
    return chunks

# Add a simple test handler at the end of the file for testing with SAS samples
def test_handler(sas_code):
    """
    Simple utility to test the Lambda function with SAS code.
    Simulates an API Gateway event with the provided SAS code.
    
    Usage (from AWS Lambda console):
    
    import lambda_function
    result = lambda_function.test_handler('''
    DATA work.step1;
        SET raw.data;
        new_var = old_var * 2;
        IF category = 'A' THEN output;
    RUN;
    ''')
    print(result)
    """
    test_event = {
        "body": json.dumps({
            "sas_code": sas_code,
            "options": {
                "disable_splitting": True,  # For simple testing
                "strategy_override": "Simple Direct"  # For faster processing
            }
        })
    }
    context = {}
    result = lambda_handler(test_event, context)
    return result
)

    # test_sas_complex = """
    # %MACRO calculate_risk(input_ds, output_ds, threshold);
    #     DATA &output_ds;
    #         SET &input_ds;
    #         IF score > &threshold THEN risk_level = 'High';
    #         ELSE risk_level = 'Low';
    #     RUN;
    # %MEND calculate_risk;
    #
    # PROC SQL;
    #    CREATE TABLE work.joined AS
    #    SELECT a.*, b.region
    #    FROM work.sales a
    #    LEFT JOIN work.regions b ON a.store_id = b.store_id;
    # QUIT;
    #
    # %calculate_risk(input_ds=work.joined, output_ds=work.risk_assessed, threshold=0.75);
    # """
    # test_handler(test_sas_complex)
