import json
import re
import logging
import time
import boto3
import os
import hashlib
from functools import lru_cache
from botocore.config import Config

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
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-3-7-sonnet-20250219-v1:0")
ANTHROPIC_VERSION = "bedrock-2023-05-31"

# Define constants
MAX_TOKENS = 64000  # Default token limit
MAX_EXTREME_TOKENS = 120000  # For extreme mode with very large files
EXTREME_CODE_THRESHOLD = 200000  # Characters
VERY_LARGE_CODE_THRESHOLD = 100000  # Characters
USE_LLM_CACHING = True
MAX_CACHE_ENTRIES = 100

# Global token usage tracking
token_usage_stats = {
    'total_tokens': 0,
    'input_tokens': 0,
    'output_tokens': 0,
    'api_calls': 0
}

# Cache for LLM responses
llm_response_cache = {}

# --- Lambda Handler (Fixed Version) ---
def lambda_handler(event, context):
    """
    Main AWS Lambda handler function.
    Orchestrates the multi-pass SAS to PySpark conversion.
    Handles API Gateway requests from the React UI.
    
    This is a fixed version with properly structured try-except blocks.
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
        # Get Lambda timeout from context and set safety margin (in seconds)
        LAMBDA_TIMEOUT = context.get_remaining_time_in_millis() / 1000 if context else 900  # Default 15 min
        SAFETY_MARGIN = float(os.environ.get("TIMEOUT_SAFETY_MARGIN", "30"))  # Default 30 seconds
        timeout_deadline = time.time() + LAMBDA_TIMEOUT - SAFETY_MARGIN
        
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

        # --- Pass 0b: Strategy Selection with Adaptive Approach ---
        # Use strategy override if provided, otherwise use assess_complexity
        strategy_override = options.get('strategy_override')
        if strategy_override in ['Simple Direct', 'Standard Multi-Pass', 'Deep Macro Analysis']:
            state["strategy"] = strategy_override
            logger.info(f"Using strategy override: {state['strategy']}")
        else:
            # Use Heuristic Complexity Analyzer to determine the approach
            complexity = assess_complexity(sas_code)
            
            if complexity == "Simple":
                state["strategy"] = "Simple Direct"
            else:  # complexity == "Complex"
                # For complex code, use the more robust multi-stage approach
                # Optionally, we could still use the LLM to choose between Standard and Deep Macro
                if "MACRO" in sas_code.upper() and sas_code.count("%MACRO") > 3:
                    state["strategy"] = "Deep Macro Analysis"
                else:
                    state["strategy"] = "Standard Multi-Pass"
            
            logger.info(f"Complexity assessed as {complexity}. Using {state['strategy']} strategy")
            
        # Performance tracking for adaptive strategy
        state["processing_stats"]["complexity_assessment"] = "Heuristic Analysis"
        state["processing_stats"]["assessed_complexity"] = complexity if 'complexity' in locals() else "Override"
        
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
        
        # --- Process based on strategy ---
        if state["strategy"] == "Simple Direct":
            # Use the Simple Conversion Path for Simple code
            logger.info("Using Simple Direct strategy with single-call approach")
            
            try:
                # Pass options as part of overall_context
                context_for_simple = {
                    "conversion_options": options
                }
                
                # Call the simple conversion path
                pyspark_code, annotations, confidence = simple_conversion_path(sas_code, context_for_simple)
                
                # Store results in state
                state["refined_pyspark_sections"] = [pyspark_code]
                state["annotations"] = annotations
                state["processing_stats"]["confidence_score"] = confidence
                state["processing_stats"]["llm_calls"] += 1  # Count the single call
                state["processing_stats"]["chunks_processed"] = 1
                
                # Consider refinement if confidence is low and refinement is enabled
                if not skip_refinement and confidence < CONFIDENCE_THRESHOLD and MAX_REFINEMENT_ATTEMPTS > 0:
                    logger.info(f"Low confidence score ({confidence}), attempting refinement")
                    try:
                        refined_code, new_confidence, notes = call_llm_for_refinement(pyspark_code)
                        if new_confidence > confidence:
                            state["refined_pyspark_sections"] = [refined_code]
                            state["annotations"].append({
                                "sas_lines": [1, sas_code_lines],
                                "pyspark_lines": [1, refined_code.count('\n') + 1],
                                "note": f"Code refined: {notes}",
                                "severity": "Info"
                            })
                            state["processing_stats"]["confidence_score"] = new_confidence
                            state["processing_stats"]["refinement_attempts"] += 1
                            state["processing_stats"]["llm_calls"] += 1
                    except Exception as e:
                        logger.error(f"Error during refinement: {e}")
                        state["warnings"].append(f"Refinement failed: {str(e)}")
            
            except Exception as e:
                logger.error(f"Error in Simple Conversion Path: {e}", exc_info=True)
                state["warnings"].append(f"Simple conversion failed: {str(e)}. Consider using a different strategy.")
                state["refined_pyspark_sections"] = ["# Error during conversion\n# " + str(e)]
                state["annotations"] = [{
                    "sas_lines": [1, sas_code_lines],
                    "pyspark_lines": [1, 2],
                    "note": f"Error during conversion: {str(e)}",
                    "severity": "Error"
                }]
        else:
            # Add code for the Complex Conversion Path here...
            pass
        
        # Convert the list of refined code sections to a single string
        pyspark_code = "\n\n".join(state["refined_pyspark_sections"])
        
        # Move imports to the top and remove duplicates if there are multiple chunks
        if len(state["refined_pyspark_sections"]) > 1:
            pyspark_code = basic_lint_check(pyspark_code)[0]
        
        # Prepare response data
        response_data = {
            'status': 'success',
            'pyspark_code': pyspark_code,
            'annotations': state["annotations"],
            'warnings': state["warnings"],
            'strategy_used': state["strategy"],
            'processing_stats': {
                'total_duration_seconds': time.time() - state["processing_stats"]["start_time"],
                'chunks_processed': state["processing_stats"]["chunks_processed"],
                'llm_calls': state["processing_stats"]["llm_calls"],
                'token_usage': token_usage_stats if USE_BEDROCK else None,
                'refinement_attempts': state["processing_stats"]["refinement_attempts"],
                'extreme_mode': state["processing_stats"]["extreme_mode"],
                'simplified_approach': state["processing_stats"]["simplified_approach"]
            },
            'processing_complete': not state["partial_processing"]
        }
        
        # Include confidence score if available
        if "confidence_score" in state["processing_stats"]:
            response_data['processing_stats']['confidence_score'] = state["processing_stats"]["confidence_score"]
        
        response_body = json.dumps(response_data, ensure_ascii=False)
        
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