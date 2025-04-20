import json
import logging
import boto3
import base64
import re
import os
from typing import Dict, List, Tuple, Optional, Any, Set
from botocore.exceptions import ClientError
from botocore.config import Config
import time
import hashlib
from functools import lru_cache
import ast # For basic syntax check
import random # Needed for cache eviction
import concurrent.futures
from datetime import datetime, timedelta
import threading
import copy
import traceback

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Lambda Timeout Protection ---
# Default Lambda timeout is 5 minutes (300 seconds)
LAMBDA_TIMEOUT_SECONDS = int(os.environ.get("LAMBDA_TIMEOUT_SECONDS", "300"))
SAFETY_MARGIN_SECONDS = int(os.environ.get("SAFETY_MARGIN_SECONDS", "30"))
EXECUTION_DEADLINE = None  # Will be set at the start of execution

def set_execution_deadline(context=None):
    """Set execution deadline based on Lambda context or default timeout"""
    global EXECUTION_DEADLINE
    if context and hasattr(context, 'get_remaining_time_in_millis'):
        # Use Lambda context to get accurate remaining time
        remaining_ms = context.get_remaining_time_in_millis()
        EXECUTION_DEADLINE = datetime.now() + timedelta(milliseconds=remaining_ms - (SAFETY_MARGIN_SECONDS * 1000))
    else:
        # Use default timeout values
        EXECUTION_DEADLINE = datetime.now() + timedelta(seconds=LAMBDA_TIMEOUT_SECONDS - SAFETY_MARGIN_SECONDS)
    logger.info(f"Execution deadline set to {EXECUTION_DEADLINE.isoformat()}")

def check_timeout():
    """Check if execution is about to exceed Lambda timeout"""
    if EXECUTION_DEADLINE and datetime.now() >= EXECUTION_DEADLINE:
        logger.warning("Approaching Lambda timeout limit, interrupting execution")
        raise TimeoutError("Execution approaching Lambda timeout limit")

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
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-3-7-sonnet-20250219-v1:0") # Defaulting to Sonnet 3
ANTHROPIC_VERSION = "bedrock-2023-05-31" # Required for Claude 3 models

# Retry and timeout settings
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
RETRY_DELAY_SECONDS = int(os.environ.get("RETRY_DELAY_SECONDS", "3"))
RETRY_BACKOFF_FACTOR = float(os.environ.get("RETRY_BACKOFF_FACTOR", "1.5"))

# Token limits for different code sizes
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "65536"))
MAX_TOKENS_PER_STRATEGY = int(os.environ.get("MAX_TOKENS_PER_STRATEGY", "65536"))
MAX_EXTREME_TOKENS = int(os.environ.get("MAX_EXTREME_TOKENS", "120000"))

# Confidence and refinement settings
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "3.5"))
MAX_REFINEMENT_ATTEMPTS = int(os.environ.get("MAX_REFINEMENT_ATTEMPTS", "3"))
REFINEMENT_THRESHOLD = float(os.environ.get("REFINEMENT_THRESHOLD", "3.0"))

# Code size thresholds
CONTEXT_SIZE_LIMIT = int(os.environ.get("CONTEXT_SIZE_LIMIT", "300000"))
VERY_LARGE_CODE_THRESHOLD = int(os.environ.get("VERY_LARGE_CODE_THRESHOLD", "20000"))
EXTREME_CODE_THRESHOLD = int(os.environ.get("EXTREME_CODE_THRESHOLD", "100000"))

# Feature flags
USE_EXTENDED_THINKING = os.environ.get("USE_EXTENDED_THINKING", "TRUE").upper() == "TRUE"
USE_PARALLEL_PROCESSING = os.environ.get("USE_PARALLEL_PROCESSING", "TRUE").upper() == "TRUE"
USE_INCREMENTAL_CONVERSION = os.environ.get("USE_INCREMENTAL_CONVERSION", "TRUE").upper() == "TRUE"
USE_ADAPTIVE_CHUNKING = os.environ.get("USE_ADAPTIVE_CHUNKING", "TRUE").upper() == "TRUE"
USE_TOKEN_OPTIMIZATION = os.environ.get("USE_TOKEN_OPTIMIZATION", "TRUE").upper() == "TRUE"

# Complexity Analyzer Thresholds
SIMPLE_MAX_LINES = int(os.environ.get("SIMPLE_MAX_LINES", "1000"))
SIMPLE_MAX_MACROS = int(os.environ.get("SIMPLE_MAX_MACROS", "10"))
SIMPLE_MAX_PROCS = int(os.environ.get("SIMPLE_MAX_PROCS", "20"))

# Caching Configuration
USE_LLM_CACHING = os.environ.get("USE_LLM_CACHING", "TRUE").upper() == "TRUE"
MAX_CACHE_ENTRIES = int(os.environ.get("MAX_CACHE_ENTRIES", "200"))
# Remove CACHE_TTL_HOURS as it's not used
# CACHE_TTL_HOURS = int(os.environ.get("CACHE_TTL_HOURS", "24"))  # New parameter for cache time-to-live
llm_response_cache = {}

# Define error handling retry statuses for Bedrock
BEDROCK_RETRY_EXCEPTIONS = [
    'ThrottlingException',
    'ServiceUnavailableException',
    'InternalServerException',
    'ModelTimeoutException'
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
    'context_focus': 0,
    'chunk_conversion': 0,
    'refinement': 0,
    'summarization': 0
}

# Add global variables for token rate limiting at the top with other globals
LAST_API_CALL_TIME = 0
TOKEN_RATE_LIMIT_WINDOW = 1.0  # seconds
TOKEN_RATE_PER_SECOND = 10000  # tokens per second (approximate AWS Bedrock limit)
ACCUMULATED_TOKENS = 0

def assess_complexity(sas_code: str, thresholds: Optional[Dict[str, int]] = None) -> str:
    """
    Analyze SAS code complexity to determine whether to use Simple or Complex conversion path.
    """
    # Set default thresholds if not provided
    if thresholds is None:
        thresholds = {
            'SIMPLE_MAX_LINES': int(os.environ.get("SIMPLE_MAX_LINES", "20")),
            'SIMPLE_MAX_MACROS': int(os.environ.get("SIMPLE_MAX_MACROS", "2")),
            'SIMPLE_MAX_PROCS': int(os.environ.get("SIMPLE_MAX_PROCS", "3")),
            'SIMPLE_MAX_MACRO_CALLS': int(os.environ.get("SIMPLE_MAX_MACRO_CALLS", "2"))
        }
    
    # Calculate complexity metrics
    loc = sas_code.count('\n') + 1
    char_count = len(sas_code)
    macro_defs = len(re.findall(r'^\s*%MACRO\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))
    proc_calls = len(re.findall(r'^\s*PROC\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))
    macro_calls = len(re.findall(r'%\w+\(.*?\)', sas_code))
    has_complex_proc = bool(re.search(r'^\s*PROC\s+(FCMP|LUA|PROTO|OPTMODEL|IML)\b', 
                                    sas_code, 
                                    re.IGNORECASE | re.MULTILINE))
    has_includes = bool(re.search(r'^\s*%INCLUDE\b', sas_code, re.IGNORECASE | re.MULTILINE))
    
    # Log the complexity metrics
    logger.info(f"SAS Complexity Metrics - Lines: {loc}, Chars: {char_count}, "
                f"Macros: {macro_defs}, PROCs: {proc_calls}, Macro Calls: {macro_calls}, "
                f"Has Complex PROCs: {has_complex_proc}, Has Includes: {has_includes}")
    
    # Classification Rule
    is_simple = (
        loc <= thresholds['SIMPLE_MAX_LINES'] and
        macro_defs <= thresholds['SIMPLE_MAX_MACROS'] and
        proc_calls <= thresholds['SIMPLE_MAX_PROCS'] and
        macro_calls <= thresholds['SIMPLE_MAX_MACRO_CALLS'] and
        not has_complex_proc and
        not has_includes
    )
    
    result = "Simple" if is_simple else "Complex"
    logger.info(f"Complexity assessment result: {result}")
    
    return result

class SASCodeChunker:
    """Handles intelligent chunking of SAS code based on logical boundaries."""
    
    def __init__(self, max_chunk_size=1000):
        self.max_chunk_size = max_chunk_size
        self.section_patterns = {
            'PROC': r'PROC\s+\w+\s+[^;]+;',
            'DATA': r'DATA\s+[^;]+;',
            'MACRO': r'%MACRO\s+[^;]+;',
            'SECTION': r'/\*.*?\*/',
            'COMMENT': r'/\*.*?\*/'
        }
    
    def identify_major_sections(self, sas_code):
        """Identify major sections of SAS code."""
        sections = []
        seen_sections = set()  # Track unique section content
        
        # First try to find PROC and DATA steps
        for section_type, pattern in self.section_patterns.items():
            matches = re.finditer(pattern, sas_code, re.DOTALL | re.IGNORECASE)
            for match in matches:
                section_content = match.group(0).strip()
                if section_content not in seen_sections:
                    seen_sections.add(section_content)
                    sections.append({
                        'name': f"{section_type} step",
                        'type': section_type,
                        'code': section_content,
                        'dependencies': self._extract_dependencies(section_content)
                    })
        
        # If no sections found, create a single section
        if not sections:
            sections.append({
                'name': 'Main section',
                'type': 'SECTION',
                'code': sas_code,
                'dependencies': []
            })
        
        return sections
    
    def _extract_dependencies(self, section_code):
        """Extract dependencies from a section of code."""
        dependencies = []
        
        # Look for dataset references
        dataset_refs = re.finditer(r'DATA\s+([^;]+)', section_code, re.IGNORECASE)
        for ref in dataset_refs:
            dependencies.append(ref.group(1).strip())
        
        # Look for SET/MERGE statements
        set_refs = re.finditer(r'(SET|MERGE)\s+([^;]+)', section_code, re.IGNORECASE)
        for ref in set_refs:
            dependencies.extend(d.strip() for d in ref.group(2).split())
        
        return list(set(dependencies))  # Remove duplicates
    
    def identify_chunks(self, sas_code: str) -> List[Dict[str, Any]]:
        """
        Identifies logical chunks in SAS code, combining related operations into larger chunks.
        Returns a maximum of 3 chunks, each containing related PROC/DATA/MACRO steps.
        """
        # First, find all potential chunks
        chunks = []
        patterns = {
            'PROC': r'PROC\s+(\w+)\s+[^;]*;.*?(?:QUIT|RUN);',
            'DATA': r'DATA\s+([^;]*);.*?RUN;',
            'MACRO': r'%MACRO\s+(\w+)\s*\([^)]*\).*?%MEND\s*\1;'
        }
        
        # Find all potential chunks
        for chunk_type, pattern in patterns.items():
            matches = re.finditer(pattern, sas_code, re.DOTALL | re.IGNORECASE)
            for match in matches:
                chunk_name = match.group(1).strip()
                chunk_content = match.group(0)
                
                # If chunk_name contains multiple datasets (for DATA step), use the first one
                if ',' in chunk_name:
                    chunk_name = chunk_name.split(',')[0].strip()
                
                chunks.append({
                    'type': chunk_type,
                    'name': chunk_name,
                    'content': chunk_content,
                    'code': chunk_content,
                    'dependencies': self._extract_dependencies(chunk_content, chunk_type)
                })
        
        # If no chunks found, create a single generic chunk
        if not chunks and sas_code.strip():
            return [{
                'type': 'GENERIC',
                'name': 'main_code',
                'content': sas_code,
                'code': sas_code,
                'dependencies': []
            }]
        
        # If we have 3 or fewer chunks, return them as is
        if len(chunks) <= 3:
            logger.info(f"Identified {len(chunks)} logical chunks in SAS code")
            return chunks
        
        # Group chunks by type and dependencies
        chunk_groups = {
            'DATA': [],
            'PROC': [],
            'MACRO': [],
            'OTHER': []
        }
        
        for chunk in chunks:
            chunk_groups[chunk['type']].append(chunk)
        
        # Create metadata about chunk relationships
        chunk_metadata = {
            'total_chunks': len(chunks),
            'chunk_types': {k: len(v) for k, v in chunk_groups.items()},
            'dependencies': {}
        }
        
        # Analyze dependencies between chunks
        for chunk in chunks:
            chunk_metadata['dependencies'][chunk['name']] = {
                'type': chunk['type'],
                'depends_on': chunk['dependencies'],
                'size': len(chunk['content'])
            }
        
        # Combine chunks into 3 logical sections
        combined_chunks = []
        
        # First chunk: All DATA steps and their immediate dependencies
        data_chunk = {
            'type': 'COMBINED',
            'name': 'data_processing_section',
            'content': '',
            'code': '',
            'dependencies': [],
            'metadata': {
                'contains': ['DATA'],
                'description': 'Data processing and transformation steps'
            }
        }
        
        # Second chunk: All PROC steps and their immediate dependencies
        proc_chunk = {
            'type': 'COMBINED',
            'name': 'proc_analysis_section',
            'content': '',
            'code': '',
            'dependencies': [],
            'metadata': {
                'contains': ['PROC'],
                'description': 'Statistical analysis and reporting procedures'
            }
        }
        
        # Third chunk: All MACRO definitions and remaining code
        macro_chunk = {
            'type': 'COMBINED',
            'name': 'macro_and_utility_section',
            'content': '',
            'code': '',
            'dependencies': [],
            'metadata': {
                'contains': ['MACRO', 'OTHER'],
                'description': 'Macro definitions and utility functions'
            }
        }
        
        # Process each group
        for group_type, group_chunks in chunk_groups.items():
            target_chunk = None
            if group_type == 'DATA':
                target_chunk = data_chunk
            elif group_type == 'PROC':
                target_chunk = proc_chunk
            else:
                target_chunk = macro_chunk
            
            # Sort chunks by dependencies
            sorted_chunks = sorted(group_chunks, 
                                 key=lambda x: len(x['dependencies']))
            
            # Add chunks to target
            for chunk in sorted_chunks:
                target_chunk['content'] += '\n\n' + chunk['content']
                target_chunk['code'] += '\n\n' + chunk['code']
                target_chunk['dependencies'].extend(chunk['dependencies'])
                target_chunk['metadata']['contains'].append(chunk['type'])
        
        # Add chunks that have content
        for chunk in [data_chunk, proc_chunk, macro_chunk]:
            if chunk['content'].strip():
                # Add metadata about size and dependencies
                chunk['metadata']['size'] = len(chunk['content'])
                chunk['metadata']['dependency_count'] = len(chunk['dependencies'])
                chunk['metadata']['unique_dependencies'] = list(set(chunk['dependencies']))
                combined_chunks.append(chunk)
        
        logger.info(f"Combined into {len(combined_chunks)} logical chunks with metadata")
        return combined_chunks
    
    def _extract_dependencies(self, chunk_content: str, chunk_type: str) -> List[str]:
        """
        Extracts dependencies from a SAS code chunk.
        """
        dependencies = []
        
        # Look for dataset references
        if chunk_type == 'DATA':
            # Look for SET, MERGE, UPDATE statements
            dataset_refs = re.findall(r'SET\s+([^;]+);|MERGE\s+([^;]+);|UPDATE\s+([^;]+);', 
                                     chunk_content, re.IGNORECASE)
            
            for ref in dataset_refs:
                # Take the first non-empty match
                dataset = next((r for r in ref if r), None)
                if dataset:
                    # Handle multiple datasets in one statement
                    datasets = [d.strip() for d in dataset.split(',')]
                    dependencies.extend(datasets)
        
        elif chunk_type == 'PROC':
            # Look for DATA= options in PROC statements
            data_refs = re.findall(r'DATA\s*=\s*([^(\s;)]+)', chunk_content, re.IGNORECASE)
            dependencies.extend([d.strip() for d in data_refs])
        
        # Handle macro calls for all chunk types
        macro_calls = re.findall(r'%(\w+)\s*\(', chunk_content)
        for macro in macro_calls:
            if macro != 'IF' and macro != 'DO' and macro != 'END':  # Skip control flow macros
                dependencies.append(macro)
        
        return dependencies

class SASContextManager:
    """
    Manages context and state during SAS to PySpark conversion process.
    Provides methods to create focused context for each chunk and update context based on results.
    """
    def __init__(self):
        self.context = {
            'schema_info': {},       # Dataset schemas
            'macros_defined': {},    # Macro definitions
            'dependencies': set(),   # Dependencies between chunks
            'converted_chunks': {},  # Already converted chunks
            'global_vars': set()     # Global variables
        }
        # Remove lock flag to avoid serialization issues
        # self._lock_flag = False  # Simple synchronization without using thread.RLock
    
    def get_focused_context(self, chunk):
        """
        Creates a focused context for a specific chunk.
        """
        if not chunk:
            logger.warning("Empty chunk provided to get_focused_context")
            return {}
            
        try:
            chunk_name = chunk.get('name', 'unknown')
            chunk_type = chunk.get('type', 'UNKNOWN')
            chunk_deps = chunk.get('depends_on', [])
            
            # Create a copy of relevant context
            # Use deepcopy to avoid any references to the original objects
            focused_context = {
                'schema_info': {},
                'macros_defined': {},
                'dependencies': list(chunk_deps),
                'global_vars': list(self.context['global_vars'])
            }
            
            # Include relevant schemas
            if chunk_deps:
                for dep in chunk_deps:
                    if dep in self.context['schema_info']:
                        focused_context['schema_info'][dep] = self.context['schema_info'][dep]
            
            # Include macros if this is not a macro definition
            if chunk_type != 'MACRO':
                focused_context['macros_defined'] = copy.deepcopy(self.context['macros_defined'])
            
            # Include already converted chunks that this chunk depends on
            for dep in chunk_deps:
                if dep in self.context['converted_chunks']:
                    if 'converted_chunks' not in focused_context:
                        focused_context['converted_chunks'] = {}
                    focused_context['converted_chunks'][dep] = copy.deepcopy(self.context['converted_chunks'][dep])
            
            return focused_context
        except Exception as e:
            logger.error(f"Error creating focused context: {e}")
            return {}
    
    def update_context(self, chunk, pyspark_code, annotations):
        """
        Updates the context with the results of a chunk conversion.
        """
        if not chunk:
            logger.warning("Empty chunk provided to update_context")
            return self.context
            
        try:
            chunk_name = chunk.get('name', 'unknown')
            chunk_type = chunk.get('type', 'UNKNOWN')
            
            # Create a copy to avoid modifying the original context
            updated_context = {}
            
            # Copy existing context elements, converting sets to lists where needed to avoid serialization issues
            for key, value in self.context.items():
                if isinstance(value, dict):
                    updated_context[key] = copy.deepcopy(value)
                elif isinstance(value, set):
                    # Convert sets to lists for better serialization
                    updated_context[key] = list(value)
                else:
                    updated_context[key] = copy.deepcopy(value)
            
            # Update schema information for DATA steps
            if chunk_type == 'DATA':
                output_dataset = chunk_name
                
                # Extract schema from annotations and code
                schema = self._extract_schema_from_result(pyspark_code, annotations)
                
                if schema:
                    if 'schema_info' not in updated_context:
                        updated_context['schema_info'] = {}
                    updated_context['schema_info'][output_dataset] = schema
                
                # Store the converted chunk
                if 'converted_chunks' not in updated_context:
                    updated_context['converted_chunks'] = {}
                updated_context['converted_chunks'][chunk_name] = {
                    'pyspark_code': pyspark_code,
                    'type': chunk_type
                }
            
            # Update macro definitions for MACRO chunks
            elif chunk_type == 'MACRO':
                # Store the macro definition
                if 'macros_defined' not in updated_context:
                    updated_context['macros_defined'] = {}
                updated_context['macros_defined'][chunk_name] = {
                    'pyspark_code': pyspark_code,
                    'parameters': self._extract_macro_params(pyspark_code)
                }
                
                # Store the converted chunk
                if 'converted_chunks' not in updated_context:
                    updated_context['converted_chunks'] = {}
                updated_context['converted_chunks'][chunk_name] = {
                    'pyspark_code': pyspark_code,
                    'type': chunk_type
                }
            
            # For other chunk types, just store the converted code
            else:
                if 'converted_chunks' not in updated_context:
                    updated_context['converted_chunks'] = {}
                updated_context['converted_chunks'][chunk_name] = {
                    'pyspark_code': pyspark_code,
                    'type': chunk_type
                }
            
            # Extract and store global variables
            global_vars = self._extract_global_vars(pyspark_code)
            if global_vars:
                if 'global_vars' not in updated_context:
                    updated_context['global_vars'] = set()
                # Check if global_vars is a list or a set
                if isinstance(updated_context['global_vars'], list):
                    updated_context['global_vars'].extend(list(global_vars))
                    # Remove duplicates
                    updated_context['global_vars'] = list(set(updated_context['global_vars']))
                else:
                    updated_context['global_vars'].update(global_vars)
                    
            logger.debug(f"Updated context for chunk {chunk_name} (type: {chunk_type})")
            
            # Update the context
            self.context = updated_context
            return self.context
            
        except Exception as e:
            logger.error(f"Error updating context: {e}")
            return self.context
    
    def _extract_schema_from_result(self, pyspark_code, annotations):
        """
        Extracts schema information from PySpark code.
        """
        schema = {}
        
        # Look for schema definitions in the code
        schema_pattern = r'\.schema\(StructType\(\[\s*(.*?)\s*\]\)\)'
        matches = re.findall(schema_pattern, pyspark_code, re.DOTALL)
        
        if matches:
            # Extract field definitions
            field_pattern = r'StructField\([\'"](\w+)[\'"],\s*(\w+)\(\)'
            for schema_def in matches:
                fields = re.findall(field_pattern, schema_def)
                for field_name, field_type in fields:
                    schema[field_name] = field_type
        
        # Look for withColumn or select statements
        column_pattern = r'\.withColumn\([\'"](\w+)[\'"],'
        col_matches = re.findall(column_pattern, pyspark_code)
        for col_name in col_matches:
            if col_name not in schema:
                schema[col_name] = "unknown"
        
        return schema
    
    def _extract_macro_params(self, pyspark_code):
        """
        Extracts parameter information from a converted macro.
        """
        params = []
        
        # Look for function definition
        func_pattern = r'def\s+(\w+)\s*\((.*?)\):'
        matches = re.search(func_pattern, pyspark_code, re.DOTALL)
        
        if matches:
            # Extract parameter list
            param_list = matches.group(2)
            if param_list:
                # Split by comma and clean up
                params = [p.strip().split('=')[0].strip() for p in param_list.split(',')]
        
        return params
    
    def _extract_global_vars(self, pyspark_code):
        """
        Extracts global variables from PySpark code.
        """
        global_vars = set()
        
        # Look for global variable assignments
        global_pattern = r'(\w+)\s*=\s*spark\.sparkContext\.broadcast\('
        matches = re.findall(global_pattern, pyspark_code)
        
        global_vars.update(matches)
        
        return global_vars

class SASConverter:
    """
    Main class for converting SAS code to PySpark.
    """
    
    def __init__(self, sas_code=None, config=None):
        self.chunker = SASCodeChunker()
        self.context_manager = SASContextManager()
        self.processed_chunks = set()
        self.dependency_graph = {}
        self.sas_code = sas_code
        self.config = config or {}
    
    def assess_complexity(self, sas_code: str) -> Dict[str, Any]:
        """
        Analyze SAS code complexity to determine whether to use Simple or Complex conversion path.
        Returns a dictionary with complexity assessment results.
        """
        # Calculate complexity metrics
        loc = sas_code.count('\n') + 1
        char_count = len(sas_code)
        macro_defs = len(re.findall(r'^\s*%MACRO\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))
        proc_calls = len(re.findall(r'^\s*PROC\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))
        macro_calls = len(re.findall(r'%\w+\(.*?\)', sas_code))
        has_complex_proc = bool(re.search(r'^\s*PROC\s+(FCMP|LUA|PROTO|OPTMODEL|IML)\b', 
                                        sas_code, 
                                        re.IGNORECASE | re.MULTILINE))
        has_includes = bool(re.search(r'^\s*%INCLUDE\b', sas_code, re.IGNORECASE | re.MULTILINE))
        
        # Log the complexity metrics
        logger.info(f"SAS Complexity Metrics - Lines: {loc}, Chars: {char_count}, "
                    f"Macros: {macro_defs}, PROCs: {proc_calls}, Macro Calls: {macro_calls}, "
                    f"Has Complex PROCs: {has_complex_proc}, Has Includes: {has_includes}")
        
        # Set thresholds
        thresholds = {
            'SIMPLE_MAX_LINES': int(os.environ.get("SIMPLE_MAX_LINES", "1000")),
            'SIMPLE_MAX_MACROS': int(os.environ.get("SIMPLE_MAX_MACROS", "10")),
            'SIMPLE_MAX_PROCS': int(os.environ.get("SIMPLE_MAX_PROCS", "20")),
            'SIMPLE_MAX_MACRO_CALLS': int(os.environ.get("SIMPLE_MAX_MACRO_CALLS", "5"))
        }
        
        # Determine if code is simple
        is_simple = (
            loc <= thresholds['SIMPLE_MAX_LINES'] and
            macro_defs <= thresholds['SIMPLE_MAX_MACROS'] and
            proc_calls <= thresholds['SIMPLE_MAX_PROCS'] and
            macro_calls <= thresholds['SIMPLE_MAX_MACRO_CALLS'] and
            not has_complex_proc and
            not has_includes
        )
        
        return {
            'is_simple': is_simple,
            'metrics': {
                'lines_of_code': loc,
                'character_count': char_count,
                'macro_definitions': macro_defs,
                'proc_calls': proc_calls,
                'macro_calls': macro_calls,
                'has_complex_proc': has_complex_proc,
                'has_includes': has_includes
            },
            'thresholds': thresholds
        }
    
    def convert(self, sas_code: str) -> Dict[str, Any]:
        """
        Main conversion method that handles the conversion process.
        """
        # First assess complexity
        complexity_assessment = self.assess_complexity(sas_code)
        
        if complexity_assessment['is_simple']:
            return self._simple_conversion(sas_code)
        else:
            return self._complex_conversion(sas_code)
    
    def _simple_conversion(self, sas_code: str) -> Dict[str, Any]:
        """
        Simple conversion path for straightforward SAS code.
        """
        logger.info("Using Simple Conversion Path")
        
        prompt = f"""Convert this SAS code to PySpark:
```sas
{sas_code}
```

Return a JSON object with:
1. pyspark_code: The converted code
2. annotations: Conversion notes
3. confidence: Score from 1-5
"""
        
        response = _call_llm_with_retry(
            lambda p: _invoke_claude37_sonnet(p),
            prompt,
            description="Simple SAS to PySpark conversion"
        )
        
        try:
            result = json.loads(response)
            return {
                'pyspark_code': result['pyspark_code'],
                'annotations': result['annotations'],
                'confidence_score': result.get('confidence', 3.0)
            }
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response: {e}")
            raise ValueError("Invalid LLM response format")
    
    def _complex_conversion(self, sas_code: str) -> Dict[str, Any]:
        """
        Complex conversion path for more intricate SAS code.
        """
        logger.info("Using Complex Multi-Stage conversion path")
        
        # Step 1: Analyze and chunk the code
        chunks = self.chunker.identify_chunks(sas_code)
        
        # Step 2: Determine processing order based on dependencies
        processing_order = self.determine_processing_order(chunks)
        
        # Step 3: Process each chunk in order
        results = []
        context = {}
        
        # Process each chunk with context
        for chunk_name in processing_order:
            chunk = next((c for c in chunks if c['name'] == chunk_name), None)
            if not chunk:
                logger.warning(f"Chunk {chunk_name} not found, skipping")
                continue
                
            logger.info(f"Processing chunk: {chunk_name}")
            
            try:
                # Get focused context for this chunk
                focused_context = self.context_manager.get_focused_context(
                    chunk, context, sas_code
                )
                
                # Convert chunk with optimized prompting
                pyspark_code, annotations, confidence, notes = call_llm_for_conversion(
                    chunk.get('content', chunk.get('code', '')),
                    focused_context,
                    chunk.get('type', 'UNKNOWN'),
                    chunk.get('strategy')
                )
                
                # Update context with results
                try:
                    context = self.context_manager.update_context(
                        chunk, pyspark_code, annotations
                    )
                except Exception as context_err:
                    logger.error(f"Error updating context: {context_err}")
                    # Continue with processing even if context update fails
                
                results.append({
                    'name': chunk_name,
                    'pyspark_code': pyspark_code,
                    'annotations': annotations,
                    'confidence_score': confidence,
                    'refinement_notes': notes
                })
                
            except Exception as e:
                logger.error(f"Error processing chunk {chunk_name}: {e}")
                # Add error result
                results.append({
                    'name': chunk_name,
                    'pyspark_code': f"# Error processing chunk {chunk_name}: {str(e)}\n",
                    'annotations': [{'note': f"Error: {str(e)}", 'severity': 'Error'}],
                    'confidence_score': 1.0,
                    'refinement_notes': f"Failed to process chunk: {str(e)}"
                })
        
        # Combine results
        combined_result = _combine_results(results)
        
        # Log final result
        logger.info(f"Conversion completed: {len(results)} chunks processed")
        logger.info(f"Final confidence score: {combined_result.get('confidence_score', 0.0)}")
        logger.debug(f"Final PySpark code (first 500 chars): {combined_result.get('pyspark_code', '')[:500]}...")
        
        return combined_result
    
    def determine_processing_order(self, chunks: List[Dict[str, Any]]) -> List[str]:
        """
        Determines the order in which chunks should be processed based on dependencies.
        Uses topological sort to handle dependencies correctly.
        """
        # Reset tracking variables
        self.processed_chunks = set()
        self.dependency_graph = {}
        
        # Build dependency graph
        for chunk in chunks:
            chunk_name = chunk['name']
            self.dependency_graph[chunk_name] = set()
            
            # Add dependencies
            if 'dependencies' in chunk:
                for dep in chunk['dependencies']:
                    # Check if dependency exists in chunks
                    if any(c['name'] == dep for c in chunks):
                        self.dependency_graph[chunk_name].add(dep)
        
        # Perform topological sort
        processing_order = []
        max_depth = 100  # Prevent infinite recursion
        
        def visit(chunk_name: str, visiting: Set[str], depth: int = 0):
            """Helper function for topological sort with cycle detection and depth limit"""
            # Check timeout periodically
            if depth % 10 == 0:  # Check every 10 levels of recursion
                check_timeout()
            
            # Check depth to prevent infinite recursion
            if depth > max_depth:
                logger.warning(f"Maximum recursion depth reached for {chunk_name}, possible circular dependency")
                return
            
            if chunk_name in self.processed_chunks:
                return
            if chunk_name in visiting:
                logger.warning(f"Circular dependency detected involving {chunk_name}")
                return
            
            visiting.add(chunk_name)
            
            # Process dependencies first
            for dep in self.dependency_graph.get(chunk_name, []):
                visit(dep, visiting, depth + 1)
            
            visiting.remove(chunk_name)
            self.processed_chunks.add(chunk_name)
            processing_order.append(chunk_name)
        
        # Process all chunks
        visiting = set()
        for chunk in chunks:
            if chunk['name'] not in self.processed_chunks:
                visit(chunk['name'], visiting)
        
        logger.info(f"Determined processing order: {processing_order}")
        return processing_order

def cache_key(prompt, model_id=None):
    """Generate a cache key from the prompt and optional model ID"""
    key_content = f"{prompt}:{model_id or BEDROCK_MODEL_ID}"
    return hashlib.md5(key_content.encode('utf-8')).hexdigest()

def _call_llm_with_retry(bedrock_invoke_function, prompt, description="LLM call", bypass_cache=False):
    """
    Wrapper for retry logic specifically around Bedrock invoke_model calls.
    Now includes caching to avoid repeated identical calls and exponential backoff for retries.
    Also includes token-aware rate limiting to prevent throttling.
    """
    global LAST_API_CALL_TIME, ACCUMULATED_TOKENS
    
    check_timeout()  # Check timeout before LLM call
    
    # Log initial metrics
    start_time = time.time()
    prompt_tokens = len(prompt.split()) // 2  # Rough estimate of tokens
    
    logger.info(f"Starting {description}")
    logger.info(f"Estimated prompt tokens: {prompt_tokens}")
    logger.info(f"Cache enabled: {USE_LLM_CACHING and not bypass_cache}")
    
    # Check cache first if enabled
    if USE_LLM_CACHING and not bypass_cache:
        key = cache_key(prompt)
        if key in llm_response_cache:
            logger.info(f"Cache hit for {description}! Using cached response.")
            logger.info(f"Cache size: {len(llm_response_cache)} entries")
            return llm_response_cache[key]

    # Not in cache or bypass_cache is True, proceed with API call
    last_exception = None
    
    # Calculate max retries based on remaining time
    max_retries = min(MAX_RETRIES, 2)  # Default to a lower value for safety
    if EXECUTION_DEADLINE:
        remaining_seconds = (EXECUTION_DEADLINE - datetime.now()).total_seconds()
        time_based_retries = max(0, int(remaining_seconds / 10) - 1)
        max_retries = min(MAX_RETRIES, time_based_retries)
    
    logger.info(f"Maximum retries set to {max_retries}")
    
    for attempt in range(max_retries + 1):
        try:
            check_timeout()  # Check before each attempt
            logger.info(f"Attempting {description} (Attempt {attempt + 1}/{max_retries + 1})...")
            
            # Token-aware rate limiting
            now = time.time()
            time_since_last_call = now - LAST_API_CALL_TIME
            
            # Reset accumulated tokens if enough time has passed
            if time_since_last_call > TOKEN_RATE_LIMIT_WINDOW:
                ACCUMULATED_TOKENS = 0
            else:
                # Calculate how many tokens we can send now
                available_tokens = TOKEN_RATE_PER_SECOND * time_since_last_call
                tokens_to_wait_for = max(0, prompt_tokens - available_tokens)
                
                if tokens_to_wait_for > 0:
                    # Need to wait to avoid throttling
                    wait_time = tokens_to_wait_for / TOKEN_RATE_PER_SECOND
                    logger.info(f"Rate limiting: Waiting {wait_time:.2f}s to avoid throttling")
                    time.sleep(wait_time)
            
            # Update token accounting
            ACCUMULATED_TOKENS = prompt_tokens
            LAST_API_CALL_TIME = time.time()
            
            # Call the actual invoke function
            result = bedrock_invoke_function(prompt)
            
            # Calculate and log metrics
            end_time = time.time()
            duration = end_time - start_time
            
            # Parse response for token usage - use a more robust approach
            try:
                if isinstance(result, str):
                    try:
                        response_body = json.loads(result)
                    except json.JSONDecodeError:
                        # For non-JSON responses, estimate tokens based on length
                        input_tokens = prompt_tokens  # Use our estimate
                        output_tokens = len(result.split()) // 2
                        logger.info(f"Token usage: {input_tokens} input + {output_tokens} output = {input_tokens + output_tokens} total tokens")
                        logger.info(f"Response time: {duration:.2f}s")
                else:
                    # If result is already parsed
                    response_body = result
                
                # Try to extract token usage if available
                usage = response_body.get('usage', {}) if isinstance(response_body, dict) else {}
                input_tokens = usage.get('input_tokens', prompt_tokens)
                output_tokens = usage.get('output_tokens', len(str(response_body).split()) // 2)
                total_tokens = input_tokens + output_tokens
                
                # Calculate cost (Claude 3 Sonnet pricing as of April 2024)
                input_cost = input_tokens * 0.000003  # $0.003 per 1K input tokens
                output_cost = output_tokens * 0.000015  # $0.015 per 1K output tokens
                total_cost = input_cost + output_cost
                
                logger.info(f"Token usage: {input_tokens} input + {output_tokens} output = {total_tokens} total tokens")
                logger.info(f"Response time: {duration:.2f}s")
                
                # Update global statistics
                token_usage_stats['total_tokens'] += total_tokens
                token_usage_stats['input_tokens'] += input_tokens
                token_usage_stats['output_tokens'] += output_tokens
                token_usage_stats['api_calls'] += 1
                
            except Exception as e:
                logger.warning(f"Could not parse token usage from response: {e}")

            # Store in cache if caching is enabled
            if USE_LLM_CACHING and not bypass_cache:
                key = cache_key(prompt)
                llm_response_cache[key] = result
                
                # Log cache statistics
                logger.info(f"Cache statistics:")
                logger.info(f"- Current size: {len(llm_response_cache)} entries")
                logger.info(f"- Max size: {MAX_CACHE_ENTRIES} entries")
                
                # Manage cache size
                if len(llm_response_cache) > MAX_CACHE_ENTRIES:
                    # Simple strategy: remove a random entry when full
                    random_key = random.choice(list(llm_response_cache.keys()))
                    del llm_response_cache[random_key]
                    logger.info(f"Cache full, removed random entry to make space.")

            return result # Success
            
        except Exception as e:
            last_exception = e # Store the last exception
            error_name = type(e).__name__
            error_msg = str(e)
            logger.warning(f"{description} attempt {attempt + 1} failed: {error_msg}")

            # Check if the error is retryable based on error type
            is_retryable = error_name in BEDROCK_RETRY_EXCEPTIONS
            
            if attempt < max_retries and is_retryable:
                # Calculate delay with exponential backoff
                delay = RETRY_DELAY_SECONDS * (RETRY_BACKOFF_FACTOR ** attempt)
                
                # Make sure we don't exceed our timeout
                if EXECUTION_DEADLINE:
                    max_safe_delay = max(0, (EXECUTION_DEADLINE - datetime.now()).total_seconds() - 5)
                    delay = min(delay, max_safe_delay)
                    if delay <= 0:
                        logger.warning(f"Not enough time left for retry, aborting")
                        raise TimeoutError("Not enough time left to retry LLM call")
                
                logger.info(f"Retrying {description} in {delay:.1f} seconds (Retryable error: {error_name})...")
                time.sleep(delay)
            else:
                logger.error(f"{description} failed after {attempt + 1} attempts.")
                raise last_exception # Re-raise the last exception after final attempt or if not retryable

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
        return """{"pyspark_code": "# Demo mode - Bedrock not available\\n\\nfrom pyspark.sql.functions import col, lit\\n\\n# This is a demo conversion since Bedrock is not available\\ndef demo_function():\\n    print('This is a demo conversion')\\n    \\n# Simulated dataframe operations\\n# Assuming input_df exists\\n# output_df = input_df.filter(col('category') == 'A')\\n# output_df = output_df.withColumn('new_var', col('old_var') * 2)\\n", "annotations": [{"sas_lines": [1, 5], "pyspark_lines": [1, 10], "note": "Demo conversion - Bedrock is not available", "severity": "Info"}], "confidence": 3.0}"""

    # Set up messages format
    messages = []
    if message_history:
        messages.extend(message_history)
    # Add the new user message
    messages.append({"role": "user", "content": prompt})

    # Configure request body with Claude 3.7 specific parameters
    body = {
        "anthropic_version": ANTHROPIC_VERSION,
        "max_tokens": MAX_TOKENS_PER_STRATEGY,  # Use MAX_TOKENS_PER_STRATEGY directly
        "messages": messages,
        "temperature": float(os.environ.get("LLM_TEMPERATURE", "0.7")),
        "top_p": float(os.environ.get("LLM_TOP_P", "0.9")),
    }

    logger.info(f"Invoking Claude 3.7 Sonnet with max_tokens={MAX_TOKENS_PER_STRATEGY}")
    start_time = time.time()

    response = bedrock_runtime.invoke_model(
        body=json.dumps(body),
        modelId=BEDROCK_MODEL_ID,
        accept='application/json',
        contentType='application/json'
    )

    response_time = time.time() - start_time
    response_body = json.loads(response['body'].read().decode('utf-8')) # Decode body

    # Extract content from the response (specific to Claude 3.7 Messages API)
    if response_body.get("type") == "message" and response_body.get("content"):
        # Find the first text block in the content list
        for block in response_body["content"]:
             if block.get("type") == "text":
                 llm_output = block["text"]
                 return llm_output
        # If no text block found (shouldn't happen with current models)
        logger.error(f"No text content block found in Bedrock response: {response_body}")
        raise ValueError("No text content block found in Bedrock response")
    else:
        logger.error(f"Unexpected Bedrock response format or error type: {response_body}")
        # Try to extract error message if available
        error_details = response_body.get("error", {}).get("message", "Unknown error structure")
        raise ValueError(f"Unexpected response format from Bedrock Claude 3.7: {error_details}")

# --- Main Conversion Function ---
def convert_sas_to_pyspark(sas_code, config=None):
    """Convert SAS code to PySpark code using the appropriate conversion path.
    
    Parameters:
    -----------
    sas_code : str
        SAS code to convert
    config : dict
        Optional configuration dictionary
        
    Returns:
    --------
    dict
        Dictionary containing the PySpark code, annotations, and confidence score
    """
    start_time = time.time()
    
    # Log initial metrics
    logger.info(f"Starting SAS to PySpark conversion")
    logger.info(f"Input size: {len(sas_code):,} characters, {sas_code.count(os.linesep) + 1:,} lines")
    
    # Validate input
    if not sas_code or not sas_code.strip():
        logger.warning("Empty SAS code provided")
        return {
            "pyspark_code": "# No SAS code was provided for conversion",
            "annotations": [{"type": "error", "message": "Empty SAS code provided"}],
            "confidence_score": 0
        }
    
    try:
        # Initialize converter
        converter = SASConverter(sas_code, config)
        
        # Determine conversion path based on code size
        lines_of_code = sas_code.count(os.linesep) + 1
        chars = len(sas_code)
        
        # Choose the appropriate conversion path based on code size
        if lines_of_code <= 50 and chars <= 2500:
            # Direct conversion for very small code
            logger.info("Using direct conversion path for small code")
            result = _direct_conversion(sas_code, converter)
        elif lines_of_code <= 500 and chars <= 25000:
            # Standard conversion for small to medium code
            logger.info("Using standard conversion path for medium-sized code")
            result = _standard_conversion(sas_code, converter)
        else:
            # Use simplified large code conversion for large code
            logger.info("Using simplified large code conversion path for large code")
            result = _large_code_conversion_simplified(sas_code, converter)
        
        # Log result metrics
        duration = time.time() - start_time
        lines_per_second = lines_of_code / duration if duration > 0 else 0
        
        logger.info(f"Conversion completed in {duration:.2f} seconds ({lines_per_second:.2f} lines/sec)")
        logger.info(f"Output metrics: confidence={result.get('confidence_score', 0)}, "
                   f"annotations={len(result.get('annotations', []))}, "
                   f"length={len(result.get('pyspark_code', ''))}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in conversion: {str(e)}")
        logger.error(traceback.format_exc())
        
        return {
            "pyspark_code": f"# Conversion error: {str(e)}",
            "annotations": [{"type": "error", "message": f"Conversion failed: {str(e)}"}],
            "confidence_score": 0
        }

def _standard_conversion(sas_code, converter):
    """
    Standard conversion for moderate-sized SAS code.
    """
    logger.info("\n=== PROGRAM FLOW: Starting Standard Conversion ===")
    logger.info("Entry point: _standard_conversion()")
    
    logger.info("Control Flow: Step 1 - Input Validation")
    if not sas_code or not sas_code.strip():
        logger.info("Control Flow: Empty input detected, returning early")
        return {
            'pyspark_code': "# No SAS code to convert",
            'annotations': [],
            'confidence_score': 0.0,
            'refinement_notes': "Empty input provided"
        }
    
    logger.info("Control Flow: Step 2 - Identifying Logical Sections")
    logger.info("Function call: _identify_logical_sections()")
    code_sections = _identify_logical_sections(sas_code)
    
    if not code_sections:
        logger.info("Control Flow: No sections found, falling back to direct conversion")
        logger.info("Function call: call_llm_for_conversion()")
        result = call_llm_for_conversion(sas_code, {})
        logger.info("Control Flow: Returning from _standard_conversion()")
        return result
    
    logger.info(f"Control Flow: Found {len(code_sections)} sections to process")
    for i, section in enumerate(code_sections, 1):
        logger.info(f"Section {i} details:")
        logger.info(f"- Name: {section.get('name', 'unknown')}")
        logger.info(f"- Type: {section.get('type', 'UNKNOWN')}")
        logger.info(f"- Length: {len(section.get('code', ''))} characters")
        logger.info(f"- Dependencies: {section.get('dependencies', [])}")
    
    logger.info("Control Flow: Step 3 - Processing Sections")
    section_results = []
    context = {}
    
    for i, section in enumerate(code_sections, 1):
        logger.info(f"\nControl Flow: Processing Section {i}/{len(code_sections)}")
        try:
            logger.info("Function call: converter.context_manager.get_focused_context()")
            focused_context = converter.context_manager.get_focused_context(section)
            
            logger.info("Function call: _process_chunk_with_context()")
            result = _process_chunk_with_context(section, focused_context, converter)
            section_results.append(result)
            
            if result and 'pyspark_code' in result:
                logger.info("Function call: converter.context_manager.update_context()")
                context = converter.context_manager.update_context(
                    section, 
                    result['pyspark_code'], 
                    result.get('annotations', [])
                )
                
        except Exception as e:
            logger.error(f"Control Flow: Error in section {i}")
            logger.error(f"Error details: {e}")
            logger.error("Stack trace:", exc_info=True)
            section_results.append({
                'name': section.get('name', 'unknown_section'),
                'pyspark_code': f"# Error processing section: {str(e)}",
                'annotations': [{'note': f"Error: {str(e)}", 'severity': 'Error'}],
                'confidence_score': 0.0,
                'refinement_notes': f"Error during processing: {str(e)}"
            })
    
    logger.info("Control Flow: Step 4 - Combining Results")
    logger.info("Function call: _combine_results()")
    combined_result = _combine_results(section_results)
    
    logger.info("Control Flow: Returning from _standard_conversion()")
    return combined_result

def _process_chunk_with_context(chunk, focused_context, converter):
    """Process a single chunk with the given context"""
    logger.info("\n=== PROGRAM FLOW: Processing Chunk ===")
    logger.info("Entry point: _process_chunk_with_context()")
    
    try:
        logger.info("Control Flow: Step 1 - Extracting Chunk Code")
        chunk_code = chunk.get('content', chunk.get('code', ''))
        chunk_type = chunk.get('type', 'UNKNOWN')
        chunk_name = chunk.get('name', 'unnamed_chunk')
        
        logger.info(f"Processing chunk: {chunk_name} (type: {chunk_type}, size: {len(chunk_code)} chars)")
        
        logger.info("Control Flow: Step 2 - Preparing Context")
        serializable_context = {}
        if isinstance(focused_context, dict):
            logger.info("Converting context to serializable format")
            for key, value in focused_context.items():
                if isinstance(value, dict):
                    serializable_context[key] = copy.deepcopy(value)
                elif isinstance(value, (list, set)):
                    serializable_context[key] = list(value)
                elif isinstance(value, (str, int, float, bool, type(None))):
                    serializable_context[key] = value
                # Skip any complex objects that might cause serialization issues
        
        # Add chunk metadata to context
        serializable_context['chunk_name'] = chunk_name
        serializable_context['chunk_type'] = chunk_type
        
        logger.info("Control Flow: Step 3 - Calling LLM")
        logger.info("Function call: call_llm_for_conversion()")
        result = call_llm_for_conversion(chunk_code, serializable_context)
        
        # Ensure the result has the chunk name
        if result and isinstance(result, dict):
            result['name'] = chunk_name
        
        logger.info(f"Control Flow: LLM Conversion complete - Confidence score: {result.get('confidence_score', 0)}")
        logger.info(f"Control Flow: Returning from _process_chunk_with_context()")
        
        return result
    except Exception as e:
        logger.error(f"Error processing chunk: {str(e)}", exc_info=True)
        # Return an error result
        return {
            'name': chunk.get('name', 'unnamed_chunk'),
            'pyspark_code': f"# Error processing chunk: {str(e)}\n",
            'annotations': [{'note': f"Error: {str(e)}", 'severity': 'Error'}],
            'confidence_score': 0.0,
            'refinement_notes': f"Failed to process chunk: {str(e)}"
        }

def _direct_conversion(sas_code):
    """Direct conversion for very small code (10-20 lines)"""
    # Detect the primary section type to generate a more targeted prompt
    section_type = _detect_section_type(sas_code)
    
    # Enhanced prompt for high-quality conversion
    prompt = f"""You are an expert SAS and PySpark developer tasked with converting SAS code to high-quality, production-ready PySpark code.

Below is the SAS code to convert:
```sas
{sas_code}
```

This code appears to be primarily a {section_type} section. Please convert it to efficient, idiomatic PySpark code following these guidelines:

1. Create clean, well-structured PySpark code that follows best practices
2. Include all necessary imports at the top (pyspark.sql, functions as F, etc.)
3. Use DataFrame operations instead of procedural code wherever possible
4. Provide proper error handling and type checking
5. Maintain the exact same business logic and data transformations
6. Add clear comments for complex transformations
7. Use PySpark's built-in functions rather than UDFs when possible for better performance
8. Implement appropriate DataFrame APIs for SAS procedures (groupBy, window functions, etc.)
9. Handle date/time conversions properly using PySpark datetime functions
10. Ensure code follows a consistent style and is well-documented

Respond with a valid JSON object with these fields:
- pyspark_code: Complete, production-ready PySpark code with all imports
- annotations: List of notes about the conversion and any important implementation details
- confidence_score: A score from 1-5 rating how confident you are in the conversion (5 = highest confidence)
- refinement_notes: Any suggestions for further optimization or improvement

Example response format:
```json
{{
  "pyspark_code": "from pyspark.sql import SparkSession, functions as F\\nfrom pyspark.sql.types import *\\n\\n# Init SparkSession\\nspark = SparkSession.builder.appName(\\"SAS_Conversion\\").getOrCreate()\\n\\n# Rest of code...",
  "annotations": ["Converted SAS DATA step to DataFrame operations", "Replaced SAS functions with PySpark equivalents"],
  "confidence_score": 4.8,
  "refinement_notes": "Code follows best practices and maintains original logic"
}}
```

Focus on generating high-quality, production-ready code that would be easy to maintain and extend. Be thoughtful in your conversion approach, ensuring that the PySpark code is both efficient and faithful to the original SAS logic.
"""
    try:
        response = _call_llm_with_retry(
            lambda p: _invoke_claude37_sonnet(p),
            prompt,
            description="Direct SAS to PySpark conversion"
        )
        
        # Parse response
        try:
            if isinstance(response, str):
                result = _parse_conversion_response(response)
            else:
                result = response
                
            # Log successful direct conversion
            logger.info("Direct conversion successful")
            logger.debug(f"Direct conversion result (first 200 chars): {result.get('pyspark_code', '')[:200]}...")
            
            return {
                'pyspark_code': result.get('pyspark_code', ''),
                'annotations': result.get('annotations', []),
                'confidence_score': float(result.get('confidence_score', result.get('confidence', 3.0))),
                'refinement_notes': result.get('refinement_notes', '')
            }
        except json.JSONDecodeError:
            # If JSON parsing fails, try to extract code directly
            code_pattern = r'```python\s*([\s\S]*?)\s*```'
            code_match = re.search(code_pattern, response if isinstance(response, str) else '')
            if code_match:
                code = code_match.group(1).strip()
                return {
                    'pyspark_code': code,
                    'annotations': [{'note': 'Extracted from non-JSON response', 'severity': 'Warning'}],
                    'confidence_score': 2.0,
                    'refinement_notes': 'Response format was invalid JSON'
                }
            else:
                logger.error(f"Failed to parse LLM response and could not extract code")
                raise ValueError("Invalid LLM response format")
    except Exception as e:
        logger.error(f"Error in direct conversion: {e}")
        return {
            'pyspark_code': f"# Error in conversion: {str(e)}\n",
            'annotations': [{'note': f"Error: {str(e)}", 'severity': 'Error'}],
            'confidence_score': 0.0,
            'refinement_notes': f"Conversion failed: {str(e)}"
        }

def _large_code_conversion(sas_code, converter):
    """Optimized conversion path for large code (10,000-50,000 lines)"""
    check_timeout()  # Check timeout before processing large code
    
    # Use the simplified approach instead of complex chunking
    logger.info("Using simplified approach for large code conversion")
    return _large_code_conversion_simplified(sas_code, converter)
    
    # The code below is kept commented for reference but won't be executed
    """
    # Use available chunking methods for large code
    if USE_ADAPTIVE_CHUNKING:
        # First try to identify major sections
        chunks = converter.chunker.identify_major_sections(sas_code)
        
        # If no major sections found, fall back to regular chunking
        if not chunks:
            chunks = converter.chunker.identify_chunks(sas_code)
    else:
        chunks = converter.chunker.identify_chunks(sas_code)
    
    # Filter out chunks that are just comments or empty
    valid_chunks = []
    for chunk in chunks:
        chunk_code = chunk.get('code', '')
        # Check if chunk contains actual code, not just comments or whitespace
        if _is_valid_chunk(chunk_code):
            valid_chunks.append(chunk)
        else:
            logger.info(f"Skipping chunk {chunk.get('name', 'unknown')} as it appears to be just comments or empty")
    
    # If we filtered out too many chunks, fall back to the original list
    if len(valid_chunks) < len(chunks) * 0.5:  # If we lost more than half the chunks
        logger.warning(f"Too many chunks were filtered out as comments. Using original chunks.")
        valid_chunks = chunks
    else:
        chunks = valid_chunks
        logger.info(f"Using {len(chunks)} valid chunks after filtering")
    
    # Optimize chunk size based on token usage if enabled
    if USE_TOKEN_OPTIMIZATION:
        chunks = _optimize_chunk_sizes(chunks)
    
    processing_order = converter.determine_processing_order(chunks)
    logger.info(f"Processing {len(chunks)} chunks in optimized order")
    
    # Process chunks with parallel execution if enabled
    if USE_PARALLEL_PROCESSING and len(chunks) > 3:
        return _parallel_chunk_processing(chunks, sas_code, converter)
    else:
        return _sequential_chunk_processing(chunks, processing_order, sas_code, converter)
    """

def _is_valid_chunk(code):
    """Check if a chunk contains actual code and not just comments or whitespace"""
    if not code or not code.strip():
        return False
    
    # Remove comments
    code_without_comments = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)  # Remove block comments
    code_without_comments = re.sub(r'^\s*\*.*$', '', code_without_comments, flags=re.MULTILINE)  # Remove * comments
    code_without_comments = re.sub(r'^\s*//.*$', '', code_without_comments, flags=re.MULTILINE)  # Remove line comments
    
    # Check if anything remains after removing comments and whitespace
    return bool(code_without_comments.strip())

def _optimize_chunk_sizes(chunks):
    """Optimize chunk sizes to maximize token usage efficiency"""
    optimized_chunks = []
    
    for chunk in chunks:
        # Estimate token count for this chunk
        estimated_tokens = _estimate_token_count(chunk['code'])
        
        # If chunk is too large, split it further
        if estimated_tokens > MAX_TOKENS * 0.7:  # Use 70% of max tokens as threshold
            logger.info(f"Chunk too large ({estimated_tokens} estimated tokens), splitting further")
            sub_chunks = _split_chunk_further(chunk)
            optimized_chunks.extend(sub_chunks)
        else:
            optimized_chunks.append(chunk)
    
    return optimized_chunks

def _estimate_token_count(code):
    """Estimate token count for a code snippet"""
    # Simple estimation: ~4 characters per token
    return len(code) // 4

def _split_chunk_further(chunk):
    """Split a large chunk into smaller sub-chunks"""
    # For DATA steps, split by statements
    if chunk['type'] == 'DATA':
        return _split_data_step(chunk)
    # For PROC steps, split by statements
    elif chunk['type'] == 'PROC':
        return _split_proc_step(chunk)
    # For macros, split by statements
    elif chunk['type'] == 'MACRO':
        return _split_macro(chunk)
    # For other types, split by lines
    else:
        return _split_by_lines(chunk)

def _split_data_step(chunk):
    """Split a DATA step into smaller sub-chunks"""
    code = chunk['code']
    sub_chunks = []
    
    # Find all statements in the DATA step
    statements = re.findall(r'(\w+\s+[^;]*;)', code)
    
    # Group statements into sub-chunks
    current_sub_chunk = []
    current_size = 0
    
    for statement in statements:
        statement_size = len(statement)
        
        # If adding this statement would exceed threshold, create a new sub-chunk
        if current_size + statement_size > MAX_TOKENS * 0.7 // 4:  # Convert tokens to chars
            if current_sub_chunk:
                sub_chunks.append({
                    'type': 'DATA',
                    'name': f"{chunk['name']}_part{len(sub_chunks)+1}",
                    'code': '\n'.join(current_sub_chunk),
                    'dependencies': chunk['dependencies']
                })
                current_sub_chunk = []
                current_size = 0
        
        current_sub_chunk.append(statement)
        current_size += statement_size
    
    # Add the last sub-chunk if it exists
    if current_sub_chunk:
        sub_chunks.append({
            'type': 'DATA',
            'name': f"{chunk['name']}_part{len(sub_chunks)+1}",
            'code': '\n'.join(current_sub_chunk),
            'dependencies': chunk['dependencies']
        })
    
    return sub_chunks

def _split_proc_step(chunk):
    """Split a PROC step into smaller sub-chunks"""
    # Similar to _split_data_step but for PROC steps
    # Implementation details omitted for brevity
    return [chunk]  # Placeholder

def _split_macro(chunk):
    """Split a macro into smaller sub-chunks"""
    # Similar to _split_data_step but for macros
    # Implementation details omitted for brevity
    return [chunk]  # Placeholder

def _split_by_lines(chunk):
    """Split a chunk by lines"""
    code = chunk['code']
    lines = code.split('\n')
    
    sub_chunks = []
    current_sub_chunk = []
    current_size = 0
    
    for line in lines:
        line_size = len(line)
        
        # If adding this line would exceed threshold, create a new sub-chunk
        if current_size + line_size > MAX_TOKENS * 0.7 // 4:  # Convert tokens to chars
            if current_sub_chunk:
                sub_chunks.append({
                    'type': chunk['type'],
                    'name': f"{chunk['name']}_part{len(sub_chunks)+1}",
                    'code': '\n'.join(current_sub_chunk),
                    'dependencies': chunk['dependencies']
                })
                current_sub_chunk = []
                current_size = 0
        
        current_sub_chunk.append(line)
        current_size += line_size
    
    # Add the last sub-chunk if it exists
    if current_sub_chunk:
        sub_chunks.append({
            'type': chunk['type'],
            'name': f"{chunk['name']}_part{len(sub_chunks)+1}",
            'code': '\n'.join(current_sub_chunk),
            'dependencies': chunk['dependencies']
        })
    
    return sub_chunks

def _extreme_code_conversion(sas_code, converter):
    """Specialized conversion path for extreme code (50,000+ lines)"""
    check_timeout()  # Check timeout before processing extreme code
    
    # Use incremental conversion for extreme code
    if USE_INCREMENTAL_CONVERSION:
        return _incremental_conversion(sas_code, converter)
    else:
        # Fall back to large code conversion with optimized settings
        return _large_code_conversion(sas_code, converter)

def _parallel_chunk_processing(chunks, sas_code, converter):
    """Process chunks in parallel for improved performance"""
    check_timeout()  # Check timeout before parallel processing
    
    results = []
    
    # Calculate optimal number of workers based on remaining time and chunk count
    max_workers = min(5, len(chunks))
    if EXECUTION_DEADLINE:
        remaining_seconds = (EXECUTION_DEADLINE - datetime.now()).total_seconds()
        # Rough estimate: each chunk might take 30s, allow for 2 cycles per worker
        time_based_workers = max(1, int(remaining_seconds / 60))
        max_workers = min(max_workers, time_based_workers)
    
    logger.info(f"Parallel processing with {max_workers} workers for {len(chunks)} chunks")
    
    # Create simplified chunks with only necessary data to avoid serialization issues
    serializable_chunks = []
    for chunk in chunks:
        # Create a simplified copy with only basic data
        simplified_chunk = {
            'name': chunk.get('name', 'unknown'),
            'type': chunk.get('type', 'UNKNOWN'),
            'code': chunk.get('content', chunk.get('code', '')),
            'dependencies': list(chunk.get('dependencies', [])) if chunk.get('dependencies') else []
        }
        serializable_chunks.append(simplified_chunk)
    
    # Process chunks in parallel with a thread pool - don't use multiprocessing
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_chunk = {}
        chunk_count = 0
        
        # Submit chunks in batches to avoid overloading
        for chunk in serializable_chunks:
            check_timeout()  # Check before submitting each batch
            
            # Instead of getting focused context here, we'll pass minimal context
            # and let _process_simple_chunk handle it to avoid serialization issues
            
            # Submit a simplified function that doesn't require the converter
            future = executor.submit(
                call_llm_for_conversion,
                chunk.get('code', ''),
                {'chunk_name': chunk.get('name', 'unknown')}  # Minimal context
            )
            future_to_chunk[future] = chunk
            chunk_count += 1
            
            # Process in small batches with brief pauses to allow for timeout checks
            if chunk_count % 3 == 0:
                time.sleep(0.1)  # Brief pause to allow other threads to run
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_chunk):
            check_timeout()  # Check timeout as results complete
            chunk = future_to_chunk[future]
            try:
                result = future.result()
                
                # Add the chunk name to the result
                result['name'] = chunk.get('name', 'unknown')
                results.append(result)
                
                # Don't update context manager here to avoid thread lock issues
                # Context will be built from final results later
                
            except Exception as e:
                logger.error(f"Error processing chunk {chunk.get('name', 'unknown')}: {e}")
                # Add error result
                results.append({
                    'name': chunk.get('name', 'unknown'),
                    'pyspark_code': f"# Error processing chunk: {str(e)}\n",
                    'annotations': [{'note': f"Error: {str(e)}", 'severity': 'Error'}],
                    'confidence_score': 0.0,
                    'refinement_notes': f"Failed to process chunk: {str(e)}"
                })
    
    # After all parallel processing is complete, update the context with all results
    # This avoids thread lock issues since we're now in a single thread
    try:
        for result in results:
            if 'pyspark_code' in result:
                chunk_name = result.get('name', 'unknown')
                chunk = next((c for c in chunks if c.get('name') == chunk_name), None)
                if chunk:
                    try:
                        converter.context_manager.update_context(
                            chunk, 
                            result.get('pyspark_code', ''), 
                            result.get('annotations', [])
                        )
                    except Exception as context_err:
                        logger.error(f"Error updating context for {chunk_name}: {str(context_err)}")
    except Exception as e:
        logger.error(f"Error during post-processing context updates: {str(e)}")
    
    # Combine results
    return _combine_results(results)

def _process_simple_chunk(chunk, context):
    """Simplified chunk processor that only depends on serializable objects"""
    try:
        chunk_code = chunk.get('code', '')
        chunk_name = chunk.get('name', 'unknown')
        
        # Ensure context is serializable
        serializable_context = {}
        if context and isinstance(context, dict):
            for key, value in context.items():
                if isinstance(value, dict):
                    serializable_context[key] = copy.deepcopy(value)
                elif isinstance(value, (list, set)):
                    serializable_context[key] = list(value)
                elif isinstance(value, (str, int, float, bool, type(None))):
                    serializable_context[key] = value
        
        # Add chunk name to context
        serializable_context['chunk_name'] = chunk_name
        
        # Convert chunk with context
        logger.info(f"Processing simple chunk: {chunk_name} ({len(chunk_code)} chars)")
        result = call_llm_for_conversion(
            chunk_code,
            serializable_context
        )
        
        # Build a clean result object
        return {
            'name': chunk_name,
            'pyspark_code': result.get('pyspark_code', ''),
            'annotations': result.get('annotations', []),
            'confidence_score': result.get('confidence_score', 0.0),
            'refinement_notes': result.get('refinement_notes', '')
        }
    except Exception as e:
        logger.error(f"Error processing chunk: {str(e)}")
        return {
            'name': chunk.get('name', 'unknown'),
            'pyspark_code': f"# Error processing chunk: {str(e)}\n",
            'annotations': [{'note': f"Error: {str(e)}", 'severity': 'Error'}],
            'confidence_score': 0.0,
            'refinement_notes': f"Failed to process chunk: {str(e)}"
        }

def _sequential_chunk_processing(chunks, processing_order, sas_code, converter):
    """Process chunks sequentially with optimized context management"""
    results = []
    context = {}
    
    for chunk_name in processing_order:
        chunk = next((c for c in chunks if c['name'] == chunk_name), None)
        if not chunk:
            logger.warning(f"Chunk {chunk_name} not found, skipping")
            continue
            
        logger.info(f"Processing chunk: {chunk_name}")
        
        try:
            # Get focused context for this chunk - only pass the chunk parameter
            focused_context = converter.context_manager.get_focused_context(chunk)
            
            # Convert to serializable context
            serializable_context = {}
            if focused_context:
                # Create a simplified copy without any complex objects
                for key, value in focused_context.items():
                    if isinstance(value, dict):
                        serializable_context[key] = copy.deepcopy(value)
                    elif isinstance(value, (list, set)):
                        serializable_context[key] = list(value)
                    elif isinstance(value, (str, int, float, bool, type(None))):
                        serializable_context[key] = value
            
            # Process chunk with context
            chunk_code = chunk.get('content', chunk.get('code', ''))
            result = call_llm_for_conversion(
                chunk_code,
                serializable_context
            )
            
            # Update context with results
            try:
                context = converter.context_manager.update_context(
                    chunk, result.get('pyspark_code', ''), result.get('annotations', [])
                )
            except Exception as context_err:
                logger.error(f"Error updating context: {context_err}")
                # Continue with processing even if context update fails
            
            results.append({
                'name': chunk_name,
                'pyspark_code': result.get('pyspark_code', ''),
                'annotations': result.get('annotations', []),
                'confidence_score': result.get('confidence_score', 0.0),
                'refinement_notes': result.get('refinement_notes', '')
            })
            
        except Exception as e:
            logger.error(f"Error processing chunk {chunk_name}: {e}")
            results.append({
                'name': chunk_name,
                'pyspark_code': f"# Error processing chunk {chunk_name}: {str(e)}\n",
                'annotations': [{'note': f"Error: {str(e)}", 'severity': 'Error'}],
                'confidence_score': 0.0,
                'refinement_notes': f"Failed to process chunk: {str(e)}"
            })
    
    # Combine results
    return _combine_results(results)

def _combine_results(results):
    """
    Intelligently combine results from multiple chunks into one.
    Handles imports, duplications, and annotations properly.
    """
    if not results:
        return {'pyspark_code': '', 'annotations': [], 'confidence_score': 0.0}
    
    if len(results) == 1:
        return results[0]
    
    logger.info(f"Combining {len(results)} chunk results")
    
    # Extract information from all results
    all_code = []
    all_annotations = []
    all_confidences = []
    
    # First, remove duplicate import statements
    for result in results:
        pyspark_code = result.get('pyspark_code', '')
        annotations = result.get('annotations', [])
        confidence = result.get('confidence_score', 0.0)
        
        # Process the code to avoid duplicates
        all_code.append(pyspark_code)
        all_annotations.extend(annotations)
        all_confidences.append(confidence)
    
    # Combine code sections with better handling of import statements
    combined_code = "\n".join(all_code)
    
    # Calculate the average confidence score
    average_confidence = sum(all_confidences) / len(all_confidences) if all_confidences else 0.0
    
    # Deduplicate combined code
    deduplicated_code = deduplicate_pyspark_code(combined_code)
    
    # Create the final result
    combined_result = {
        'pyspark_code': deduplicated_code,
        'annotations': all_annotations,
        'confidence_score': average_confidence
    }
    
    logger.info(f"Combined result has {len(deduplicated_code)} characters and confidence {average_confidence}")
    
    return combined_result

def _incremental_conversion(sas_code, converter):
    """Incremental conversion for extreme code (50,000+ lines)"""
    check_timeout()  # Check timeout before sectioning extreme code
    
    # Split code into major sections
    sections = converter.chunker.identify_major_sections(sas_code)
    logger.info(f"Identified {len(sections)} major sections for incremental conversion")
    
    if not sections:
        logger.warning("No sections identified for incremental conversion, falling back to large code conversion")
        return _large_code_conversion(sas_code, converter)
    
    all_results = []
    processed_sections = 0
    total_sections = len(sections)
    
    # Process each section incrementally
    for i, section in enumerate(sections):
        check_timeout()  # Check timeout before each section
        
        # Calculate if we have time for this section
        if EXECUTION_DEADLINE and total_sections > 0:
            remaining_seconds = (EXECUTION_DEADLINE - datetime.now()).total_seconds()
            estimated_time_needed = remaining_seconds * (total_sections - processed_sections) / total_sections
            
            if estimated_time_needed > remaining_seconds:
                logger.warning(f"Not enough time to complete all sections. Processed {processed_sections}/{total_sections}")
                break
        
        logger.info(f"Processing section {i+1}/{len(sections)}: {section.get('name', 'unknown')}")
        
        try:
            # Convert section
            section_result = _large_code_conversion(section['code'], converter)
            
            # Add section metadata
            section_result['section_name'] = section.get('name', f"Section_{i+1}")
            section_result['section_index'] = i
            
            all_results.append(section_result)
            processed_sections += 1
        except TimeoutError:
            # If we're out of time, stop processing sections
            logger.warning(f"Timeout while processing section {i+1}, stopping incremental conversion")
            all_results.append({
                'pyspark_code': f"# Timeout while processing section {i+1}\n# Remaining sections omitted due to time constraints\n",
                'annotations': [{'note': "Processing timeout", 'severity': 'Warning'}],
                'confidence_score': 1.0,
                'refinement_notes': "Section processing interrupted due to timeout",
                'section_name': section.get('name', f"Section_{i+1}"),
                'section_index': i
            })
            break
        except Exception as e:
            logger.error(f"Error processing section {i+1}: {e}")
            # Add error result for this section
            all_results.append({
                'pyspark_code': f"# Error processing section {i+1}: {str(e)}\n",
                'annotations': [{'note': f"Error: {str(e)}", 'severity': 'Error'}],
                'confidence_score': 1.0,
                'refinement_notes': f"Failed to process section {i+1}: {str(e)}",
                'section_name': section.get('name', f"Section_{i+1}"),
                'section_index': i
            })
    
    check_timeout()  # Final check before combining results
    
    # Add note if we didn't process all sections
    if processed_sections < total_sections:
        all_results.append({
            'pyspark_code': f"\n\n# Note: {total_sections - processed_sections} sections were not processed due to time constraints\n",
            'annotations': [{'note': f"{total_sections - processed_sections} sections not processed", 'severity': 'Warning'}],
            'confidence_score': 1.0,
            'refinement_notes': "Partial conversion due to timeout",
            'section_name': "Timeout_Notice",
            'section_index': -1
        })
    
    # Combine all section results
    combined_code = "\n\n# ===== SECTION BREAKS =====\n\n".join(
        f"# Section: {r['section_name']}\n{r['pyspark_code']}"
        for r in all_results
    )
    
    # Combine annotations with section information
    combined_annotations = []
    for r in all_results:
        for ann in r.get('annotations', []):
            # Add section information to annotation
            ann['section'] = r.get('section_name', 'unknown')
            combined_annotations.append(ann)
    
    # Calculate overall confidence
    avg_confidence = sum(r.get('confidence_score', 0) for r in all_results) / max(1, len(all_results))
    
    # Combine refinement notes with section information
    combined_notes = "\n\n".join(
        f"Section {r.get('section_name', 'Section_' + str(r.get('section_index', 0)))}:\n{r.get('refinement_notes', '')}"
        for r in all_results
        if r.get('refinement_notes')
    )
    
    # Add completion percentage
    completion_percentage = (processed_sections / total_sections) * 100 if total_sections > 0 else 100
    completion_note = f"Processed {processed_sections}/{total_sections} sections ({completion_percentage:.1f}%)"
    logger.info(completion_note)
    
    return {
        'pyspark_code': combined_code,
        'annotations': combined_annotations,
        'confidence_score': avg_confidence,
        'refinement_notes': combined_notes,
        'completion_info': {
            'total_sections': total_sections,
            'processed_sections': processed_sections,
            'completion_percentage': completion_percentage
        }
    }

# --- Lambda Handler ---
def lambda_handler(event, context):
    """
    AWS Lambda handler for SAS to PySpark conversion.
    Handles both file attachments and direct text input from React UI.
    """
    # Set execution deadline for timeout protection
    set_execution_deadline(context)
    
    # Initialize response headers for CORS (React UI)
    headers = {
        'Access-Control-Allow-Origin': '*',  # Configure this for your React domain
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'POST,OPTIONS'
    }
    
    # Handle OPTIONS request for CORS
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': headers,
            'body': ''
        }
    
    try:
        # Get request body and content type
        body = event.get('body', '')
        if not body:
            raise ValueError("Missing request body")

        content_type = event.get('headers', {}).get('content-type', '').lower()
        
        # Extract SAS code based on content type
        sas_code = None
        
        if 'application/json' in content_type:
            # Handle JSON request (for text input from React)
            try:
                body_json = json.loads(body)
                # Check both possible fields from React
                sas_code = body_json.get('sas_code') or body_json.get('text')
            except json.JSONDecodeError as e:
                logger.error(f"JSON parsing error: {e}")
                raise ValueError(f"Invalid JSON format: {str(e)}")
                
        elif 'multipart/form-data' in content_type:
            # Handle file upload
            try:
                # Handle base64 encoded body from API Gateway
                if event.get('isBase64Encoded', False):
                    decoded_body = base64.b64decode(body).decode('utf-8')
                else:
                    decoded_body = body

                # Extract boundary from content type
                boundary = content_type.split('boundary=')[-1].strip()
                
                # Split body by boundary
                parts = decoded_body.split('--' + boundary)
                
                # Look for file content or text field
                for part in parts:
                    # Check for file upload
                    if 'filename=' in part and '.sas' in part.lower():
                        content_start = part.find('\r\n\r\n') + 4
                        content_end = part.rfind('\r\n')
                        sas_code = part[content_start:content_end].strip()
                        break
                    # Check for text field
                    elif 'name="sas_code"' in part or 'name="text"' in part:
                        content_start = part.find('\r\n\r\n') + 4
                        content_end = part.rfind('\r\n')
                        sas_code = part[content_start:content_end].strip()
                        break
                        
            except Exception as e:
                logger.error(f"Form-data parsing error: {e}")
                raise ValueError(f"Invalid form-data format: {str(e)}")
                
        else:
            # Handle raw text input
            sas_code = body.strip()
        
        # Validate SAS code
        if not sas_code:
            raise ValueError("No SAS code found in request. Please provide code either as a file upload or text input.")
        
        if not isinstance(sas_code, str):
            raise ValueError("SAS code must be a string")
        
        # Remove any BOM characters and normalize line endings
        sas_code = sas_code.encode('utf-8').decode('utf-8-sig')
        sas_code = sas_code.replace('\r\n', '\n').replace('\r', '\n')
        
        # Log code size
        code_size = len(sas_code)
        logger.info(f"Received SAS code with {code_size} characters")
        
        # Check if code is too large for 5-minute Lambda timeout
        if code_size > EXTREME_CODE_THRESHOLD * 3:
            message = f"Code size ({code_size} chars) is too large for Lambda processing (would exceed timeout)"
            logger.warning(message)
            return {
                'statusCode': 413,  # Payload Too Large
                'headers': headers,
                'body': json.dumps({
                    'error': 'Code too large',
                    'message': message,
                    'details': 'Consider splitting the code into smaller chunks and processing separately'
                })
            }
            
        # Check if code is close to limits
        if code_size > EXTREME_CODE_THRESHOLD * 2:
            logger.warning(f"Code size ({code_size} chars) may take significant time to process")
        
        # Convert SAS to PySpark
        result = convert_sas_to_pyspark(sas_code)
        
        # Generate filename and save to S3
        pyspark_code = result.get('pyspark_code', '')
        if pyspark_code and pyspark_code.strip():
            try:
                filename = generate_pyspark_filename(sas_code, result)
                s3_uri = save_to_s3(pyspark_code, filename)
                # Add S3 info to the result
                result['s3_uri'] = s3_uri
                result['filename'] = filename
                logger.info(f"Saved PySpark code to S3: {s3_uri}")
            except Exception as s3_error:
                # Log error but don't fail the request
                logger.error(f"Failed to save to S3: {s3_error}")
                result['s3_error'] = str(s3_error)
        else:
            logger.warning("No PySpark code was generated, skipping S3 save")
            result['s3_error'] = "No PySpark code was generated to save"
        
        # Log final response that will be returned to the React app
        logger.info(f"Conversion complete with confidence score: {result.get('confidence_score', 0.0)}")
        logger.info(f"Annotations count: {len(result.get('annotations', []))}")
        logger.info(f"PySpark code length: {len(pyspark_code)}")
        
        # Return successful response
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(result)
        }
        
    except TimeoutError as e:
        # Handle timeout specifically
        error_msg = str(e)
        logger.error(f"Timeout error: {error_msg}")
        return {
            'statusCode': 408,  # Request Timeout
            'headers': headers,
            'body': json.dumps({
                'error': 'Request timeout',
                'message': "The conversion took too long and exceeded the Lambda time limit",
                'details': 'Please try with a smaller code sample or break your code into smaller chunks'
            })
        }
        
    except ValueError as e:
        # Handle validation errors
        error_msg = str(e)
        logger.error(f"Validation error: {error_msg}")
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps({
                'error': 'Validation error',
                'message': error_msg,
                'details': 'Please ensure you have provided valid SAS code either as a file or text input.'
            })
        }
        
    except Exception as e:
        # Handle unexpected errors
        error_msg = str(e)
        logger.error(f"Unexpected error: {error_msg}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({
                'error': 'Internal server error',
                'message': error_msg,
                'details': 'An unexpected error occurred while processing your request.'
            })
        }

# --- Optimized Prompting Strategy ---
def _detect_section_type(sas_code):
    """
    Detect the predominant section type in SAS code.
    Returns: 'DATA', 'PROC', 'MACRO', or 'GENERIC'
    """
    sas_code_lower = sas_code.lower()
    
    # Count occurrences to determine predominant type
    data_count = len(re.findall(r'\bdata\s+\w+', sas_code_lower))
    proc_count = len(re.findall(r'\bproc\s+\w+', sas_code_lower))
    macro_count = len(re.findall(r'\b%macro\s+\w+', sas_code_lower))
    
    # Determine predominant type
    if macro_count > data_count and macro_count > proc_count:
        return 'MACRO'
    elif data_count > proc_count:
        return 'DATA'
    elif proc_count > 0:
        return 'PROC'
    else:
        return 'GENERIC'

def _get_type_specific_prompt(section_type, sas_code):
    """
    Generate a type-specific prompt based on the section type.
    Returns a prompt with guidance specific to the section type.
    """
    base_prompt = (
        "Convert the following SAS code to PySpark code. "
        "Return a JSON object with these fields:\n"
        "- pyspark_code: The converted code that is ready to run\n"
        "- annotations: A list of any important notes about the conversion\n"
        "- confidence_score: A number from 1-5 representing confidence in the conversion accuracy\n"
        "- refinement_notes: Any additional notes about optimizations or improvements made\n\n"
    )
    
    if section_type == 'DATA':
        specific_guidance = (
            "This code contains SAS DATA steps. For optimal conversion:\n"
            "- Convert DATA steps to PySpark DataFrame operations\n"
            "- Use DataFrame.withColumn() for variable assignments\n"
            "- Replace SAS functions with PySpark equivalents (e.g., INPUT → cast, PUT → format)\n"
            "- Convert BY statements to groupBy() operations\n"
            "- Handle SAS-specific date/time functions appropriately\n"
        )
    elif section_type == 'PROC':
        specific_guidance = (
            "This code contains SAS PROC steps. For optimal conversion:\n"
            "- Convert PROC SORT to DataFrame.sort()\n"
            "- Convert PROC SQL to Spark SQL or DataFrame operations\n"
            "- Convert PROC MEANS/SUMMARY to groupBy().agg() operations\n"
            "- Convert PROC FREQ to groupBy().count() with appropriate transformations\n"
            "- Implement PROC TRANSPOSE using pivot() or groupBy().pivot()\n"
        )
    elif section_type == 'MACRO':
        specific_guidance = (
            "This code contains SAS macros. For optimal conversion:\n"
            "- Convert SAS macros to Python functions\n"
            "- Replace macro variables with Python parameters\n"
            "- Convert %IF-%THEN-%ELSE logic to Python if-else statements\n"
            "- Replace %DO loops with Python for loops\n"
            "- Convert macro invocations to function calls\n"
        )
    else:  # GENERIC
        specific_guidance = (
            "For optimal conversion:\n"
            "- Map SAS data structures to PySpark DataFrames\n"
            "- Convert SAS procedures to equivalent PySpark operations\n"
            "- Replace SAS functions with PySpark equivalents\n"
            "- Pay special attention to data types and date/time conversions\n"
        )
    
    # Add optimization guidance for all types
    optimization_guidance = (
        "Optimize the resulting PySpark code by:\n"
        "- Using efficient DataFrame operations\n"
        "- Minimizing shuffles when possible\n"
        "- Selecting appropriate join strategies\n"
        "- Incorporating best practices for performance\n"
    )
    
    json_format_guidance = (
        "IMPORTANT: Your response must be a valid JSON object with the specified fields.\n"
        "For example:\n"
        "```json\n"
        "{\n"
        '  "pyspark_code": "from pyspark.sql import functions as F\\n\\n# Code here",\n'
        '  "annotations": ["Note 1", "Note 2"],\n'
        '  "confidence_score": 4.5,\n'
        '  "refinement_notes": "Optimized by using broadcast join..."\n'
        "}\n"
        "```\n"
    )
    
    return f"{base_prompt}{specific_guidance}\n\n{optimization_guidance}\n\n{json_format_guidance}"

def _format_context_information(context):
    """Format the context information for use in the prompt."""
    if not context:
        return ""
        
    context_sections = []
    
    if context.get('variables'):
        vars_text = "\n".join([f"- {var}" for var in context['variables']])
        context_sections.append(f"SAS VARIABLES:\n{vars_text}")
    
    if context.get('datasets'):
        ds_text = "\n".join([f"- {ds}" for ds in context['datasets']])
        context_sections.append(f"SAS DATASETS:\n{ds_text}")
    
    if context.get('macros'):
        macros_text = "\n".join([f"- {macro}" for macro in context['macros']])
        context_sections.append(f"SAS MACROS:\n{macros_text}")
    
    if context.get('code_context'):
        context_sections.append(f"RELEVANT CODE CONTEXT:\n{context['code_context']}")
    
    if not context_sections:
        return ""
        
    return "CONTEXT INFORMATION:\n" + "\n\n".join(context_sections) + "\n\n"

def _optimize_context_for_tokens(context, max_tokens=3000):
    """
    Optimize the context to fit within token limits while preserving critical information.
    Returns a streamlined context object.
    """
    if not context:
        return {}
    
    # Create a copy to avoid modifying the original
    optimized_context = copy.deepcopy(context)
    
    # Prioritize elements to keep (in order of importance)
    # 1. Keep all variables
    # 2. Keep all datasets
    # 3. Truncate macros if needed
    # 4. Truncate code_context if needed
    
    # Estimate token counts for each section
    estimated_tokens = 0
    
    # Estimate tokens for variables
    if 'variables' in optimized_context:
        var_tokens = sum(len(var.split()) for var in optimized_context['variables'])
        estimated_tokens += var_tokens
    
    # Estimate tokens for datasets
    if 'datasets' in optimized_context:
        ds_tokens = sum(len(ds.split()) for ds in optimized_context['datasets'])
        estimated_tokens += ds_tokens
    
    # If we're already over the limit, start truncating
    if estimated_tokens > max_tokens and 'macros' in optimized_context:
        # Remove macros entirely if variables and datasets already exceed the limit
        del optimized_context['macros']
    else:
        # Estimate tokens for macros
        if 'macros' in optimized_context:
            macro_tokens = sum(len(macro.split()) for macro in optimized_context['macros'])
            estimated_tokens += macro_tokens
            
            # If macros push us over the limit, truncate them
            if estimated_tokens > max_tokens:
                # Keep the most important macros (shorter ones are often more fundamental)
                macros_sorted = sorted(optimized_context['macros'], key=len)
                optimized_context['macros'] = []
                
                for macro in macros_sorted:
                    macro_token_count = len(macro.split())
                    if estimated_tokens - macro_tokens + macro_token_count <= max_tokens:
                        optimized_context['macros'].append(macro)
                        estimated_tokens = estimated_tokens - macro_tokens + macro_token_count
                        macro_tokens = macro_token_count
                    else:
                        break
    
    # Finally, deal with code_context if needed
    if 'code_context' in optimized_context:
        context_tokens = len(optimized_context['code_context'].split())
        estimated_tokens += context_tokens
        
        if estimated_tokens > max_tokens:
            # If we have too many tokens, truncate the code_context
            # Keep the most recent/relevant parts (last N tokens)
            words = optimized_context['code_context'].split()
            available_tokens = max_tokens - (estimated_tokens - context_tokens)
            
            if available_tokens > 100:  # Only keep code_context if we can keep a meaningful amount
                optimized_context['code_context'] = ' '.join(words[-available_tokens:])
                estimated_tokens = max_tokens
            else:
                # If we can only keep a tiny bit of context, it's better to drop it
                del optimized_context['code_context']
                estimated_tokens -= context_tokens
    
    return optimized_context

def call_llm_for_conversion(sas_code, context=None, retries=MAX_RETRIES):
    """
    Converts SAS code to PySpark and provides refinement in a single call.
    For code up to 2000 lines, processes in a single call to avoid chunking issues.
    """
    try:
        # For relatively small code (up to ~2000 lines), avoid chunking
        if sas_code.count('\n') <= 2000:
            logger.info("Processing code as a single unit since it's under 2000 lines")
            # Get type-specific prompt for the entire code
            prompt = f"""Convert this SAS code to equivalent PySpark code. Focus on correctness and ensure the code can be run end-to-end without any missing references or duplicated code.

Return a JSON object with:
- pyspark_code: The converted PySpark code
- annotations: List of important notes about the conversion
- confidence_score: A score from 1-5 rating your confidence in the conversion
- refinement_notes: Any issues encountered or suggestions for improvement

Important guidelines:
1. DO NOT repeat or duplicate code sections
2. Ensure all variables referenced are properly defined
3. Include all necessary import statements at the top
4. Make sure the logic flow is maintained end-to-end
5. Remove any commented out code blocks
6. Use meaningful variable names
7. Include docstrings for functions

SAS CODE:
```sas
{sas_code}
```
"""
            
            # Call LLM with retry
            response = _call_llm_with_retry(
                lambda p: _invoke_claude37_sonnet(p),
                prompt,
                description="SAS to PySpark conversion (full code)"
            )
            
            # Parse the conversion response
            result = _parse_conversion_response(response)
            
            # If the code is present and not malformed, deduplicate it
            if 'pyspark_code' in result and result['pyspark_code']:
                result['pyspark_code'] = deduplicate_pyspark_code(result['pyspark_code'])
                
            return result
        
        # For larger code, use the original section-based approach
        # Detect the predominant section type in the SAS code
        section_type = _detect_section_type(sas_code)
        
        # Get type-specific prompt based on the section type
        prompt = _get_type_specific_prompt(section_type, sas_code)
        
        # Optimize context to fit within token limits
        optimized_context = _optimize_context_for_tokens(context)
        
        # Format context information for the prompt
        context_text = _format_context_information(optimized_context)
        
        # For very large code, reduce detail in the prompt to avoid throttling
        code_size = len(sas_code)
        if code_size > 15000:
            # Handle large code by optimizing the prompt
            logger.info(f"Large code detected ({code_size} chars). Optimizing prompt for throttling prevention.")
            
            # Estimate token count for planning
            estimated_tokens = (len(prompt) + len(context_text) + len(sas_code)) // 4
            
            # If estimated tokens are high, reduce prompt complexity
            if estimated_tokens > 5000:
                # Simplify prompt for large code
                prompt = """Convert this SAS code to equivalent PySpark code. Focus on correctness.
Return a JSON object with:
- pyspark_code: The converted PySpark code
- annotations: List of important notes about the conversion
- confidence_score: A score from 1-5 rating your confidence in the conversion
- refinement_notes: Any issues encountered or suggestions for improvement
"""
                logger.info("Simplified prompt to reduce token usage")
        
        # Construct the full prompt with SAS code and context
        full_prompt = f"{prompt}\n\n{context_text}SAS CODE:\n```sas\n{sas_code}\n```"
        
        # Call LLM with retry
        response = _call_llm_with_retry(
            lambda p: _invoke_claude37_sonnet(p),
            full_prompt,
            description="SAS to PySpark conversion"
        )
        
        # Parse the conversion response
        result = _parse_conversion_response(response)
        return result
                
    except Exception as e:
        logger.error(f"Unexpected error in call_llm_for_conversion: {str(e)}")
        return {
            'pyspark_code': f"# Unexpected error: {str(e)}",
            'annotations': [f"Error: {str(e)}"],
            'confidence_score': 0.0,
            'refinement_notes': f"Unexpected error: {str(e)}"
        }

def call_llm_for_refinement(pyspark_code, sas_code, annotations=None, previous_confidence=None):
    """
    Reviews and refines PySpark code converted from SAS.
    
    Args:
        pyspark_code (str): The PySpark code to review
        sas_code (str): The original SAS code for reference
        annotations (list, optional): Previous annotations from conversion
        previous_confidence (float, optional): Confidence score from initial conversion
    
    Returns:
        tuple: (refined_code, annotations, confidence_score, refinement_notes)
    """
    try:
        # Create prompt based on whether this is initial review or refinement
        if previous_confidence is None or previous_confidence >= REFINEMENT_THRESHOLD:
            # Initial review prompt
            prompt = f"""Review and refine this PySpark code converted from SAS.

Original SAS code:
```sas
{sas_code}
```

PySpark code to review:
```python
{pyspark_code}
```

Provide the following in a JSON object:
1. refined_code: The improved PySpark code
2. annotations: List of important notes about the code
3. confidence_score: A score from 1-5 rating your confidence in the correctness
4. refinement_notes: Specific improvements made or issues identified

Your review should focus on:
- Correctness of the conversion
- PySpark best practices
- Optimizing performance
- Handling any missed SAS functionality
- Improving readability
"""
        else:
            # Follow-up refinement prompt when previous confidence was low
            prompt = f"""This PySpark code was converted from SAS but has a low confidence score ({previous_confidence}).
Please thoroughly refine it to improve correctness and quality.

Original SAS code:
```sas
{sas_code}
```

Current PySpark code (needs improvement):
```python
{pyspark_code}
```

Previous annotations:
{json.dumps(annotations) if annotations else "None"}

Provide the following in a JSON object:
1. refined_code: The significantly improved PySpark code
2. annotations: Updated list of notes about the code
3. confidence_score: A new score from 1-5 rating your confidence
4. refinement_notes: Detailed explanation of your refinements
"""

        # Call the model
        response = _call_llm_with_retry(
            lambda p: _invoke_claude37_sonnet(p),
            prompt,
            description="PySpark refinement"
        )
        
        # Parse the response
        result = _parse_refinement_response(response)
        
        # Return structured results
        return (
            result.get('refined_code', pyspark_code),
            result.get('annotations', annotations or []),
            float(result.get('confidence_score', 0.0)),
            result.get('refinement_notes', "No refinement notes provided")
        )
        
    except Exception as e:
        logger.error(f"Error during refinement: {str(e)}")
        # Return original code with error annotation
        return (
            pyspark_code,
            (annotations or []) + [{"note": f"Refinement error: {str(e)}", "severity": "Error"}],
            float(previous_confidence or 2.0),
            f"Refinement failed: {str(e)}"
        )

def _parse_refinement_response(response):
    """
    Parses the refinement response from LLM.
    Handles both well-formed JSON and extracts code from non-JSON responses.
    
    Args:
        response (str): The LLM response
        
    Returns:
        dict: Parsed result with refined_code, annotations, confidence_score, and refinement_notes
    """
    # Default result
    result = {
        'refined_code': '',
        'annotations': [],
        'confidence_score': 2.0,
        'refinement_notes': 'Failed to parse response'
    }
    
    try:
        # First attempt: Try direct JSON parsing
        try:
            parsed = json.loads(response)
            if isinstance(parsed, dict):
                # Validate expected fields
                if 'refined_code' in parsed or 'code' in parsed:
                    result = {
                        'refined_code': parsed.get('refined_code', parsed.get('code', '')),
                        'annotations': parsed.get('annotations', []),
                        'confidence_score': float(parsed.get('confidence_score', parsed.get('confidence', 2.0))),
                        'refinement_notes': parsed.get('refinement_notes', parsed.get('notes', ''))
                    }
                    return result
        except json.JSONDecodeError:
            pass  # Continue to fallback parsing methods
            
        # Second attempt: Extract JSON block
        json_pattern = r'```json\s*([\s\S]*?)\s*```'
        json_match = re.search(json_pattern, response)
        if json_match:
            try:
                parsed = json.loads(json_match.group(1))
                if isinstance(parsed, dict):
                    if 'refined_code' in parsed or 'code' in parsed:
                        result = {
                            'refined_code': parsed.get('refined_code', parsed.get('code', '')),
                            'annotations': parsed.get('annotations', []),
                            'confidence_score': float(parsed.get('confidence_score', parsed.get('confidence', 2.0))),
                            'refinement_notes': parsed.get('refinement_notes', parsed.get('notes', ''))
                        }
                        return result
            except json.JSONDecodeError:
                pass  # Continue to next fallback
            
        # Third attempt: Extract code block if JSON parsing fails
        code_pattern = r'```python\s*([\s\S]*?)\s*```'
        code_match = re.search(code_pattern, response)
        if code_match:
            code = code_match.group(1).strip()
            result['refined_code'] = code
            
            # Try to extract confidence score
            confidence_pattern = r'[Cc]onfidence:?\s*(\d+\.?\d*)'
            confidence_match = re.search(confidence_pattern, response)
            if confidence_match:
                try:
                    result['confidence_score'] = float(confidence_match.group(1))
                except ValueError:
                    pass  # Keep default confidence
            
            # Extract notes (everything after code block)
            if code_match.end() < len(response):
                notes = response[code_match.end():].strip()
                result['refinement_notes'] = notes
                
                # Extract simple annotations
                annotation_pattern = r'- (.*?)(?:\n|$)'
                annotations = re.findall(annotation_pattern, notes)
                if annotations:
                    result['annotations'] = [{"note": note.strip()} for note in annotations]
                        
        return result
                
    except Exception as e:
        logger.error(f"Error parsing refinement response: {e}")
        result['refinement_notes'] = f"Error parsing response: {str(e)}"
        return result

def _parse_conversion_response(response):
    """
    Parses the conversion response from LLM.
    Handles both well-formed JSON and extracts code from non-JSON responses.
    
    Args:
        response (str): The LLM response
        
    Returns:
        dict: Parsed result with pyspark_code, annotations, confidence_score, and refinement_notes
    """
    # Default result
    result = {
        'pyspark_code': '',
        'annotations': [],
        'confidence_score': 2.0,
        'refinement_notes': 'Failed to parse response'
    }
    
    if not response or not isinstance(response, str):
        logger.error(f"Invalid response: {type(response)}")
        return result
        
    logger.info(f"Parsing response of length {len(response)} characters")
    logger.info(f"Response preview: {response[:200]}...")
    
    try:
        # First attempt: Try direct JSON parsing
        try:
            parsed = json.loads(response)
            if isinstance(parsed, dict):
                # Validate expected fields
                if 'pyspark_code' in parsed or 'code' in parsed:
                    result = {
                        'pyspark_code': parsed.get('pyspark_code', parsed.get('code', '')),
                        'annotations': parsed.get('annotations', []),
                        'confidence_score': float(parsed.get('confidence_score', parsed.get('confidence', 2.0))),
                        'refinement_notes': parsed.get('refinement_notes', parsed.get('notes', ''))
                    }
                    logger.info(f"Successfully parsed JSON response, extracted {len(result['pyspark_code'])} characters of PySpark code")
                    return result
        except json.JSONDecodeError:
            logger.info("JSON parsing failed, trying alternate methods")
            
        # Second attempt: Extract JSON block
        json_pattern = r'```json\s*([\s\S]*?)\s*```'
        json_match = re.search(json_pattern, response)
        if json_match:
            try:
                json_str = json_match.group(1).strip()
                logger.info(f"Found JSON block of length {len(json_str)}")
                parsed = json.loads(json_str)
                if isinstance(parsed, dict):
                    if 'pyspark_code' in parsed or 'code' in parsed:
                        result = {
                            'pyspark_code': parsed.get('pyspark_code', parsed.get('code', '')),
                            'annotations': parsed.get('annotations', []),
                            'confidence_score': float(parsed.get('confidence_score', parsed.get('confidence', 2.0))),
                            'refinement_notes': parsed.get('refinement_notes', parsed.get('notes', ''))
                        }
                        logger.info(f"Successfully parsed JSON block, extracted {len(result['pyspark_code'])} characters of PySpark code")
                        return result
            except json.JSONDecodeError as e:
                logger.info(f"JSON block parsing failed: {e}")
            
        # Third attempt: Extract code block if JSON parsing fails
        code_pattern = r'```(?:python|pyspark)?\s*([\s\S]*?)\s*```'
        code_matches = list(re.finditer(code_pattern, response))
        
        if code_matches:
            # Use the largest code block found
            largest_match = max(code_matches, key=lambda m: len(m.group(1)))
            code = largest_match.group(1).strip()
            
            if code:
                logger.info(f"Found code block of length {len(code)}")
                result['pyspark_code'] = code
                
                # Try to extract confidence score
                confidence_pattern = r'[Cc]onfidence:?\s*(\d+\.?\d*)'
                confidence_match = re.search(confidence_pattern, response)
                if confidence_match:
                    try:
                        result['confidence_score'] = float(confidence_match.group(1))
                        logger.info(f"Extracted confidence score: {result['confidence_score']}")
                    except ValueError:
                        pass  # Keep default confidence
                
                # Extract notes (everything after code block)
                if largest_match.end() < len(response):
                    notes = response[largest_match.end():].strip()
                    result['refinement_notes'] = notes
                    
                    # Extract simple annotations
                    annotation_pattern = r'- (.*?)(?:\n|$)'
                    annotations = re.findall(annotation_pattern, notes)
                    if annotations:
                        result['annotations'] = [{"note": note.strip()} for note in annotations]
                
                logger.info(f"Successfully extracted code block, code length: {len(result['pyspark_code'])}")
                return result
        else:
            # Last resort: If no code blocks found, but response has multiple lines,
            # try to use the whole response as code
            if '\n' in response and 'import' in response and ('spark' in response.lower() or 'pyspark' in response.lower()):
                logger.info("No code blocks found, but response looks like code. Using entire response.")
                result['pyspark_code'] = response.strip()
                result['refinement_notes'] = "Used entire response as code - no code blocks found"
                return result
            
        # If we got here, we couldn't extract any code
        logger.warning("Failed to extract valid PySpark code from response")
        result['refinement_notes'] = "Could not extract PySpark code from response"
        return result
                
    except Exception as e:
        logger.error(f"Error parsing conversion response: {e}")
        result['refinement_notes'] = f"Error parsing response: {str(e)}"
        return result

def _identify_logical_sections(sas_code):
    """
    Identify logical sections in SAS code for processing.
    Uses the SASCodeChunker to break down the code into components.
    """
    try:
        chunker = SASCodeChunker()
        # Changed to use the already modified identify_major_sections method
        # which now has duplicate prevention logic
        sections = chunker.identify_major_sections(sas_code)
        
        # If we have too many sections (over 20), consolidate them
        if len(sections) > 20:
            logger.info(f"Found {len(sections)} sections - consolidating to reduce processing overhead")
            consolidated_sections = []
            
            # Group sections in batches
            batch_size = max(1, len(sections) // 10)  # Aim for around 10 consolidated sections
            
            for i in range(0, len(sections), batch_size):
                batch = sections[i:i+batch_size]
                
                # Create a consolidated section from this batch
                consolidated_code = "\n\n".join(section.get('code', '') for section in batch)
                section_names = [section.get('name', 'unknown') for section in batch]
                
                consolidated_sections.append({
                    'name': f"Consolidated ({i//batch_size+1}): {section_names[0]}...",
                    'type': 'CONSOLIDATED',
                    'code': consolidated_code,
                    'dependencies': []
                })
            
            logger.info(f"Consolidated into {len(consolidated_sections)} sections")
            sections = consolidated_sections
        
        # If no major sections found, create a single section for the entire code
        if not sections:
            logger.info("No logical sections identified, creating a single section")
            sections = [{
                'name': 'complete_sas_code',
                'type': 'UNKNOWN',
                'code': sas_code,
                'dependencies': []
            }]
            
        return sections
    except Exception as e:
        logger.error(f"Error identifying logical sections: {e}")
        # Return a single section as fallback
        return [{
            'name': 'error_fallback',
            'type': 'UNKNOWN',
            'code': sas_code,
            'dependencies': []
        }]

# Add new function for generating filenames
def generate_pyspark_filename(sas_code, result):
    """
    Generates a simple filename for the converted PySpark code with a timestamp.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Construct simple filename
    filename = f"converted_pyspark_{timestamp}.py"
    return filename

# Add function to save to S3
def save_to_s3(pyspark_code, filename, bucket_name="converted-pyspark-code"):
    """
    Saves the PySpark code to an S3 bucket with the given filename.
    Returns the S3 URI of the saved file.
    """
    try:
        # Deduplicate and format code before saving
        formatted_code = deduplicate_pyspark_code(pyspark_code)
        
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Body=formatted_code.encode('utf-8'),
            Bucket=bucket_name,
            Key=filename,
            ContentType='text/x-python'
        )
        s3_uri = f"s3://{bucket_name}/{filename}"
        logger.info(f"Successfully saved PySpark code to {s3_uri}")
        return s3_uri
    except Exception as e:
        logger.error(f"Error saving to S3: {e}")
        raise

# Add this new function for removing duplicates from PySpark code
def deduplicate_pyspark_code(pyspark_code):
    """
    Removes duplicate code blocks from PySpark code.
    Often LLM-based conversion repeats similar blocks when processing sections separately.
    """
    if not pyspark_code:
        return pyspark_code

    # Split the code into lines
    lines = pyspark_code.split('\n')
    
    # Find imports and consolidate them
    import_lines = []
    non_import_lines = []
    
    for line in lines:
        line_stripped = line.strip()
        if line_stripped.startswith('import ') or line_stripped.startswith('from '):
            if line_stripped not in import_lines:
                import_lines.append(line_stripped)
        else:
            non_import_lines.append(line)
    
    # Now detect and remove duplicate function and class definitions
    seen_functions = set()
    seen_classes = set()
    deduplicated_lines = []
    
    i = 0
    while i < len(non_import_lines):
        line = non_import_lines[i].strip()
        
        # Check for function definitions
        if line.startswith('def '):
            # Extract function signature
            func_name = line.split('(')[0].replace('def ', '').strip()
            
            # Find the entire function
            function_lines = []
            function_lines.append(non_import_lines[i])
            
            j = i + 1
            indent_level = len(non_import_lines[i]) - len(non_import_lines[i].lstrip())
            
            # Continue until we find a line with the same or less indentation
            while j < len(non_import_lines):
                if non_import_lines[j].strip() == '':
                    function_lines.append(non_import_lines[j])
                    j += 1
                    continue
                    
                curr_indent = len(non_import_lines[j]) - len(non_import_lines[j].lstrip())
                if curr_indent <= indent_level and non_import_lines[j].strip() != '':
                    break
                    
                function_lines.append(non_import_lines[j])
                j += 1
            
            # Create a hash of the function content for comparison
            function_hash = hashlib.md5('\n'.join(function_lines).encode()).hexdigest()
            
            if function_hash not in seen_functions:
                seen_functions.add(function_hash)
                deduplicated_lines.extend(function_lines)
                
            i = j
        
        # Check for class definitions
        elif line.startswith('class '):
            # Extract class name
            class_name = line.split('(')[0].replace('class ', '').strip()
            
            # Find the entire class
            class_lines = []
            class_lines.append(non_import_lines[i])
            
            j = i + 1
            indent_level = len(non_import_lines[i]) - len(non_import_lines[i].lstrip())
            
            # Continue until we find a line with the same or less indentation
            while j < len(non_import_lines):
                if non_import_lines[j].strip() == '':
                    class_lines.append(non_import_lines[j])
                    j += 1
                    continue
                    
                curr_indent = len(non_import_lines[j]) - len(non_import_lines[j].lstrip())
                if curr_indent <= indent_level and non_import_lines[j].strip() != '':
                    break
                    
                class_lines.append(non_import_lines[j])
                j += 1
            
            # Create a hash of the class content for comparison
            class_hash = hashlib.md5('\n'.join(class_lines).encode()).hexdigest()
            
            if class_hash not in seen_classes:
                seen_classes.add(class_hash)
                deduplicated_lines.extend(class_lines)
                
            i = j
            
        # Simple deduplication of identical statements
        elif i > 0 and line and line == non_import_lines[i-1].strip():
            # Skip duplicated lines
            i += 1
        else:
            deduplicated_lines.append(non_import_lines[i])
            i += 1
    
    # Add a header
    header = [
        "# PySpark Conversion",
        f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "# This file contains PySpark code converted from SAS.",
        "",
        "# Imports"
    ]
    
    # Combine everything
    final_code = '\n'.join(header) + '\n' + '\n'.join(sorted(import_lines)) + '\n\n' + '\n'.join(deduplicated_lines)
    
    # Remove multiple blank lines
    while '\n\n\n' in final_code:
        final_code = final_code.replace('\n\n\n', '\n\n')
    
    return final_code

def _large_code_conversion_simplified(sas_code, converter):
    """
    Simplified version of large code conversion that processes code in chunks.
    Only splits code if it's over 2000 lines, and uses intelligent chunking.
    """
    logger.info("Using simplified large code conversion")
    
    # Count lines in the code
    lines = sas_code.splitlines()
    line_count = len(lines)
    logger.info(f"Code has {line_count} lines")
    
    # If code is under 2000 lines, process it as a single unit
    if line_count <= 2000:
        logger.info("Processing code as a single unit since it's under 2000 lines")
        return _process_chunk_with_context({
            'type': 'SINGLE',
            'name': 'full_code',
            'content': sas_code,
            'code': sas_code,
            'dependencies': []
        }, {}, converter)
    
    # For code over 2000 lines, split into exactly 3 chunks with intelligent boundaries
    logger.info("Code exceeds 2000 lines, splitting into 3 intelligent chunks")
    
    # Define important SAS statements for boundary detection in priority order
    boundary_markers = [
        # Priority 1: Macro definitions (strongest boundary)
        (r'^\s*%MACRO\s+\w+', 200),
        # Priority 2: PROC blocks
        (r'^\s*PROC\s+\w+', 150),
        # Priority 3: DATA steps
        (r'^\s*DATA\s+\w+', 100),
        # Priority 4: RUN statements (weakest boundary)
        (r'^\s*RUN\s*;', 50)
    ]
    
    # Calculate ideal chunk boundaries
    target_chunk_size = line_count // 3
    boundary_points = [target_chunk_size, 2 * target_chunk_size]
    
    # Find optimal chunk boundaries
    actual_boundaries = [0]  # Start with first line
    
    for target_point in boundary_points:
        best_boundary = target_point
        highest_priority = -1
        search_range = min(300, line_count // 6)  # Look up to 300 lines or 1/6 of code
        
        # Search around the target point for the best boundary
        for offset in range(-search_range, search_range + 1):
            potential_boundary = target_point + offset
            if potential_boundary <= actual_boundaries[-1] or potential_boundary >= line_count:
                continue
                
            line = lines[potential_boundary].strip().upper()
            
            # Check if this line matches any boundary marker
            for pattern, priority in boundary_markers:
                if re.match(pattern, line, re.IGNORECASE):
                    # If higher priority or closer to target (when same priority)
                    if (priority > highest_priority) or \
                       (priority == highest_priority and abs(potential_boundary - target_point) < abs(best_boundary - target_point)):
                        highest_priority = priority
                        best_boundary = potential_boundary
                        break
        
        actual_boundaries.append(best_boundary)
    
    actual_boundaries.append(line_count)  # End with last line
    logger.info(f"Identified chunk boundaries at lines: {actual_boundaries[1:-1]}")
    
    # Create chunks based on the identified boundaries
    chunks = []
    for i in range(3):
        start_idx = actual_boundaries[i]
        end_idx = actual_boundaries[i+1]
        
        chunk_content = '\n'.join(lines[start_idx:end_idx])
        chunks.append({
            'type': 'COMBINED',
            'name': f'combined_section_{i+1}',
            'content': chunk_content,
            'code': chunk_content,
            'dependencies': []
        })
        logger.info(f"Created chunk {i+1} with {end_idx - start_idx} lines ({len(chunk_content)} chars)")
    
    # Process chunks in sequence
    all_results = []
    for chunk in chunks:
        logger.info(f"Processing chunk: {chunk['name']} ({len(chunk['content'])} chars)")
        result = _process_chunk_with_context(chunk, {}, converter)
        all_results.append(result)
        logger.info(f"Processed {chunk['name']} with confidence {result.get('confidence_score', 0.0)}")
    
    # Combine results
    final_result = _combine_results(all_results)
    
    # Add import statements
    imports = _generate_import_statements(final_result['pyspark_code'])
    final_result['pyspark_code'] = imports + final_result['pyspark_code']
    
    return final_result

def _generate_import_statements(pyspark_code):
    """Generate necessary import statements based on the PySpark code"""
    imports = [
        "from pyspark.sql import SparkSession, functions as F, Window",
        "from pyspark.sql.types import *",
        "import pandas as pd",
        "import numpy as np",
        ""  # Empty line after imports
    ]
    return "\n".join(imports) + "\n\n"