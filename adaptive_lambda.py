import json
import re
import logging
import time
import os
from functools import lru_cache
from lambda_function import (
    _invoke_claude37_sonnet,
    _call_llm_with_retry,
    deterministic_split,
    basic_lint_check,
    token_usage_stats,
    llm_call_counts
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration
SIMPLE_MAX_LINES = int(os.environ.get("SIMPLE_MAX_LINES", "500"))
SIMPLE_MAX_MACROS = int(os.environ.get("SIMPLE_MAX_MACROS", "5")) 
SIMPLE_MAX_PROCS = int(os.environ.get("SIMPLE_MAX_PROCS", "10"))
REFINEMENT_THRESHOLD = float(os.environ.get("REFINEMENT_THRESHOLD", "3.5"))
CONTEXT_SIZE_LIMIT = int(os.environ.get("CONTEXT_SIZE_LIMIT", "150000"))

class AdaptiveConverter:
    def __init__(self):
        self.overall_context = {}
        
    def assess_complexity(self, sas_code):
        """Determine if code is Simple or Complex based on heuristics"""
        loc = sas_code.count('\n') + 1
        macro_defs = len(re.findall(r'^\s*%MACRO\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))
        proc_calls = len(re.findall(r'^\s*PROC\s+\w+', sas_code, re.IGNORECASE | re.MULTILINE))
        
        is_simple = (
            loc <= SIMPLE_MAX_LINES and
            macro_defs <= SIMPLE_MAX_MACROS and
            proc_calls <= SIMPLE_MAX_PROCS
        )
        return "Simple" if is_simple else "Complex"

    def simple_conversion(self, sas_code, options):
        """Direct conversion path for simple code"""
        prompt = f"""Convert this SAS code to PySpark with annotations:
```sas
{sas_code}
```
Output JSON with pyspark_code and annotations."""
        
        response = _call_llm_with_retry(
            _invoke_claude37_sonnet, 
            prompt,
            "Simple Conversion",
            llm_call_type_key='simple_conversion'
        )
        
        try:
            result = json.loads(response)
            pyspark_code = result['pyspark_code']
            annotations = result['annotations']
            pyspark_code, _ = basic_lint_check(pyspark_code)
            return pyspark_code, annotations
        except Exception as e:
            logger.error(f"Simple conversion failed: {e}")
            raise

    def complex_conversion(self, sas_code, options):
        """Multi-stage conversion for complex code"""
        super_chunks = deterministic_split(sas_code)
        final_pyspark = []
        final_annotations = []
        cumulative_line_offset = 0
        
        for super_chunk in super_chunks:
            # Structural analysis
            logical_chunks = self._structural_analysis(super_chunk)
            
            for chunk in logical_chunks:
                # Convert each chunk with context
                pyspark_chunk, annotations, confidence = self._convert_chunk(
                    chunk['code'], 
                    self.overall_context,
                    chunk
                )
                
                # Refine if low confidence
                if confidence < REFINEMENT_THRESHOLD:
                    pyspark_chunk, confidence, _ = self._refine_chunk(
                        pyspark_chunk,
                        self.overall_context
                    )
                
                # Update context
                self._update_context(chunk, pyspark_chunk)
                
                # Remap line numbers and accumulate
                final_pyspark.append(pyspark_chunk)
                final_annotations.extend(self._remap_annotations(
                    annotations,
                    cumulative_line_offset,
                    len(final_pyspark)
                ))
                
                cumulative_line_offset += len(super_chunk.splitlines())
        
        # Assemble final code
        full_code = "\n\n".join(final_pyspark)
        full_code, _ = basic_lint_check(full_code)
        return full_code, final_annotations

    def _structural_analysis(self, super_chunk):
        """Analyze super-chunk structure"""
        prompt = f"""Analyze this SAS code structure:
```sas
{super_chunk}
```
Output logical chunks as JSON."""
        
        response = _call_llm_with_retry(
            _invoke_claude37_sonnet,
            prompt,
            "Structural Analysis", 
            llm_call_type_key='structural_analysis'
        )
        return json.loads(response)

    def _convert_chunk(self, chunk_code, context, chunk_info):
        """Convert a single chunk with context"""
        prompt = f"""Convert this SAS chunk to PySpark using context:
Context: {json.dumps(context)}
SAS:
```sas
{chunk_code}
```
Output JSON with code, annotations and confidence."""
        
        response = _call_llm_with_retry(
            _invoke_claude37_sonnet,
            prompt,
            "Chunk Conversion",
            llm_call_type_key='chunk_conversion'
        )
        
        result = json.loads(response)
        return (
            result['pyspark_code'],
            result['annotations'],
            result['confidence_score']
        )

    def _refine_chunk(self, pyspark_code, context):
        """Refine a PySpark chunk"""
        prompt = f"""Refine this PySpark code using context:
Context: {json.dumps(context)}
PySpark:
```python
{pyspark_code}
```
Output JSON with refined code, confidence and notes."""
        
        response = _call_llm_with_retry(
            _invoke_claude37_sonnet,
            prompt,
            "Refinement",
            llm_call_type_key='refinement'
        )
        
        result = json.loads(response)
        return (
            result['refined_code'],
            result['confidence_score'],
            result['refinement_notes']
        )

    def _update_context(self, chunk_info, pyspark_code):
        """Update overall context with new info"""
        # Simplified context update - would be enhanced in real implementation
        if chunk_info['type'] == 'MACRO':
            self.overall_context.setdefault('macros', {})[chunk_info['name']] = {
                'definition': pyspark_code
            }
        else:
            self.overall_context.setdefault('datasets', {})[chunk_info['name']] = {
                'schema': 'inferred'  # Would extract schema in real implementation
            }

    def _remap_annotations(self, annotations, sas_offset, pyspark_offset):
        """Adjust annotation line numbers"""
        return [{
            **ann,
            'sas_lines': [ann['sas_lines'][0] + sas_offset, 
                         ann['sas_lines'][1] + sas_offset],
            'pyspark_lines': [ann['pyspark_lines'][0] + pyspark_offset,
                            ann['pyspark_lines'][1] + pyspark_offset]
        } for ann in annotations]

def convert_sas_to_pyspark(sas_code, options=None):
    """Main conversion function using adaptive strategy"""
    options = options or {}
    converter = AdaptiveConverter()
    
    # Assess complexity
    complexity = converter.assess_complexity(sas_code)
    logger.info(f"Using {complexity} conversion path")
    
    # Execute appropriate path
    if complexity == "Simple":
        pyspark_code, annotations = converter.simple_conversion(sas_code, options)
    else:
        pyspark_code, annotations = converter.complex_conversion(sas_code, options)
    
    # Prepare stats
    stats = {
        'token_usage': token_usage_stats,
        'llm_calls': llm_call_counts,
        'strategy': f"Adaptive-{complexity}"
    }
    
    return {
        'pyspark_code': pyspark_code,
        'annotations': annotations,
        'processing_stats': stats
    }
