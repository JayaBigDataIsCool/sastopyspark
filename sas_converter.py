import re
import json
import logging
from typing import Dict, List, Tuple, Optional, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SASCodeChunker:
    """
    Handles intelligent chunking of SAS code based on logical boundaries.
    """
    
    @staticmethod
    def identify_chunks(sas_code: str) -> List[Dict[str, Any]]:
        """
        Identifies logical chunks in SAS code (PROC steps, DATA steps, MACRO definitions).
        Returns a list of chunks with metadata.
        """
        chunks = []
        
        # Regular expressions for different SAS code blocks
        patterns = {
            'proc': r'PROC\s+(\w+)\s+[^;]*;.*?RUN;',
            'data': r'DATA\s+([^;]*);.*?RUN;',
            'macro': r'%MACRO\s+(\w+)\s*\([^)]*\).*?%MEND\s*\1;'
        }
        
        # Find all chunks
        for chunk_type, pattern in patterns.items():
            matches = re.finditer(pattern, sas_code, re.DOTALL | re.IGNORECASE)
            for match in matches:
                chunk_name = match.group(1).strip()
                chunk_content = match.group(0)
                
                chunks.append({
                    'type': chunk_type,
                    'name': chunk_name,
                    'content': chunk_content,
                    'dependencies': SASCodeChunker._extract_dependencies(chunk_content)
                })
        
        return chunks
    
    @staticmethod
    def _extract_dependencies(chunk_content: str) -> List[str]:
        """
        Extracts dependencies from a SAS code chunk.
        """
        dependencies = []
        
        # Look for dataset references
        dataset_refs = re.findall(r'DATA\s+([^;]+);|SET\s+([^;]+);|MERGE\s+([^;]+);', 
                                 chunk_content, re.IGNORECASE)
        
        for ref in dataset_refs:
            # Take the first non-empty match
            dataset = next((r for r in ref if r), None)
            if dataset:
                dependencies.append(dataset.strip())
        
        return dependencies

class SASContextManager:
    """
    Manages context and state during SAS to PySpark conversion.
    """
    
    def __init__(self):
        self.dataset_schemas = {}
        self.macro_definitions = {}
        self.variable_types = {}
        self.processing_order = []
    
    def update_schema(self, dataset_name: str, schema: Dict[str, str]):
        """Updates the schema for a dataset."""
        self.dataset_schemas[dataset_name] = schema
    
    def get_schema(self, dataset_name: str) -> Optional[Dict[str, str]]:
        """Retrieves the schema for a dataset."""
        return self.dataset_schemas.get(dataset_name)
    
    def add_macro(self, macro_name: str, definition: str):
        """Adds a macro definition to the context."""
        self.macro_definitions[macro_name] = definition
    
    def get_macro(self, macro_name: str) -> Optional[str]:
        """Retrieves a macro definition."""
        return self.macro_definitions.get(macro_name)
    
    def update_processing_order(self, chunk_name: str):
        """Updates the processing order of chunks."""
        if chunk_name not in self.processing_order:
            self.processing_order.append(chunk_name)

class SASConverter:
    """
    Main class for converting SAS code to PySpark.
    """
    
    def __init__(self, llm_caller):
        self.llm_caller = llm_caller
        self.context = SASContextManager()
    
    def convert(self, sas_code: str) -> Dict[str, Any]:
        """
        Converts SAS code to PySpark using intelligent chunking.
        """
        # Step 1: Analyze and chunk the code
        chunks = SASCodeChunker.identify_chunks(sas_code)
        
        # Step 2: Determine processing order based on dependencies
        processing_order = self._determine_processing_order(chunks)
        
        # Step 3: Process each chunk in order
        results = []
        for chunk_name in processing_order:
            chunk = next(c for c in chunks if c['name'] == chunk_name)
            result = self._process_chunk(chunk)
            results.append(result)
        
        # Step 4: Assemble final PySpark code
        final_code = self._assemble_final_code(results)
        
        return {
            'pyspark_code': final_code,
            'annotations': self._generate_annotations(results),
            'confidence': self._calculate_confidence(results)
        }
    
    def _determine_processing_order(self, chunks: List[Dict[str, Any]]) -> List[str]:
        """
        Determines the order in which chunks should be processed based on dependencies.
        """
        # Simple topological sort based on dependencies
        order = []
        visited = set()
        
        def visit(chunk_name):
            if chunk_name in visited:
                return
            visited.add(chunk_name)
            
            chunk = next(c for c in chunks if c['name'] == chunk_name)
            for dep in chunk['dependencies']:
                visit(dep)
            
            order.append(chunk_name)
        
        for chunk in chunks:
            if chunk['name'] not in visited:
                visit(chunk['name'])
        
        return order
    
    def _process_chunk(self, chunk: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes a single chunk of SAS code.
        """
        # Prepare context-aware prompt
        context_info = self._prepare_context_info(chunk)
        
        prompt = f"""Convert this SAS code chunk to PySpark, considering the following context:
{json.dumps(context_info, indent=2)}

SAS Code:
```sas
{chunk['content']}
```

Return a JSON object with:
1. pyspark_code: The converted code
2. schema_updates: Any new dataset schemas discovered
3. annotations: Conversion notes
4. confidence: Score from 1-5
"""
        
        # Call LLM for conversion
        response = self.llm_caller(prompt)
        result = json.loads(response)
        
        # Update context with new information
        if 'schema_updates' in result:
            for dataset, schema in result['schema_updates'].items():
                self.context.update_schema(dataset, schema)
        
        return result
    
    def _prepare_context_info(self, chunk: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepares context information for the LLM prompt.
        """
        return {
            'available_schemas': self.context.dataset_schemas,
            'available_macros': list(self.context.macro_definitions.keys()),
            'chunk_type': chunk['type'],
            'chunk_name': chunk['name'],
            'dependencies': chunk['dependencies']
        }
    
    def _assemble_final_code(self, results: List[Dict[str, Any]]) -> str:
        """
        Assembles the final PySpark code from processed chunks.
        """
        # Start with imports
        final_code = """from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd
import numpy as np

# Initialize Spark session
spark = SparkSession.builder \\
    .appName("SAS_Conversion") \\
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \\
    .getOrCreate()

"""
        
        # Add each chunk's code
        for result in results:
            final_code += f"\n# {result.get('chunk_name', 'Unknown Chunk')}\n"
            final_code += result['pyspark_code']
            final_code += "\n"
        
        return final_code
    
    def _generate_annotations(self, results: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """
        Generates annotations from all processed chunks.
        """
        annotations = []
        for result in results:
            annotations.extend(result.get('annotations', []))
        return annotations
    
    def _calculate_confidence(self, results: List[Dict[str, Any]]) -> float:
        """
        Calculates overall confidence score.
        """
        if not results:
            return 1.0
        
        confidences = [float(r.get('confidence', 1.0)) for r in results]
        return sum(confidences) / len(confidences) 