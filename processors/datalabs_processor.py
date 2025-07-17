"""
DataLabs processor for manuals with markdown formatting and image descriptions.
"""

import requests
import json
import time
import logging
from typing import Dict, Any, List, Optional
from .base_processor import BaseProcessor
from models.document import Document
from config.settings import settings
from core.circuit_breaker import CircuitBreakerConfig, circuit_breaker_manager


logger = logging.getLogger(__name__)


class DataLabsProcessor(BaseProcessor):
    """Processor for manuals using DataLabs API for markdown conversion."""
    
    SUPPORTED_TYPES = ['manual', 'spreadsheet']
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.api_key = getattr(settings, 'DATALABS_API_KEY', None)
        self.base_url = config.get('base_url', 'https://www.datalab.to/api/v1/marker') if config else 'https://www.datalab.to/api/v1/marker'
        self.timeout = config.get('timeout', 300) if config else 300  # 5 minutes
        self.poll_interval = config.get('poll_interval', 10) if config else 10  # 10 seconds
        
        if not self.api_key:
            raise ValueError("DataLabs API key not found in settings")
        
        # Initialize circuit breaker for DataLabs API
        circuit_config = CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=60,
            success_threshold=2,
            timeout=30,
            expected_exceptions=(requests.RequestException, requests.HTTPError, requests.Timeout)
        )
        self.circuit_breaker = circuit_breaker_manager.get_circuit_breaker('datalabs_api', circuit_config)
    
    def supports_document_type(self, document_type: str) -> bool:
        """Check if processor supports given document type."""
        return document_type in self.SUPPORTED_TYPES
    
    def process(self, document: Document, content: bytes) -> Dict[str, Any]:
        """
        Process document using DataLabs API for markdown conversion.
        
        Args:
            document: Document metadata
            content: Raw document content as bytes
            
        Returns:
            Dictionary with markdown formatted content and page identifiers
        """
        if not self.validate_content(content):
            raise ValueError("Invalid document content")
        
        try:
            # Submit document for processing
            check_url = self._submit_document(document, content)
            
            # Poll for completion
            result = self._poll_for_completion(check_url)
            
            # Parse and structure the result
            structured_result = self._parse_datalabs_result(document, result)
            
            # Save outputs to files for inspection
            self._save_processing_outputs(document, result, structured_result)
            
            return structured_result
            
        except Exception as e:
            logger.error(f"Error processing document {document.filename} with DataLabs: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Exception details: {repr(e)}")
            return {
                'pages': [],
                'document_metadata': {
                    'filename': document.filename,
                    'document_type': document.document_type,
                    'processing_method': 'datalabs_markdown'
                },
                'processing_info': {
                    'processor': 'DataLabsProcessor',
                    'success': False,
                    'error': str(e),
                    'error_type': type(e).__name__
                }
            }
    
    def _submit_document(self, document: Document, content: bytes) -> str:
        """
        Submit document to DataLabs for processing with circuit breaker protection.
        
        Args:
            document: Document metadata
            content: Raw document content
            
        Returns:
            Job ID for tracking processing status
        """
        def _submit_request():
            # Prepare the request
            files = {
                'file': (document.filename, content, 'application/pdf')
            }
            
            headers = {
                'X-API-Key': self.api_key
            }
            
            # Configuration for markdown output with page identifiers
            data = {
                'output_format': 'markdown',
                'paginate': True,
                'use_llm': True,
                'format_lines': True,
                'disable_image_extraction': True
            }
            
            # Submit the job - base_url already includes full path
            response = requests.post(
                self.base_url,
                headers=headers,
                files=files,
                data=data,
                timeout=30
            )
            
            if response.status_code != 200:
                raise requests.HTTPError(f"Failed to submit document to DataLabs: {response.status_code} - {response.text}")
            
            result = response.json()
            request_id = result.get('request_id')
            check_url = result.get('request_check_url')
            
            if not request_id or not check_url:
                raise Exception("No request ID or check URL returned from DataLabs")
            
            logger.info(f"Document {document.filename} submitted to DataLabs with request ID: {request_id}")
            return check_url
        
        # Execute with circuit breaker protection
        return self.circuit_breaker.call(_submit_request)
    
    def _poll_for_completion(self, check_url: str) -> Dict[str, Any]:
        """
        Poll DataLabs API for job completion with circuit breaker protection.
        
        Args:
            check_url: URL to check status for
            
        Returns:
            Processing result from DataLabs
        """
        def _check_status():
            headers = {
                'X-API-Key': self.api_key
            }
            
            response = requests.get(
                check_url,
                headers=headers,
                timeout=30
            )
            
            if response.status_code != 200:
                raise requests.HTTPError(f"Failed to check job status: {response.status_code} - {response.text}")
            
            return response.json()
        
        start_time = time.time()
        
        while time.time() - start_time < self.timeout:
            try:
                # Use circuit breaker for status check
                result = self.circuit_breaker.call(_check_status)
                status = result.get('status')
                
                if status == 'complete':
                    logger.info(f"DataLabs job completed successfully")
                    return result  # Result data is directly in the response
                
                elif status == 'failed':
                    error_message = result.get('error', 'Unknown error')
                    raise Exception(f"DataLabs processing failed: {error_message}")
                
                elif status == 'processing':
                    logger.info(f"DataLabs job still processing...")
                    time.sleep(self.poll_interval)
                
                else:
                    logger.warning(f"Unknown status from DataLabs: {status}")
                    time.sleep(self.poll_interval)
                    
            except Exception as e:
                logger.error(f"Error while polling DataLabs: {str(e)}")
                time.sleep(self.poll_interval)
        
        raise Exception(f"DataLabs processing timed out after {self.timeout} seconds")
    
    def _parse_datalabs_result(self, document: Document, datalabs_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse and structure DataLabs result into our standard format.
        
        Args:
            document: Original document metadata
            datalabs_result: Result from DataLabs API
            
        Returns:
            Structured result in our standard format
        """
        pages = []
        
        # Extract markdown content from DataLabs result
        markdown_content = datalabs_result.get('markdown', '')
        page_count = datalabs_result.get('page_count', 1)
        
        # If pagination is enabled, split by page delimiters
        if self._is_paginated(markdown_content):
            page_sections = self._split_paginated_content(markdown_content)
            for i, section in enumerate(page_sections):
                page_number = i + 1
                page_data = {
                    'page_number': page_number,
                    'page_id': f'{document.filename}_page_{page_number}',
                    'content': section.strip(),
                    'metadata': {
                        'character_count': len(section),
                        'word_count': len(section.split()),
                        'has_content': bool(section.strip())
                    }
                }
                pages.append(page_data)
        else:
            # Single page content
            page_data = {
                'page_number': 1,
                'page_id': f'{document.filename}_page_1',
                'content': markdown_content.strip(),
                'metadata': {
                    'character_count': len(markdown_content),
                    'word_count': len(markdown_content.split()),
                    'has_content': bool(markdown_content.strip())
                }
            }
            pages.append(page_data)
        
        # Build structured result
        structured_result = {
            'pages': pages,
            'document_metadata': {
                'filename': document.filename,
                'document_type': document.document_type,
                'processing_method': 'datalabs_markdown',
                'page_count': page_count,
                'total_characters': sum(p['metadata']['character_count'] for p in pages),
                'total_words': sum(p['metadata']['word_count'] for p in pages)
            },
            'processing_info': {
                'processor': 'DataLabsProcessor',
                'success': True,
                'processing_time': None,  # Will be set by caller
                'api_response': datalabs_result
            }
        }
        
        return structured_result
    
    def _is_paginated(self, content: str) -> bool:
        """Check if content contains page delimiters"""
        # DataLabs uses page delimiters when paginate=True
        return '---' in content or '\n\n# ' in content or 'Page ' in content
    
    def _split_paginated_content(self, content: str) -> List[str]:
        """Split paginated content into individual pages"""
        # Simple split by common page delimiters
        # This could be made more sophisticated based on actual DataLabs output
        if '---' in content:
            return [page.strip() for page in content.split('---') if page.strip()]
        elif '\n\n# ' in content:
            # Split by header patterns
            sections = content.split('\n\n# ')
            if sections:
                pages = [sections[0]]  # First section
                pages.extend([f'# {section}' for section in sections[1:]])
                return [page.strip() for page in pages if page.strip()]
        
        # Fallback: return as single page
        return [content.strip()]
    
    def _save_processing_outputs(self, document: Document, raw_result: Dict[str, Any], structured_result: Dict[str, Any]) -> None:
        """Save DataLabs processing outputs to files for inspection"""
        import json
        import os
        from datetime import datetime
        
        # Create outputs directory if it doesn't exist
        output_dir = "datalabs_outputs"
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename based on document name and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = document.filename.replace('.pdf', '').replace('.', '_')
        
        try:
            # Save raw DataLabs API response
            raw_filename = f"{output_dir}/{base_filename}_{timestamp}_raw_response.json"
            with open(raw_filename, 'w', encoding='utf-8') as f:
                json.dump(raw_result, f, indent=2, ensure_ascii=False)
            
            # Save structured result
            structured_filename = f"{output_dir}/{base_filename}_{timestamp}_structured_result.json"
            with open(structured_filename, 'w', encoding='utf-8') as f:
                json.dump(structured_result, f, indent=2, ensure_ascii=False)
            
            # Save just the markdown content for easy viewing
            markdown_content = raw_result.get('markdown', '')
            if markdown_content:
                markdown_filename = f"{output_dir}/{base_filename}_{timestamp}_content.md"
                with open(markdown_filename, 'w', encoding='utf-8') as f:
                    f.write(markdown_content)
            
            logger.info(f"Saved DataLabs outputs to {output_dir}:")
            logger.info(f"  - Raw response: {raw_filename}")
            logger.info(f"  - Structured result: {structured_filename}")
            logger.info(f"  - Markdown content: {markdown_filename}")
            
        except Exception as e:
            logger.error(f"Error saving DataLabs outputs: {e}")
    
    def validate_content(self, content: bytes) -> bool:
        """
        Validate that content can be processed by DataLabs.
        
        Args:
            content: Raw document content
            
        Returns:
            True if content is valid for DataLabs processing
        """
        if not super().validate_content(content):
            return False
        
        # Check for supported file types (PDF is primary)
        if content.startswith(b'%PDF-'):
            return True
        
        # Check for other supported formats if needed
        # (Word docs, etc.)
        
        return False
    
    def get_supported_formats(self) -> List[str]:
        """
        Get list of supported file formats.
        
        Returns:
            List of supported file format extensions
        """
        return ['.pdf', '.docx', '.doc']
    
    def get_circuit_breaker_stats(self) -> Dict[str, Any]:
        """
        Get circuit breaker statistics for DataLabs API.
        
        Returns:
            Circuit breaker statistics
        """
        return self.circuit_breaker.get_stats()