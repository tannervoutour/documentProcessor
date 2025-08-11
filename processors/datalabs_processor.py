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
from utils.pdf_chunker import PDFChunker, ChunkProcessor


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
        self.max_file_size_mb = config.get('max_file_size_mb', 80) if config else 80  # 80MB max per chunk
        
        if not self.api_key:
            raise ValueError("DataLabs API key not found in settings")
        
        # Initialize PDF chunker
        self.pdf_chunker = PDFChunker(max_chunk_size_mb=self.max_file_size_mb)
        
        # Initialize circuit breaker for DataLabs API
        circuit_config = CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=60,
            success_threshold=2,
            timeout=600,  # 10 minutes to handle large files
            expected_exceptions=(requests.RequestException, requests.HTTPError, requests.Timeout, requests.ConnectionError)
        )
        self.circuit_breaker = circuit_breaker_manager.get_circuit_breaker('datalabs_api', circuit_config)
    
    def supports_document_type(self, document_type: str) -> bool:
        """Check if processor supports given document type."""
        return document_type in self.SUPPORTED_TYPES
    
    def process(self, document: Document, content: bytes) -> Dict[str, Any]:
        """
        Process document using DataLabs API for markdown conversion.
        Large files will be automatically chunked.
        
        Args:
            document: Document metadata
            content: Raw document content as bytes
            
        Returns:
            Dictionary with markdown formatted content and page identifiers
        """
        if not self.validate_content(content):
            raise ValueError("Invalid document content")
        
        try:
            # DEBUG: Check actual PDF page count before processing
            self._debug_pdf_pages(content, document.filename)
            
            # Check if file needs chunking
            file_size_mb = len(content) / (1024 * 1024)
            
            if file_size_mb > self.max_file_size_mb:
                logger.info(f"Large file detected ({file_size_mb:.1f}MB), chunking document: {document.filename}")
                return self._process_chunked_document(document, content)
            else:
                logger.info(f"Processing document normally ({file_size_mb:.1f}MB): {document.filename}")
                return self._process_single_document(document, content)
                
        except Exception as e:
            logger.error(f"Error processing document {document.filename} with DataLabs: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Exception details: {repr(e)}")
            
            return {
                'pages': [],
                'document_metadata': {
                    'filename': document.filename,
                    'document_type': document.document_type,
                    'processing_method': 'datalabs_api'
                },
                'processing_info': {
                    'success': False,
                    'error': str(e),
                    'processor': 'DataLabsProcessor'
                }
            }
    
    def _process_single_document(self, document: Document, content: bytes) -> Dict[str, Any]:
        """Process a single document that doesn't need chunking"""
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
    
    def _process_chunked_document(self, document: Document, content: bytes) -> Dict[str, Any]:
        """Process a large document by chunking it into smaller pieces"""
        try:
            # Create chunks
            chunks = self.pdf_chunker.chunk_pdf(content, document.filename)
            logger.info(f"Created {len(chunks)} chunks for document: {document.filename}")
            
            # DEBUG: Log chunk page ranges
            for i, chunk in enumerate(chunks):
                logger.info(f"  Chunk {i+1}: pages {chunk.start_page}-{chunk.end_page} ({chunk.size_bytes} bytes)")
            
            # Process each chunk
            chunk_results = []
            for i, chunk in enumerate(chunks):
                logger.info(f"Processing chunk {i+1}/{len(chunks)} for {document.filename} (pages {chunk.start_page}-{chunk.end_page})")
                
                # Try processing each chunk with retry logic
                max_chunk_retries = 2
                chunk_result = None
                
                for retry_attempt in range(max_chunk_retries):
                    try:
                        # Create a temporary document object for the chunk
                        chunk_doc = Document(
                            filename=chunk.chunk_id,
                            s3_key=document.s3_key,
                            file_size=chunk.size_bytes,
                            last_modified=document.last_modified,
                            etag=f"{document.etag}_chunk_{i+1}"
                        )
                        chunk_doc.document_type = document.document_type
                        
                        # Process the chunk
                        chunk_result = self._process_single_document(chunk_doc, chunk.content)
                        
                        # If successful, break out of retry loop
                        if chunk_result.get('processing_info', {}).get('success', False):
                            break
                        else:
                            # If processing failed but didn't throw exception, log and potentially retry
                            error_msg = chunk_result.get('processing_info', {}).get('error', 'Unknown error')
                            logger.warning(f"Chunk {i+1} processing failed (attempt {retry_attempt + 1}): {error_msg}")
                            if retry_attempt < max_chunk_retries - 1:
                                time.sleep(2 ** retry_attempt)  # Exponential backoff
                                continue
                            else:
                                # Use the failed result
                                break
                                
                    except Exception as e:
                        logger.error(f"Error processing chunk {i+1} (attempt {retry_attempt + 1}): {str(e)}")
                        if retry_attempt < max_chunk_retries - 1:
                            time.sleep(2 ** retry_attempt)  # Exponential backoff
                            continue
                        else:
                            # Re-raise the exception to be caught by outer try-except
                            raise
                
                if chunk_result:
                    # Add chunk metadata to preserve page identifiers
                    chunk_result['chunk_info'] = {
                        'chunk_id': chunk.chunk_id,
                        'start_page': chunk.start_page,
                        'end_page': chunk.end_page,
                        'size_bytes': chunk.size_bytes
                    }
                    
                    chunk_results.append(chunk_result)
                else:
                    # This should not happen, but handle gracefully
                    logger.error(f"No result obtained for chunk {i+1}")
                    chunk_results.append({
                        'pages': [],
                        'document_metadata': {
                            'filename': chunk.chunk_id,
                            'document_type': document.document_type,
                            'processing_method': 'datalabs_api_chunk'
                        },
                        'processing_info': {
                            'success': False,
                            'error': 'No result obtained after retries',
                            'error_type': 'NoResultError',
                            'processor': 'DataLabsProcessor',
                            'chunk_details': {
                                'chunk_number': i+1,
                                'total_chunks': len(chunks),
                                'page_range': f"{chunk.start_page}-{chunk.end_page}",
                                'size_bytes': chunk.size_bytes
                            }
                        },
                        'chunk_info': {
                            'chunk_id': chunk.chunk_id,
                            'start_page': chunk.start_page,
                            'end_page': chunk.end_page,
                            'size_bytes': chunk.size_bytes
                        }
                    })
            
            # Get expected page count from original PDF  
            expected_page_count = None
            try:
                import PyPDF2
                import io
                pdf_reader = PyPDF2.PdfReader(io.BytesIO(content))
                expected_page_count = len(pdf_reader.pages)
                logger.info(f"Expected page count for validation: {expected_page_count}")
            except Exception as e:
                logger.warning(f"Could not determine expected page count: {e}")
            
            # Combine chunk results while preserving page identifiers
            combined_result = ChunkProcessor.combine_chunk_results(chunk_results, document.filename, expected_page_count)
            
            logger.info(f"Successfully processed chunked document {document.filename}: {len(chunk_results)} chunks, {combined_result['processing_info']['successful_chunks']} successful")
            
            return combined_result
            
        except Exception as e:
            logger.error(f"Error processing chunked document {document.filename}: {str(e)}")
            
            return {
                'pages': [],
                'document_metadata': {
                    'filename': document.filename,
                    'document_type': document.document_type,
                    'processing_method': 'datalabs_api_chunked'
                },
                'processing_info': {
                    'success': False,
                    'error': f"Chunking failed: {str(e)}",
                    'processor': 'DataLabsProcessor'
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
                timeout=600  # 10 minutes for large file uploads
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
                timeout=60  # 1 minute for status checks
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
                logger.error(f"Exception type: {type(e).__name__}")
                
                # If circuit breaker is open, stop polling
                if 'circuit breaker' in str(e).lower():
                    logger.error("Circuit breaker is open, stopping polling")
                    raise Exception(f"DataLabs API circuit breaker is open: {str(e)}")
                
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
        
        # DEBUG: Log DataLabs response details
        logger.info(f"DataLabs result for {document.filename}:")
        logger.info(f"  - Reported page_count: {page_count}")
        logger.info(f"  - Content length: {len(markdown_content)} characters")
        logger.info(f"  - Is paginated: {self._is_paginated(markdown_content)}")
        
        # If pagination is enabled, split by page delimiters
        if self._is_paginated(markdown_content):
            page_sections = self._split_paginated_content(markdown_content)
            logger.info(f"  - Split into {len(page_sections)} page sections")
            
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
        # Look for proper page delimiters first
        import re
        
        # DataLabs should use specific page markers when paginate=True
        # Look for patterns like "Page 1", "PAGE 1", or page break markers
        page_patterns = [
            r'\n---+\s*Page\s+\d+\s*---+\n',  # --- Page 1 ---
            r'\n={3,}\s*Page\s+\d+\s*={3,}\n',  # === Page 1 ===
            r'\n\*{3,}\s*Page\s+\d+\s*\*{3,}\n',  # *** Page 1 ***
            r'\n\f',  # Form feed character (page break)
            r'\n\s*Page\s+\d+\s*\n',  # Standalone "Page N"
        ]
        
        for pattern in page_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                # Split by this pattern and clean up
                pages = re.split(pattern, content, flags=re.IGNORECASE)
                # Remove empty pages and strip whitespace
                pages = [page.strip() for page in pages if page.strip()]
                logger.info(f"Split content using pattern '{pattern}' into {len(pages)} pages")
                return pages
        
        # If no specific page delimiters found, check for section headers but be more conservative
        # Only split if there are a reasonable number of sections (not too many)
        if '\n\n# ' in content:
            sections = content.split('\n\n# ')
            if len(sections) <= 50:  # Reasonable limit to prevent page explosion
                pages = [sections[0]]  # First section
                pages.extend([f'# {section}' for section in sections[1:]])
                cleaned_pages = [page.strip() for page in pages if page.strip()]
                logger.info(f"Split content by headers into {len(cleaned_pages)} sections")
                return cleaned_pages
            else:
                logger.warning(f"Too many header sections ({len(sections)}), treating as single page")
        
        # Check for simple --- delimiters but be conservative
        if '---' in content:
            potential_pages = content.split('---')
            # Only split if reasonable number of pages
            if len(potential_pages) <= 20 and all(len(p.strip()) > 50 for p in potential_pages if p.strip()):
                pages = [page.strip() for page in potential_pages if page.strip()]
                logger.info(f"Split content by --- delimiters into {len(pages)} pages")
                return pages
            else:
                logger.warning(f"--- split would create {len(potential_pages)} pages, treating as single page")
        
        # Fallback: return as single page
        logger.info("No reliable page delimiters found, treating as single page")
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
    
    def _debug_pdf_pages(self, pdf_content: bytes, filename: str) -> None:
        """Debug function to check actual PDF page count"""
        try:
            import PyPDF2
            import io
            
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            actual_page_count = len(pdf_reader.pages)
            
            logger.info(f"DEBUG: PDF {filename} actual page count: {actual_page_count}")
            
            # Check first few pages for content
            for i in range(min(3, actual_page_count)):
                try:
                    page = pdf_reader.pages[i]
                    text = page.extract_text()
                    logger.debug(f"  Page {i+1}: {len(text)} characters")
                    if len(text) < 50:
                        logger.warning(f"  WARNING: Page {i+1} has very little content ({len(text)} chars)")
                except Exception as e:
                    logger.warning(f"  Could not extract text from page {i+1}: {e}")
                    
        except Exception as e:
            logger.warning(f"Could not debug PDF pages for {filename}: {e}")