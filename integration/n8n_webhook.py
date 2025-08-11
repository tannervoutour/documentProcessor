"""
n8n webhook integration for document processing notifications.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import requests
from models.document import Document
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.content_utils import (
    extract_file_type,
    consolidate_full_text,
    extract_first_n_pages_text,
    determine_content_format,
    calculate_content_statistics,
    validate_processing_result,
    clean_content_for_webhook,
    format_machine_names,
    get_processing_timestamp
)
from core.circuit_breaker import CircuitBreakerConfig, circuit_breaker_manager


logger = logging.getLogger(__name__)


class N8nPayloadBuilder:
    """Builds n8n webhook payload from document processing results."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def build_webhook_payload(
        self,
        document: Document,
        processing_result: Dict,
        metadata: Dict
    ) -> Dict[str, Any]:
        """
        Build complete n8n webhook payload from document and processing results.
        
        Args:
            document: Document object with metadata
            processing_result: Results from document processing
            metadata: Additional metadata from UI
            
        Returns:
            Dictionary formatted for n8n webhook
        """
        try:
            # Validate processing result
            validation_errors = validate_processing_result(processing_result)
            if validation_errors:
                raise ValueError(f"Invalid processing result: {validation_errors}")
            
            pages = processing_result.get('pages', [])
            document_metadata = processing_result.get('document_metadata', {})
            processing_info = processing_result.get('processing_info', {})
            
            # 1. File name
            filename = document.filename
            
            # 2. File id (S3 identifier)
            file_id = document.file_id
            s3_key = document.s3_key
            
            # 3. Document type (manually specified in UI)
            document_type = metadata.get('document_type', document.document_type)
            
            # 4. Content format determination
            content_format = determine_content_format(processing_result)
            
            # 5. Machine names (array from UI)
            machine_names = format_machine_names(
                metadata.get('machine_names', document.machine_names or [])
            )
            
            # 6. File type (extracted from filename)
            file_type = extract_file_type(filename)
            
            # 7. Full document text with page identifiers
            full_text = consolidate_full_text(pages)
            full_text_cleaned = clean_content_for_webhook(full_text)
            
            # 8. First 10 pages as separate parameter
            first_10_pages_text = extract_first_n_pages_text(pages, 10)
            first_10_pages_cleaned = clean_content_for_webhook(first_10_pages_text)
            
            # Additional metadata
            content_stats = calculate_content_statistics(pages)
            
            # Generate S3 document URL
            s3_document_url = None
            try:
                # Import here to avoid circular imports
                from config.settings import S3_BUCKET_NAME, AWS_REGION
                
                # Generate standard S3 URL format
                if AWS_REGION and AWS_REGION != 'us-east-1':
                    s3_document_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{document.s3_key}"
                else:
                    s3_document_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{document.s3_key}"
                    
            except Exception as e:
                self.logger.warning(f"Could not generate S3 URL for {document.s3_key}: {e}")
                s3_document_url = f"https://s3.amazonaws.com/{document.s3_key}"
            
            # Document type boolean flags
            diagram = (document_type == 'diagram')
            sparepartslist = (document_type == 'sparepartslist')
            
            # Build the payload
            payload = {
                # Required parameters
                "filename": filename,
                "file_id": file_id,
                "document_type": document_type,
                "content_format": content_format,
                "machines": machine_names,
                "file_type": file_type,
                "s3_document_url": s3_document_url,
                "diagram": diagram,
                "sparepartslist": sparepartslist,
                "full_text": full_text_cleaned,
                "first_10_pages": first_10_pages_cleaned,
                
                # Additional metadata
                "document_metadata": {
                    "s3_key": s3_key,
                    "s3_document_url": s3_document_url,
                    "file_size": document.file_size,
                    "last_modified": document.last_modified.isoformat() if hasattr(document.last_modified, 'isoformat') else str(document.last_modified),
                    "processing_method": document_metadata.get('processing_method', 'unknown'),
                    "processor_used": processing_info.get('processor', 'unknown'),
                    "processing_timestamp": get_processing_timestamp(),
                    "processing_success": processing_info.get('success', False),
                    "processing_time": processing_info.get('processing_time')
                },
                
                # Content statistics
                "content_statistics": content_stats,
                
                # Processing details
                "processing_info": {
                    "pages_processed": processing_info.get('pages_processed', 0),
                    "pages_with_content": processing_info.get('pages_with_content', 0),
                    "processor": processing_info.get('processor', 'unknown'),
                    "success": processing_info.get('success', False),
                    "error": processing_info.get('error'),
                    "datalabs_job_id": processing_info.get('datalabs_job_id')
                }
            }
            
            # Add format-specific metadata
            if content_format == 'markdown':
                payload["markdown_metadata"] = {
                    "images_detected": content_stats.get('total_images', 0),
                    "tables_detected": content_stats.get('total_tables', 0),
                    "has_rich_formatting": True
                }
            
            self.logger.info(f"Built webhook payload for document: {filename}")
            self.logger.debug(f"Payload size: {len(json.dumps(payload))} characters")
            
            return payload
            
        except Exception as e:
            self.logger.error(f"Error building webhook payload: {str(e)}")
            raise
    
    def build_error_payload(
        self,
        document: Document,
        metadata: Dict,
        error_message: str
    ) -> Dict[str, Any]:
        """
        Build error payload for failed document processing.
        
        Args:
            document: Document object
            metadata: Metadata from UI
            error_message: Error message
            
        Returns:
            Error payload for n8n webhook
        """
        return {
            "filename": document.filename,
            "file_id": document.file_id,
            "document_type": metadata.get('document_type', document.document_type),
            "machines": format_machine_names(
                metadata.get('machine_names', document.machine_names or [])
            ),
            "file_type": extract_file_type(document.filename),
            "processing_error": True,
            "error_message": error_message,
            "error_timestamp": get_processing_timestamp(),
            "document_metadata": {
                "s3_key": document.s3_key,
                "file_size": document.file_size,
                "last_modified": document.last_modified.isoformat() if hasattr(document.last_modified, 'isoformat') else str(document.last_modified)
            }
        }
    
    def validate_payload(self, payload: Dict) -> List[str]:
        """
        Validate webhook payload structure.
        
        Args:
            payload: Webhook payload to validate
            
        Returns:
            List of validation errors
        """
        errors = []
        
        # Required fields
        required_fields = [
            'filename', 'file_id', 'document_type', 'content_format',
            'machines', 'file_type', 's3_document_url', 'diagram', 'sparepartslist', 
            'full_text', 'first_10_pages'
        ]
        
        for field in required_fields:
            if field not in payload:
                errors.append(f"Missing required field: {field}")
            elif payload[field] is None:
                errors.append(f"Field cannot be null: {field}")
        
        # Validate machines is array
        if 'machines' in payload and not isinstance(payload['machines'], list):
            errors.append("machines must be an array")
        
        # Validate content is not empty
        if 'full_text' in payload and not payload['full_text'].strip():
            errors.append("full_text cannot be empty")
        
        return errors


class N8nWebhookClient:
    """Client for sending webhook notifications to n8n."""
    
    def __init__(self, webhook_url: str, api_key: str = None):
        """
        Initialize webhook client.
        
        Args:
            webhook_url: n8n webhook URL
            api_key: Optional API key for authentication
        """
        self.webhook_url = webhook_url
        self.api_key = api_key
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'DocumentProcessor/1.0'
        })
        
        # Add API key if provided
        if api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {api_key}'
            })
        
        # Initialize circuit breaker for n8n webhooks
        circuit_config = CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout=120,
            success_threshold=3,
            timeout=120,
            expected_exceptions=(requests.RequestException, requests.HTTPError, requests.Timeout)
        )
        self.circuit_breaker = circuit_breaker_manager.get_circuit_breaker('n8n_webhook', circuit_config)
    
    def send_webhook(self, payload: Dict, timeout: int = 120, max_retries: int = 3) -> Dict:
        """
        Send webhook notification to n8n with circuit breaker protection and retry logic.
        
        Args:
            payload: Webhook payload
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            
        Returns:
            Response information
        """
        import time
        
        def _send_request():
            self.logger.debug(f"Payload keys: {list(payload.keys())}")
            
            response = self.session.post(
                self.webhook_url,
                json=payload,
                timeout=timeout
            )
            
            response.raise_for_status()
            
            result = {
                'success': True,
                'status_code': response.status_code,
                'response_data': response.json() if response.content else None,
                'response_headers': dict(response.headers),
                'request_id': response.headers.get('X-Request-ID'),
                'timestamp': get_processing_timestamp()
            }
            
            return result
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Sending webhook to n8n (attempt {attempt + 1}/{max_retries}): {self.webhook_url}")
                
                # Use circuit breaker for webhook request
                result = self.circuit_breaker.call(_send_request)
                result['attempts'] = attempt + 1
                
                self.logger.info(f"Webhook sent successfully: {result['status_code']} (attempt {attempt + 1})")
                return result
                
            except Exception as e:
                error_msg = f"Webhook error: {str(e)} (attempt {attempt + 1}/{max_retries})"
                self.logger.error(error_msg)
                
                if attempt < max_retries - 1:
                    # Wait before retrying (exponential backoff)
                    wait_time = 2 ** attempt
                    self.logger.info(f"Retrying webhook in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    return {
                        'success': False,
                        'error': f"Webhook request failed after {max_retries} attempts: {str(e)}",
                        'error_type': type(e).__name__,
                        'timestamp': get_processing_timestamp(),
                        'attempts': max_retries
                    }
        
        # This should never be reached due to the loop and returns above
        return {
            'success': False,
            'error': 'Webhook request failed after all retries',
            'error_type': 'max_retries_exceeded',
            'timestamp': get_processing_timestamp(),
            'attempts': max_retries
        }
    
    def send_document_processed(
        self,
        document: Document,
        processing_result: Dict,
        metadata: Dict
    ) -> Dict:
        """
        Send document processed notification.
        
        Args:
            document: Document object
            processing_result: Processing results
            metadata: Document metadata
            
        Returns:
            Webhook response
        """
        try:
            payload_builder = N8nPayloadBuilder()
            
            # Check if processing was successful
            processing_info = processing_result.get('processing_info', {})
            if not processing_info.get('success', False):
                # Send error payload for failed processing
                error_message = processing_info.get('error', 'Processing failed')
                self.logger.info(f"Processing failed for {document.filename}, sending error payload: {error_message}")
                return self.send_processing_error(document, metadata, error_message)
            
            payload = payload_builder.build_webhook_payload(
                document, processing_result, metadata
            )
            
            # Validate payload
            validation_errors = payload_builder.validate_payload(payload)
            if validation_errors:
                raise ValueError(f"Invalid payload: {validation_errors}")
            
            return self.send_webhook(payload)
            
        except Exception as e:
            self.logger.error(f"Error sending document processed webhook: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'error_type': 'payload_error',
                'timestamp': get_processing_timestamp()
            }
    
    def send_processing_error(
        self,
        document: Document,
        metadata: Dict,
        error_message: str
    ) -> Dict:
        """
        Send processing error notification.
        
        Args:
            document: Document object
            metadata: Document metadata
            error_message: Error message
            
        Returns:
            Webhook response
        """
        try:
            payload_builder = N8nPayloadBuilder()
            payload = payload_builder.build_error_payload(
                document, metadata, error_message
            )
            
            return self.send_webhook(payload)
            
        except Exception as e:
            self.logger.error(f"Error sending processing error webhook: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'error_type': 'payload_error',
                'timestamp': get_processing_timestamp()
            }
    
    def test_webhook_connection(self) -> Dict:
        """
        Test webhook connection with a simple ping.
        
        Returns:
            Connection test result
        """
        test_payload = {
            'test': True,
            'message': 'Document processor webhook test',
            'timestamp': get_processing_timestamp()
        }
        
        return self.send_webhook(test_payload)
    
    def get_webhook_info(self) -> Dict:
        """
        Get webhook configuration info.
        
        Returns:
            Webhook configuration
        """
        return {
            'webhook_url': self.webhook_url,
            'has_api_key': bool(self.api_key),
            'headers': dict(self.session.headers)
        }
    
    def get_circuit_breaker_stats(self) -> Dict[str, Any]:
        """
        Get circuit breaker statistics for n8n webhooks.
        
        Returns:
            Circuit breaker statistics
        """
        return self.circuit_breaker.get_stats()