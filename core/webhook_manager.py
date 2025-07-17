"""
Webhook management utilities for n8n integration.
"""

import logging
from typing import Dict, List, Any, Optional
from integration.n8n_webhook import N8nWebhookClient
from models.document import Document
from config.settings import N8N_WEBHOOK_URL, N8N_API_KEY


logger = logging.getLogger(__name__)


class WebhookManager:
    """Manager for webhook operations and configuration."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.webhook_client = None
        self.webhook_enabled = False
        
        # Initialize webhook client if configured
        self._initialize_webhook_client()
    
    def _initialize_webhook_client(self) -> None:
        """Initialize the webhook client if configuration is available."""
        if N8N_WEBHOOK_URL:
            try:
                self.webhook_client = N8nWebhookClient(N8N_WEBHOOK_URL, N8N_API_KEY)
                self.webhook_enabled = True
                self.logger.info("Webhook client initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize webhook client: {e}")
                self.webhook_enabled = False
        else:
            self.logger.warning("Webhook URL not configured - webhooks disabled")
            self.webhook_enabled = False
    
    def is_webhook_enabled(self) -> bool:
        """Check if webhook is enabled and configured."""
        return self.webhook_enabled and self.webhook_client is not None
    
    def test_webhook_connection(self) -> Dict:
        """
        Test webhook connection.
        
        Returns:
            Test result dictionary
        """
        if not self.is_webhook_enabled():
            return {
                'success': False,
                'error': 'Webhook not configured or disabled',
                'webhook_enabled': False
            }
        
        try:
            result = self.webhook_client.test_webhook_connection()
            result['webhook_enabled'] = True
            return result
        except Exception as e:
            self.logger.error(f"Webhook connection test failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'webhook_enabled': True
            }
    
    def send_document_notification(
        self,
        document: Document,
        processing_result: Dict,
        metadata: Dict
    ) -> Dict:
        """
        Send document processing notification.
        
        Args:
            document: Document object
            processing_result: Processing results
            metadata: Document metadata
            
        Returns:
            Notification result
        """
        if not self.is_webhook_enabled():
            return {
                'success': False,
                'error': 'Webhook not enabled',
                'sent': False
            }
        
        try:
            result = self.webhook_client.send_document_processed(
                document, processing_result, metadata
            )
            result['sent'] = True
            return result
        except Exception as e:
            self.logger.error(f"Failed to send document notification: {e}")
            return {
                'success': False,
                'error': str(e),
                'sent': False
            }
    
    def send_error_notification(
        self,
        document: Document,
        metadata: Dict,
        error_message: str
    ) -> Dict:
        """
        Send error notification.
        
        Args:
            document: Document object
            metadata: Document metadata
            error_message: Error message
            
        Returns:
            Notification result
        """
        if not self.is_webhook_enabled():
            return {
                'success': False,
                'error': 'Webhook not enabled',
                'sent': False
            }
        
        try:
            result = self.webhook_client.send_processing_error(
                document, metadata, error_message
            )
            result['sent'] = True
            return result
        except Exception as e:
            self.logger.error(f"Failed to send error notification: {e}")
            return {
                'success': False,
                'error': str(e),
                'sent': False
            }
    
    def send_batch_notification(self, batch_results: Dict) -> Dict:
        """
        Send batch processing notification.
        
        Args:
            batch_results: Batch processing results
            
        Returns:
            Notification result
        """
        if not self.is_webhook_enabled():
            return {
                'success': False,
                'error': 'Webhook not enabled',
                'sent': False
            }
        
        try:
            from utils.content_utils import get_processing_timestamp
            
            batch_payload = {
                'batch_processing_complete': True,
                'total_processed': batch_results.get('total_processed', 0),
                'successful_count': len(batch_results.get('successful', [])),
                'failed_count': len(batch_results.get('failed', [])),
                'successful_documents': batch_results.get('successful', []),
                'failed_documents': batch_results.get('failed', []),
                'timestamp': get_processing_timestamp()
            }
            
            result = self.webhook_client.send_webhook(batch_payload)
            result['sent'] = True
            return result
        except Exception as e:
            self.logger.error(f"Failed to send batch notification: {e}")
            return {
                'success': False,
                'error': str(e),
                'sent': False
            }
    
    def get_webhook_info(self) -> Dict:
        """
        Get webhook configuration information.
        
        Returns:
            Webhook configuration details
        """
        if not self.is_webhook_enabled():
            return {
                'enabled': False,
                'configured': bool(N8N_WEBHOOK_URL),
                'webhook_url': N8N_WEBHOOK_URL,
                'has_api_key': bool(N8N_API_KEY)
            }
        
        try:
            client_info = self.webhook_client.get_webhook_info()
            return {
                'enabled': True,
                'configured': True,
                **client_info
            }
        except Exception as e:
            self.logger.error(f"Failed to get webhook info: {e}")
            return {
                'enabled': False,
                'configured': True,
                'error': str(e)
            }
    
    def get_webhook_statistics(self) -> Dict:
        """
        Get webhook usage statistics.
        
        Returns:
            Webhook statistics
        """
        # This would typically be implemented with a database or cache
        # For now, return basic info
        return {
            'webhook_enabled': self.is_webhook_enabled(),
            'webhook_url': N8N_WEBHOOK_URL,
            'configuration_complete': bool(N8N_WEBHOOK_URL and N8N_API_KEY)
        }
    
    def validate_webhook_configuration(self) -> Dict:
        """
        Validate webhook configuration.
        
        Returns:
            Validation result
        """
        errors = []
        warnings = []
        
        if not N8N_WEBHOOK_URL:
            errors.append("N8N_WEBHOOK_URL not configured")
        elif not N8N_WEBHOOK_URL.startswith(('http://', 'https://')):
            errors.append("N8N_WEBHOOK_URL must start with http:// or https://")
        
        if not N8N_API_KEY:
            warnings.append("N8N_API_KEY not configured - webhook will work but without authentication")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'webhook_enabled': self.is_webhook_enabled()
        }


# Global webhook manager instance
webhook_manager = WebhookManager()