"""
Factory for creating appropriate document processors based on document type.
"""

from typing import Dict, Any, Optional
import logging
from .base_processor import BaseProcessor
from .pymupdf_processor import PyMuPDFProcessor
from .datalabs_processor import DataLabsProcessor


logger = logging.getLogger(__name__)


class ProcessorFactory:
    """Factory for creating document processors based on document type."""
    
    # Mapping of document types to their processors
    PROCESSOR_MAPPING = {
        'manual': DataLabsProcessor,
        'diagram': PyMuPDFProcessor,
        'sparepartslist': DataLabsProcessor,  # Changed to DataLabs for markdown processing
        'spreadsheet': DataLabsProcessor,  # Can be processed by DataLabs or PyMuPDF
        'plain_document': PyMuPDFProcessor,  # Simple text extraction for small documents
    }
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the processor factory.
        
        Args:
            config: Configuration dictionary for processors
        """
        self.config = config or {}
        self._processor_instances = {}
    
    def get_processor(self, document_type: str) -> BaseProcessor:
        """
        Get the appropriate processor for a document type.
        
        Args:
            document_type: Type of document to process
            
        Returns:
            Processor instance for the document type
            
        Raises:
            ValueError: If document type is not supported
        """
        if document_type not in self.PROCESSOR_MAPPING:
            raise ValueError(f"Unsupported document type: {document_type}")
        
        processor_class = self.PROCESSOR_MAPPING[document_type]
        
        # Return cached instance if available
        if processor_class in self._processor_instances:
            return self._processor_instances[processor_class]
        
        # Create new instance
        try:
            processor_config = self.config.get(processor_class.__name__, {})
            processor = processor_class(processor_config)
            self._processor_instances[processor_class] = processor
            
            logger.info(f"Created processor {processor_class.__name__} for document type: {document_type}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create processor for document type {document_type}: {str(e)}")
            raise
    
    def get_available_processors(self) -> Dict[str, str]:
        """
        Get mapping of document types to processor names.
        
        Returns:
            Dictionary mapping document types to processor class names
        """
        return {
            doc_type: processor_class.__name__ 
            for doc_type, processor_class in self.PROCESSOR_MAPPING.items()
        }
    
    def supports_document_type(self, document_type: str) -> bool:
        """
        Check if a document type is supported.
        
        Args:
            document_type: Type of document to check
            
        Returns:
            True if document type is supported
        """
        return document_type in self.PROCESSOR_MAPPING
    
    def get_processor_for_document(self, document) -> BaseProcessor:
        """
        Get processor for a specific document based on its type.
        
        Args:
            document: Document object with document_type attribute
            
        Returns:
            Appropriate processor instance
        """
        return self.get_processor(document.document_type)
    
    def configure_processor(self, processor_name: str, config: Dict[str, Any]) -> None:
        """
        Configure a specific processor.
        
        Args:
            processor_name: Name of the processor class
            config: Configuration dictionary
        """
        self.config[processor_name] = config
        
        # Clear cached instance to force recreation with new config
        processor_class = None
        for doc_type, cls in self.PROCESSOR_MAPPING.items():
            if cls.__name__ == processor_name:
                processor_class = cls
                break
        
        if processor_class and processor_class in self._processor_instances:
            del self._processor_instances[processor_class]
            logger.info(f"Cleared cached instance for {processor_name} due to configuration change")
    
    def clear_cache(self) -> None:
        """Clear all cached processor instances."""
        self._processor_instances.clear()
        logger.info("Cleared all cached processor instances")
    
    @classmethod
    def get_default_config(cls) -> Dict[str, Any]:
        """
        Get default configuration for all processors.
        
        Returns:
            Default configuration dictionary
        """
        return {
            'PyMuPDFProcessor': {
                'extract_images': False
            },
            'DataLabsProcessor': {
                'base_url': 'https://api.datalabs.com',
                'timeout': 300,
                'poll_interval': 10
            }
        }
    
    def validate_processors(self) -> Dict[str, bool]:
        """
        Validate that all processors can be instantiated.
        
        Returns:
            Dictionary mapping processor names to validation status
        """
        results = {}
        
        for doc_type, processor_class in self.PROCESSOR_MAPPING.items():
            try:
                processor_config = self.config.get(processor_class.__name__, {})
                processor = processor_class(processor_config)
                results[processor_class.__name__] = True
                logger.info(f"Processor {processor_class.__name__} validated successfully")
                
            except Exception as e:
                results[processor_class.__name__] = False
                logger.error(f"Processor {processor_class.__name__} validation failed: {str(e)}")
        
        return results


# Global factory instance
processor_factory = ProcessorFactory()