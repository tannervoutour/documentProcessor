"""
Factory for creating appropriate document processors based on document type.
"""

from typing import Dict, Any, Optional, List
import logging
from .base_processor import BaseProcessor
from .pymupdf_processor import PyMuPDFProcessor
from .datalabs_processor import DataLabsProcessor


logger = logging.getLogger(__name__)


class ProcessorFactory:
    """Factory for creating document processors based on processing method."""
    
    # Mapping of processing methods to their processors
    PROCESSOR_MAPPING = {
        'markdown': DataLabsProcessor,  # Rich markdown formatting with DataLabs
        'plain_text': PyMuPDFProcessor,  # Simple text extraction with PyMuPDF
    }
    
    # Legacy mapping for backward compatibility (deprecated)
    LEGACY_DOCUMENT_TYPE_MAPPING = {
        'manual': DataLabsProcessor,
        'diagram': PyMuPDFProcessor,
        'sparepartslist': DataLabsProcessor,
        'spreadsheet': DataLabsProcessor,
        'plain_document': PyMuPDFProcessor,
    }
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the processor factory.
        
        Args:
            config: Configuration dictionary for processors
        """
        self.config = config or {}
        self._processor_instances = {}
    
    def get_processor_by_method(self, processing_method: str) -> BaseProcessor:
        """
        Get the appropriate processor for a processing method.
        
        Args:
            processing_method: Method of processing ('markdown' or 'plain_text')
            
        Returns:
            Processor instance for the processing method
            
        Raises:
            ValueError: If processing method is not supported
        """
        if processing_method not in self.PROCESSOR_MAPPING:
            raise ValueError(f"Unsupported processing method: {processing_method}. Supported: {list(self.PROCESSOR_MAPPING.keys())}")
        
        processor_class = self.PROCESSOR_MAPPING[processing_method]
        
        # Return cached instance if available
        if processor_class in self._processor_instances:
            return self._processor_instances[processor_class]
        
        # Create new instance
        try:
            processor_config = self.config.get(processor_class.__name__, {})
            processor = processor_class(processor_config)
            self._processor_instances[processor_class] = processor
            
            logger.info(f"Created processor {processor_class.__name__} for processing method: {processing_method}")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create processor for processing method {processing_method}: {str(e)}")
            raise
    
    def get_processor(self, document_type: str) -> BaseProcessor:
        """
        DEPRECATED: Get processor by document type. Use get_processor_by_method instead.
        
        Args:
            document_type: Type of document to process
            
        Returns:
            Processor instance for the document type
            
        Raises:
            ValueError: If document type is not supported
        """
        logger.warning(f"Using deprecated get_processor method with document_type: {document_type}. Use get_processor_by_method instead.")
        
        if document_type not in self.LEGACY_DOCUMENT_TYPE_MAPPING:
            raise ValueError(f"Unsupported document type: {document_type}")
        
        processor_class = self.LEGACY_DOCUMENT_TYPE_MAPPING[document_type]
        
        # Return cached instance if available
        if processor_class in self._processor_instances:
            return self._processor_instances[processor_class]
        
        # Create new instance
        try:
            processor_config = self.config.get(processor_class.__name__, {})
            processor = processor_class(processor_config)
            self._processor_instances[processor_class] = processor
            
            logger.info(f"Created processor {processor_class.__name__} for document type: {document_type} (legacy)")
            return processor
            
        except Exception as e:
            logger.error(f"Failed to create processor for document type {document_type}: {str(e)}")
            raise
    
    def get_available_processors(self) -> Dict[str, str]:
        """
        Get mapping of processing methods to processor names.
        
        Returns:
            Dictionary mapping processing methods to processor class names
        """
        return {
            method: processor_class.__name__ 
            for method, processor_class in self.PROCESSOR_MAPPING.items()
        }
    
    def get_available_processing_methods(self) -> List[str]:
        """
        Get list of available processing methods.
        
        Returns:
            List of supported processing methods
        """
        return list(self.PROCESSOR_MAPPING.keys())
    
    def supports_processing_method(self, processing_method: str) -> bool:
        """
        Check if a processing method is supported.
        
        Args:
            processing_method: Processing method to check
            
        Returns:
            True if processing method is supported
        """
        return processing_method in self.PROCESSOR_MAPPING
    
    def supports_document_type(self, document_type: str) -> bool:
        """
        DEPRECATED: Check if a document type is supported. Use supports_processing_method instead.
        
        Args:
            document_type: Type of document to check
            
        Returns:
            True if document type is supported
        """
        return document_type in self.LEGACY_DOCUMENT_TYPE_MAPPING
    
    def get_processor_for_document(self, document, processing_method: str = None) -> BaseProcessor:
        """
        Get processor for a specific document.
        
        Args:
            document: Document object
            processing_method: Processing method to use ('markdown' or 'plain_text')
            
        Returns:
            Appropriate processor instance
        """
        if processing_method:
            return self.get_processor_by_method(processing_method)
        else:
            # Fallback to legacy method (deprecated)
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