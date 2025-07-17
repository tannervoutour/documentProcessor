"""
Base processor class for document processing.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List
from models.document import Document


class BaseProcessor(ABC):
    """Base class for all document processors."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
    
    @abstractmethod
    def process(self, document: Document, content: bytes) -> Dict[str, Any]:
        """
        Process a document and return structured data.
        
        Args:
            document: Document metadata
            content: Raw document content as bytes
            
        Returns:
            Dictionary containing processed data with structure:
            {
                'pages': [
                    {
                        'page_number': int,
                        'page_id': str,
                        'content': str,
                        'metadata': dict
                    }
                ],
                'document_metadata': dict,
                'processing_info': dict
            }
        """
        pass
    
    @abstractmethod
    def supports_document_type(self, document_type: str) -> bool:
        """
        Check if processor supports given document type.
        
        Args:
            document_type: Type of document (manual, diagram, etc.)
            
        Returns:
            True if processor supports the document type
        """
        pass
    
    def validate_content(self, content: bytes) -> bool:
        """
        Validate that content can be processed.
        
        Args:
            content: Raw document content
            
        Returns:
            True if content is valid for processing
        """
        return content is not None and len(content) > 0