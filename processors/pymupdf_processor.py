"""
PyMuPDF processor for diagrams and technical drawings.
Simple text extraction with page-level identification.
"""

import pymupdf
from typing import Dict, Any, List
from io import BytesIO
import logging
from .base_processor import BaseProcessor
from models.document import Document


logger = logging.getLogger(__name__)


class PyMuPDFProcessor(BaseProcessor):
    """Processor for diagrams and technical drawings using PyMuPDF."""
    
    SUPPORTED_TYPES = ['diagram', 'sparepartslist']
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.extract_images = config.get('extract_images', False) if config else False
    
    def supports_document_type(self, document_type: str) -> bool:
        """Check if processor supports given document type."""
        return document_type in self.SUPPORTED_TYPES
    
    def process(self, document: Document, content: bytes) -> Dict[str, Any]:
        """
        Process document using PyMuPDF for simple text extraction.
        
        Args:
            document: Document metadata
            content: Raw PDF content as bytes
            
        Returns:
            Dictionary with page-level text extraction results
        """
        if not self.validate_content(content):
            raise ValueError("Invalid document content")
        
        try:
            # Open PDF from bytes
            pdf_document = pymupdf.open(stream=content, filetype="pdf")
            
            pages = []
            total_pages = len(pdf_document)
            
            for page_num in range(total_pages):
                page = pdf_document[page_num]
                
                # Extract text from page
                text = page.get_text()
                
                # Clean and normalize text
                cleaned_text = self._clean_text(text)
                
                # Create page data
                page_data = {
                    'page_number': page_num + 1,
                    'page_id': f"{document.filename}_page_{page_num + 1}",
                    'content': cleaned_text,
                    'metadata': {
                        'character_count': len(cleaned_text),
                        'word_count': len(cleaned_text.split()),
                        'has_content': bool(cleaned_text.strip()),
                        'page_size': {
                            'width': page.rect.width,
                            'height': page.rect.height
                        }
                    }
                }
                
                # Add image information if requested
                if self.extract_images:
                    page_data['metadata']['image_count'] = len(page.get_images())
                
                pages.append(page_data)
            
            pdf_document.close()
            
            # Calculate document-level statistics
            total_text = ' '.join([p['content'] for p in pages])
            
            return {
                'pages': pages,
                'document_metadata': {
                    'total_pages': total_pages,
                    'total_characters': len(total_text),
                    'total_words': len(total_text.split()),
                    'processing_method': 'pymupdf_text_extraction',
                    'document_type': document.document_type,
                    'filename': document.filename
                },
                'processing_info': {
                    'processor': 'PyMuPDFProcessor',
                    'success': True,
                    'pages_processed': len(pages),
                    'pages_with_content': sum(1 for p in pages if p['metadata']['has_content'])
                }
            }
            
        except Exception as e:
            logger.error(f"Error processing document {document.filename}: {str(e)}")
            return {
                'pages': [],
                'document_metadata': {
                    'filename': document.filename,
                    'document_type': document.document_type,
                    'processing_method': 'pymupdf_text_extraction'
                },
                'processing_info': {
                    'processor': 'PyMuPDFProcessor',
                    'success': False,
                    'error': str(e)
                }
            }
    
    def _clean_text(self, text: str) -> str:
        """
        Clean and normalize extracted text.
        
        Args:
            text: Raw text from PyMuPDF
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Remove excessive whitespace
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            if line:  # Only keep non-empty lines
                cleaned_lines.append(line)
        
        # Join lines with single newline
        cleaned_text = '\n'.join(cleaned_lines)
        
        # Remove excessive spaces
        import re
        cleaned_text = re.sub(r' +', ' ', cleaned_text)
        
        return cleaned_text
    
    def validate_content(self, content: bytes) -> bool:
        """
        Validate that content is a valid PDF.
        
        Args:
            content: Raw document content
            
        Returns:
            True if content is valid PDF
        """
        if not super().validate_content(content):
            return False
        
        # Check PDF magic bytes
        return content.startswith(b'%PDF-')
    
    def get_page_text(self, content: bytes, page_number: int) -> str:
        """
        Extract text from a specific page.
        
        Args:
            content: Raw PDF content
            page_number: Page number (1-based)
            
        Returns:
            Text content of the page
        """
        try:
            pdf_document = pymupdf.open(stream=content, filetype="pdf")
            
            if page_number < 1 or page_number > len(pdf_document):
                raise ValueError(f"Invalid page number: {page_number}")
            
            page = pdf_document[page_number - 1]  # Convert to 0-based
            text = page.get_text()
            pdf_document.close()
            
            return self._clean_text(text)
            
        except Exception as e:
            logger.error(f"Error extracting text from page {page_number}: {str(e)}")
            return ""