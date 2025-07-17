"""
Content utility functions for document processing and n8n webhook preparation.
"""

import logging
from typing import Dict, List, Any, Optional
from models.document import Document


logger = logging.getLogger(__name__)


def extract_file_type(filename: str) -> str:
    """
    Extract file type from filename.
    
    Args:
        filename: Document filename
        
    Returns:
        File extension in uppercase (e.g., 'PDF', 'CSV', 'DOCX')
    """
    if '.' not in filename:
        return 'UNKNOWN'
    
    extension = filename.split('.')[-1].lower()
    
    # Map common extensions to standard types
    extension_map = {
        'pdf': 'PDF',
        'csv': 'CSV',
        'xlsx': 'EXCEL',
        'xls': 'EXCEL',
        'docx': 'WORD',
        'doc': 'WORD',
        'txt': 'TEXT',
        'json': 'JSON',
        'xml': 'XML'
    }
    
    return extension_map.get(extension, extension.upper())


def consolidate_full_text(pages: List[Dict]) -> str:
    """
    Consolidate all page content into a single string with page identifiers.
    
    Args:
        pages: List of page dictionaries from processing results
        
    Returns:
        Consolidated text with page identifiers
    """
    if not pages:
        return ""
    
    consolidated_parts = []
    
    for page in pages:
        page_number = page.get('page_number', 1)
        page_id = page.get('page_id', f'page_{page_number}')
        content = page.get('content', '').strip()
        
        if content:
            # Add page identifier header
            page_header = f"=== {page_id} ==="
            consolidated_parts.append(page_header)
            consolidated_parts.append(content)
            consolidated_parts.append("")  # Empty line between pages
    
    return "\n".join(consolidated_parts)


def extract_first_n_pages(pages: List[Dict], n: int = 10) -> List[Dict]:
    """
    Extract the first N pages from processing results.
    
    Args:
        pages: List of page dictionaries from processing results
        n: Number of pages to extract (default: 10)
        
    Returns:
        List of first N pages with content and metadata
    """
    if not pages:
        return []
    
    first_pages = []
    
    for page in pages[:n]:
        page_data = {
            'page_number': page.get('page_number', 1),
            'page_id': page.get('page_id', f'page_{page.get("page_number", 1)}'),
            'content': page.get('content', '').strip(),
            'metadata': page.get('metadata', {}),
            'has_content': bool(page.get('content', '').strip())
        }
        first_pages.append(page_data)
    
    return first_pages


def extract_first_n_pages_text(pages: List[Dict], n: int = 10) -> str:
    """
    Extract the first N pages as consolidated text.
    
    Args:
        pages: List of page dictionaries from processing results
        n: Number of pages to extract (default: 10)
        
    Returns:
        Consolidated text of first N pages with page identifiers
    """
    first_pages = extract_first_n_pages(pages, n)
    return consolidate_full_text(first_pages)


def determine_content_format(processing_result: Dict) -> str:
    """
    Determine the content format from processing results.
    
    Args:
        processing_result: Results from document processing
        
    Returns:
        Content format ('markdown' or 'text')
    """
    document_metadata = processing_result.get('document_metadata', {})
    processing_method = document_metadata.get('processing_method', '')
    
    if 'datalabs' in processing_method:
        return 'markdown'
    elif 'pymupdf' in processing_method:
        return 'text'
    else:
        return 'text'  # Default to text


def calculate_content_statistics(pages: List[Dict]) -> Dict[str, Any]:
    """
    Calculate content statistics from pages.
    
    Args:
        pages: List of page dictionaries
        
    Returns:
        Dictionary with content statistics
    """
    if not pages:
        return {
            'total_pages': 0,
            'pages_with_content': 0,
            'total_characters': 0,
            'total_words': 0,
            'total_images': 0,
            'total_tables': 0,
            'average_words_per_page': 0.0
        }
    
    total_characters = 0
    total_words = 0
    pages_with_content = 0
    total_images = 0
    total_tables = 0
    
    for page in pages:
        metadata = page.get('metadata', {})
        
        if metadata.get('has_content', False):
            pages_with_content += 1
        
        total_characters += metadata.get('character_count', 0)
        total_words += metadata.get('word_count', 0)
        total_images += metadata.get('image_count', 0)
        total_tables += metadata.get('table_count', 0)
    
    average_words_per_page = total_words / len(pages) if pages else 0.0
    
    return {
        'total_pages': len(pages),
        'pages_with_content': pages_with_content,
        'total_characters': total_characters,
        'total_words': total_words,
        'total_images': total_images,
        'total_tables': total_tables,
        'average_words_per_page': round(average_words_per_page, 2)
    }


def validate_processing_result(processing_result: Dict) -> List[str]:
    """
    Validate processing result structure for webhook preparation.
    
    Args:
        processing_result: Results from document processing
        
    Returns:
        List of validation errors (empty if valid)
    """
    errors = []
    
    # Check required top-level keys
    required_keys = ['pages', 'document_metadata', 'processing_info']
    for key in required_keys:
        if key not in processing_result:
            errors.append(f"Missing required key: {key}")
    
    # Check processing success
    processing_info = processing_result.get('processing_info', {})
    if not processing_info.get('success', False):
        errors.append("Processing was not successful")
    
    # Check pages structure
    pages = processing_result.get('pages', [])
    if not isinstance(pages, list):
        errors.append("Pages must be a list")
    elif len(pages) == 0:
        errors.append("No pages found in processing result")
    
    # Check document metadata
    document_metadata = processing_result.get('document_metadata', {})
    required_metadata = ['filename', 'document_type', 'processing_method']
    for key in required_metadata:
        if key not in document_metadata:
            errors.append(f"Missing required document metadata: {key}")
    
    return errors


def prepare_page_identifiers(pages: List[Dict]) -> List[str]:
    """
    Extract page identifiers from pages.
    
    Args:
        pages: List of page dictionaries
        
    Returns:
        List of page identifiers
    """
    identifiers = []
    
    for page in pages:
        page_id = page.get('page_id')
        if page_id:
            identifiers.append(page_id)
        else:
            # Fallback to page number
            page_number = page.get('page_number', 1)
            identifiers.append(f"page_{page_number}")
    
    return identifiers


def clean_content_for_webhook(content: str) -> str:
    """
    Clean content for webhook transmission.
    
    Args:
        content: Raw content string
        
    Returns:
        Cleaned content suitable for webhook
    """
    if not content:
        return ""
    
    # Remove excessive whitespace
    lines = content.split('\n')
    cleaned_lines = []
    
    for line in lines:
        line = line.strip()
        if line:  # Only keep non-empty lines
            cleaned_lines.append(line)
        elif cleaned_lines and not cleaned_lines[-1] == "":
            # Keep one empty line between sections
            cleaned_lines.append("")
    
    # Join lines and remove excessive empty lines
    cleaned_content = '\n'.join(cleaned_lines)
    
    # Remove more than 2 consecutive newlines
    import re
    cleaned_content = re.sub(r'\n{3,}', '\n\n', cleaned_content)
    
    return cleaned_content.strip()


def format_machine_names(machine_names: List[str]) -> List[str]:
    """
    Format machine names for webhook.
    
    Args:
        machine_names: List of machine names
        
    Returns:
        Formatted list of machine names
    """
    if not machine_names:
        return []
    
    formatted_names = []
    for name in machine_names:
        if isinstance(name, str) and name.strip():
            formatted_names.append(name.strip())
    
    return formatted_names


def get_processing_timestamp() -> str:
    """
    Get current timestamp for processing.
    
    Returns:
        ISO formatted timestamp
    """
    from datetime import datetime
    return datetime.utcnow().isoformat() + "Z"