"""
PDF chunking utility for splitting large documents into smaller chunks
that can be processed by APIs with file size limits.
"""

import PyPDF2
import io
import logging
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ChunkInfo:
    """Information about a document chunk"""
    chunk_id: str
    start_page: int
    end_page: int
    content: bytes
    size_bytes: int
    
class PDFChunker:
    """Utility class for chunking PDF documents"""
    
    def __init__(self, max_chunk_size_mb: int = 80):
        """
        Initialize PDF chunker.
        
        Args:
            max_chunk_size_mb: Maximum chunk size in MB (default 80MB to stay under 100MB limit)
        """
        self.max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024
        
    def chunk_pdf(self, pdf_content: bytes, filename: str) -> List[ChunkInfo]:
        """
        Split PDF into chunks based on size limit.
        
        Args:
            pdf_content: PDF content as bytes
            filename: Original filename for reference
            
        Returns:
            List of ChunkInfo objects
        """
        try:
            # Read PDF
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            total_pages = len(pdf_reader.pages)
            
            logger.info(f"Chunking PDF {filename} with {total_pages} pages")
            
            chunks = []
            current_chunk_pages = []
            current_chunk_size = 0
            chunk_counter = 1
            chunk_start_page = 0
            
            for page_num in range(total_pages):
                page = pdf_reader.pages[page_num]
                
                # Create a temporary PDF with just this page to estimate size
                temp_pdf = PyPDF2.PdfWriter()
                temp_pdf.add_page(page)
                
                # Get page size
                temp_buffer = io.BytesIO()
                temp_pdf.write(temp_buffer)
                page_size = temp_buffer.tell()
                temp_buffer.close()
                
                # Check if adding this page would exceed limit
                if current_chunk_size + page_size > self.max_chunk_size_bytes and current_chunk_pages:
                    # Create chunk from current pages
                    chunk_info = self._create_chunk(
                        current_chunk_pages, 
                        filename, 
                        chunk_counter,
                        chunk_start_page,
                        chunk_start_page + len(current_chunk_pages) - 1
                    )
                    chunks.append(chunk_info)
                    
                    # Start new chunk
                    chunk_start_page = page_num
                    current_chunk_pages = [page]
                    current_chunk_size = page_size
                    chunk_counter += 1
                else:
                    # Add page to current chunk
                    current_chunk_pages.append(page)
                    current_chunk_size += page_size
            
            # Create final chunk if there are remaining pages
            if current_chunk_pages:
                chunk_info = self._create_chunk(
                    current_chunk_pages, 
                    filename, 
                    chunk_counter,
                    chunk_start_page,
                    chunk_start_page + len(current_chunk_pages) - 1
                )
                chunks.append(chunk_info)
            
            logger.info(f"Created {len(chunks)} chunks for {filename}")
            return chunks
            
        except Exception as e:
            logger.error(f"Error chunking PDF {filename}: {str(e)}")
            raise
    
    def _create_chunk(self, pages: List, filename: str, chunk_number: int, start_page: int, end_page: int) -> ChunkInfo:
        """Create a chunk from a list of pages"""
        # Create PDF writer
        pdf_writer = PyPDF2.PdfWriter()
        
        # Add pages to writer
        for page in pages:
            pdf_writer.add_page(page)
        
        # Write to bytes
        buffer = io.BytesIO()
        pdf_writer.write(buffer)
        chunk_content = buffer.getvalue()
        buffer.close()
        
        # Create chunk info
        chunk_id = f"{filename}_chunk_{chunk_number}"
        
        return ChunkInfo(
            chunk_id=chunk_id,
            start_page=start_page + 1,  # 1-based page numbering
            end_page=end_page + 1,      # 1-based page numbering
            content=chunk_content,
            size_bytes=len(chunk_content)
        )
    
    def _estimate_pages_per_chunk(self, total_pages: int, total_chunks: int) -> int:
        """Estimate average pages per chunk"""
        return total_pages // total_chunks if total_chunks > 0 else total_pages
    
    def get_chunk_info(self, pdf_content: bytes, filename: str) -> Dict[str, Any]:
        """
        Get information about how the PDF would be chunked without actually chunking.
        
        Args:
            pdf_content: PDF content as bytes
            filename: Original filename
            
        Returns:
            Dictionary with chunking information
        """
        try:
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            total_pages = len(pdf_reader.pages)
            total_size_mb = len(pdf_content) / (1024 * 1024)
            
            # Estimate number of chunks needed
            estimated_chunks = max(1, int(total_size_mb / (self.max_chunk_size_bytes / (1024 * 1024))))
            estimated_pages_per_chunk = total_pages // estimated_chunks if estimated_chunks > 0 else total_pages
            
            return {
                'filename': filename,
                'total_pages': total_pages,
                'total_size_mb': round(total_size_mb, 2),
                'estimated_chunks': estimated_chunks,
                'estimated_pages_per_chunk': estimated_pages_per_chunk,
                'max_chunk_size_mb': self.max_chunk_size_bytes / (1024 * 1024),
                'needs_chunking': total_size_mb > (self.max_chunk_size_bytes / (1024 * 1024))
            }
            
        except Exception as e:
            logger.error(f"Error getting chunk info for {filename}: {str(e)}")
            raise

class ChunkProcessor:
    """Utility for processing chunked results and combining them"""
    
    @staticmethod
    def combine_chunk_results(chunk_results: List[Dict[str, Any]], original_filename: str, max_expected_pages: int = None) -> Dict[str, Any]:
        """
        Combine results from multiple chunks into a single result.
        
        Args:
            chunk_results: List of processing results from individual chunks
            original_filename: Original document filename
            max_expected_pages: Maximum expected pages (for validation)
            
        Returns:
            Combined processing result
        """
        if not chunk_results:
            return {
                'pages': [],
                'document_metadata': {
                    'filename': original_filename,
                    'processing_method': 'chunked',
                    'total_chunks': 0
                },
                'processing_info': {
                    'success': False,
                    'error': 'No chunk results provided',
                    'processor': 'chunked'
                }
            }
        
        # Combine all pages from all chunks
        combined_pages = []
        current_page_number = 1
        
        successful_chunks = 0
        failed_chunks = 0
        errors = []
        
        # Track original page numbers to detect duplicates or gaps
        page_numbers_seen = set()
        
        for i, chunk_result in enumerate(chunk_results):
            processing_info = chunk_result.get('processing_info', {})
            
            if processing_info.get('success', False):
                successful_chunks += 1
                chunk_pages = chunk_result.get('pages', [])
                chunk_info = chunk_result.get('chunk_info', {})
                
                # Get original page range for this chunk
                original_start_page = chunk_info.get('start_page', current_page_number)
                
                # Adjust page numbers to maintain original document page numbering
                logger.info(f"Processing chunk {i+1} with {len(chunk_pages)} pages, start_page: {original_start_page}")
                
                for j, page in enumerate(chunk_pages):
                    # Calculate the actual page number in the original document
                    original_page_num = original_start_page + j
                    
                    # DEBUG: Log page number mapping
                    old_page_num = page.get('page_number', 'unknown')
                    old_page_id = page.get('page_id', 'unknown')
                    logger.debug(f"  Remapping page: {old_page_num} (id: {old_page_id}) -> {original_page_num} (id: page_{original_page_num})")
                    
                    # Validate page number doesn't exceed expected maximum
                    if max_expected_pages and original_page_num > max_expected_pages:
                        logger.warning(f"Page number {original_page_num} exceeds expected maximum {max_expected_pages}")
                        # Cap the page number at the maximum
                        original_page_num = min(original_page_num, max_expected_pages)
                    
                    # Check for duplicate page numbers
                    if original_page_num in page_numbers_seen:
                        logger.warning(f"Duplicate page number detected: {original_page_num}")
                    else:
                        page_numbers_seen.add(original_page_num)
                    
                    # Update page identifiers to reflect original document structure
                    page['page_number'] = original_page_num
                    page['page_id'] = f"page_{original_page_num}"
                    
                    # Add chunk information for traceability
                    page['chunk_info'] = {
                        'chunk_id': chunk_info.get('chunk_id', f'chunk_{i+1}'),
                        'chunk_page_number': j + 1,  # Page number within this chunk
                        'original_page_number': original_page_num  # Page number in original document
                    }
                
                combined_pages.extend(chunk_pages)
                current_page_number += len(chunk_pages)
            else:
                failed_chunks += 1
                error_msg = processing_info.get('error', f'Chunk {i+1} failed')
                errors.append(error_msg)
                
                # Still increment page counter for failed chunks to maintain numbering
                chunk_info = chunk_result.get('chunk_info', {})
                if chunk_info:
                    start_page = chunk_info.get('start_page', current_page_number)
                    end_page = chunk_info.get('end_page', current_page_number)
                    current_page_number = end_page + 1
        
        # Create combined result
        overall_success = successful_chunks > 0 and failed_chunks == 0
        
        combined_result = {
            'pages': combined_pages,
            'document_metadata': {
                'filename': original_filename,
                'processing_method': 'chunked',
                'total_chunks': len(chunk_results),
                'successful_chunks': successful_chunks,
                'failed_chunks': failed_chunks,
                'document_type': chunk_results[0].get('document_metadata', {}).get('document_type', 'unknown') if chunk_results else 'unknown'
            },
            'processing_info': {
                'success': overall_success,
                'processor': 'chunked',
                'total_pages': len(combined_pages),
                'pages_processed': len(combined_pages),
                'pages_with_content': len([p for p in combined_pages if p.get('text', '').strip()]),
                'chunks_processed': len(chunk_results),
                'successful_chunks': successful_chunks,
                'failed_chunks': failed_chunks,
                'errors': errors if errors else None
            }
        }
        
        # Validate final page count
        if max_expected_pages:
            final_page_count = len(combined_pages)
            max_page_num = max([p.get('page_number', 0) for p in combined_pages]) if combined_pages else 0
            
            if max_page_num > max_expected_pages:
                logger.warning(f"Final result has page numbers up to {max_page_num}, but PDF only has {max_expected_pages} pages")
                combined_result['processing_info']['page_number_warning'] = f"Page numbers exceed expected count: {max_page_num} > {max_expected_pages}"
            
            if final_page_count > max_expected_pages:
                logger.warning(f"Final result has {final_page_count} pages, but PDF only has {max_expected_pages} pages")
                combined_result['processing_info']['page_count_warning'] = f"Page count exceeds expected: {final_page_count} > {max_expected_pages}"
        
        if not overall_success:
            error_details = []
            for i, chunk_result in enumerate(chunk_results):
                processing_info = chunk_result.get('processing_info', {})
                if not processing_info.get('success', False):
                    chunk_info = chunk_result.get('chunk_info', {})
                    error_msg = processing_info.get('error', 'Unknown error')
                    error_type = processing_info.get('error_type', 'Unknown')
                    chunk_details = processing_info.get('chunk_details', {})
                    page_range = chunk_details.get('page_range', f"chunk {i+1}")
                    error_details.append(f"Chunk {i+1} (pages {page_range}): {error_type} - {error_msg}")
            
            combined_result['processing_info']['error'] = f"Processing failed: {failed_chunks} of {len(chunk_results)} chunks failed. Details: {'; '.join(error_details)}"
        
        return combined_result