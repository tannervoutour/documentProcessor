import asyncio
import logging
from typing import List, Dict, Optional, Callable, Any
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from threading import Lock

from models.document import Document
from core.document_manager import DocumentManager

logger = logging.getLogger(__name__)

@dataclass
class BatchResult:
    """Result of batch processing operation"""
    total_documents: int
    successful: List[Dict[str, Any]]
    failed: List[Dict[str, Any]]
    processing_time: float
    cache_hits: int
    cache_misses: int
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_documents == 0:
            return 0.0
        return (len(self.successful) / self.total_documents) * 100
    
    @property
    def cache_hit_rate(self) -> float:
        """Calculate cache hit rate percentage"""
        total_processed = self.cache_hits + self.cache_misses
        if total_processed == 0:
            return 0.0
        return (self.cache_hits / total_processed) * 100

class BatchProcessor:
    """
    Optimized batch processor with concurrency control and resource management.
    Processes documents in parallel with configurable limits and progress tracking.
    """
    
    def __init__(self, document_manager: DocumentManager, 
                 max_concurrent_documents: int = 3,
                 max_concurrent_processors: int = 2,
                 progress_callback: Optional[Callable] = None):
        self.document_manager = document_manager
        self.max_concurrent_documents = max_concurrent_documents
        self.max_concurrent_processors = max_concurrent_processors
        self.progress_callback = progress_callback
        self.logger = logging.getLogger(__name__)
        
        # Thread-safe counters
        self._processed_count = 0
        self._cache_hits = 0
        self._cache_misses = 0
        self._lock = Lock()
        
        # Processing state
        self._cancelled = False
        self._current_batch_id = None
    
    def process_batch(self, documents_with_metadata: List[Dict]) -> BatchResult:
        """
        Process a batch of documents with optimal concurrency.
        
        Args:
            documents_with_metadata: List of dicts with 'document' and 'metadata' keys
            
        Returns:
            BatchResult with processing statistics and results
        """
        start_time = time.time()
        total_documents = len(documents_with_metadata)
        
        self.logger.info(f"Starting batch processing of {total_documents} documents")
        self.logger.info(f"Concurrency limits: {self.max_concurrent_documents} documents, {self.max_concurrent_processors} processors")
        
        # Reset counters
        with self._lock:
            self._processed_count = 0
            self._cache_hits = 0
            self._cache_misses = 0
            self._cancelled = False
            self._current_batch_id = datetime.now().isoformat()
        
        # Group documents by processor type for better resource utilization
        processor_groups = self._group_by_processor_type(documents_with_metadata)
        
        successful_results = []
        failed_results = []
        
        # Process each processor group with appropriate concurrency
        for processor_type, docs in processor_groups.items():
            if self._cancelled:
                break
                
            self.logger.info(f"Processing {len(docs)} documents with {processor_type}")
            
            # Determine concurrency for this processor type
            concurrency = self._get_processor_concurrency(processor_type)
            
            # Process documents in this group
            group_results = self._process_processor_group(docs, concurrency)
            
            successful_results.extend(group_results['successful'])
            failed_results.extend(group_results['failed'])
        
        processing_time = time.time() - start_time
        
        # Create batch result
        batch_result = BatchResult(
            total_documents=total_documents,
            successful=successful_results,
            failed=failed_results,
            processing_time=processing_time,
            cache_hits=self._cache_hits,
            cache_misses=self._cache_misses
        )
        
        self.logger.info(f"Batch processing completed in {processing_time:.2f}s")
        self.logger.info(f"Success rate: {batch_result.success_rate:.1f}%")
        self.logger.info(f"Cache hit rate: {batch_result.cache_hit_rate:.1f}%")
        
        return batch_result
    
    def _group_by_processor_type(self, documents_with_metadata: List[Dict]) -> Dict[str, List[Dict]]:
        """Group documents by their processor type for optimal scheduling"""
        from processors.processor_factory import processor_factory
        
        groups = {}
        for item in documents_with_metadata:
            document_type = item['metadata'].get('document_type', 'unknown')
            
            # Get processor class name
            try:
                processor = processor_factory.get_processor(document_type)
                processor_type = processor.__class__.__name__
            except Exception:
                processor_type = 'Unknown'
            
            if processor_type not in groups:
                groups[processor_type] = []
            groups[processor_type].append(item)
        
        self.logger.info(f"Grouped documents by processor: {[(k, len(v)) for k, v in groups.items()]}")
        return groups
    
    def _get_processor_concurrency(self, processor_type: str) -> int:
        """Get optimal concurrency for processor type"""
        # DataLabs processor is I/O intensive, can handle more concurrent requests
        # PyMuPDF is CPU intensive, should be limited
        
        concurrency_map = {
            'DataLabsProcessor': min(self.max_concurrent_documents, 4),
            'PyMuPDFProcessor': min(self.max_concurrent_processors, 2),
            'Unknown': 1
        }
        
        return concurrency_map.get(processor_type, 1)
    
    def _process_processor_group(self, documents: List[Dict], concurrency: int) -> Dict[str, List[Dict]]:
        """Process a group of documents with specific concurrency limit"""
        successful = []
        failed = []
        
        if concurrency == 1:
            # Sequential processing
            for doc_item in documents:
                if self._cancelled:
                    break
                result = self._process_single_document(doc_item)
                if result['success']:
                    successful.append(result)
                else:
                    failed.append(result)
        else:
            # Parallel processing with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                # Submit all tasks
                future_to_doc = {
                    executor.submit(self._process_single_document, doc_item): doc_item
                    for doc_item in documents
                }
                
                # Process completed tasks
                for future in as_completed(future_to_doc):
                    if self._cancelled:
                        break
                        
                    try:
                        result = future.result()
                        if result['success']:
                            successful.append(result)
                        else:
                            failed.append(result)
                    except Exception as e:
                        doc_item = future_to_doc[future]
                        document = doc_item['document']
                        self.logger.error(f"Exception in thread processing {document.filename}: {e}")
                        failed.append({
                            'success': False,
                            'document_id': document.file_id,
                            'filename': document.filename,
                            'error': str(e)
                        })
        
        return {'successful': successful, 'failed': failed}
    
    def _process_single_document(self, doc_item: Dict) -> Dict[str, Any]:
        """Process a single document and update counters"""
        document = doc_item['document']
        metadata = doc_item['metadata']
        
        try:
            # Process document
            result = self.document_manager.process_document(document, metadata)
            
            # Update counters
            with self._lock:
                self._processed_count += 1
                if result.get('from_cache', False):
                    self._cache_hits += 1
                else:
                    self._cache_misses += 1
            
            # Call progress callback if provided
            if self.progress_callback:
                try:
                    self.progress_callback(self._processed_count, document.filename, result['success'])
                except Exception as callback_error:
                    self.logger.warning(f"Progress callback error: {callback_error}")
            
            if result['success']:
                return {
                    'success': True,
                    'document_id': document.file_id,
                    'filename': document.filename,
                    'processor_used': result.get('processor_used', 'Unknown'),
                    'processing_time': result.get('processing_time', 0),
                    'from_cache': result.get('from_cache', False),
                    'webhook_sent': result.get('webhook_result', {}).get('success', False)
                }
            else:
                return {
                    'success': False,
                    'document_id': document.file_id,
                    'filename': document.filename,
                    'error': result.get('error', 'Unknown error')
                }
                
        except Exception as e:
            self.logger.error(f"Error processing document {document.filename}: {e}")
            return {
                'success': False,
                'document_id': document.file_id,
                'filename': document.filename,
                'error': str(e)
            }
    
    def cancel_batch(self) -> None:
        """Cancel current batch processing"""
        with self._lock:
            self._cancelled = True
        self.logger.info("Batch processing cancellation requested")
    
    def get_batch_stats(self) -> Dict[str, Any]:
        """Get current batch processing statistics"""
        with self._lock:
            return {
                'processed_count': self._processed_count,
                'cache_hits': self._cache_hits,
                'cache_misses': self._cache_misses,
                'cache_hit_rate': (self._cache_hits / max(1, self._cache_hits + self._cache_misses)) * 100,
                'batch_id': self._current_batch_id,
                'cancelled': self._cancelled
            }

class BatchProcessorBuilder:
    """Builder pattern for creating BatchProcessor with different configurations"""
    
    def __init__(self, document_manager: DocumentManager):
        self.document_manager = document_manager
        self.max_concurrent_documents = 3
        self.max_concurrent_processors = 2
        self.progress_callback = None
    
    def with_document_concurrency(self, max_concurrent: int) -> 'BatchProcessorBuilder':
        """Set maximum concurrent documents"""
        self.max_concurrent_documents = max_concurrent
        return self
    
    def with_processor_concurrency(self, max_concurrent: int) -> 'BatchProcessorBuilder':
        """Set maximum concurrent processors"""
        self.max_concurrent_processors = max_concurrent
        return self
    
    def with_progress_callback(self, callback: Callable) -> 'BatchProcessorBuilder':
        """Set progress callback function"""
        self.progress_callback = callback
        return self
    
    def build(self) -> BatchProcessor:
        """Build the BatchProcessor instance"""
        return BatchProcessor(
            document_manager=self.document_manager,
            max_concurrent_documents=self.max_concurrent_documents,
            max_concurrent_processors=self.max_concurrent_processors,
            progress_callback=self.progress_callback
        )