from typing import List, Dict, Optional, Set
from core.s3_client import S3Client
from core.supabase_client import SupabaseClient
from core.result_cache import ResultCache
from models.document import Document
from processors.processor_factory import processor_factory
from integration.n8n_webhook import N8nWebhookClient
from config.settings import N8N_WEBHOOK_URL, N8N_API_KEY
import logging
import json

class DocumentManager:
    """Manages document discovery and comparison"""
    
    def __init__(self, s3_client: S3Client, supabase_client: SupabaseClient):
        self.s3 = s3_client
        self.supabase = supabase_client
        self.logger = logging.getLogger(__name__)
        
        # Initialize result cache
        self.result_cache = ResultCache()
        
        # Initialize n8n webhook client if configured
        self.n8n_client = None
        if N8N_WEBHOOK_URL:
            self.n8n_client = N8nWebhookClient(N8N_WEBHOOK_URL, N8N_API_KEY)
            self.logger.info("n8n webhook client initialized")
        else:
            self.logger.warning("n8n webhook not configured - skipping webhook notifications")
    
    @property
    def cache(self):
        """Alias for result_cache for backward compatibility"""
        return self.result_cache
    
    def get_unprocessed_documents(self, prefix: str = "") -> List[Document]:
        """
        Compare S3 and Supabase to find unprocessed documents
        Returns list of Document objects that need processing
        """
        # Get all documents from S3
        s3_documents = list(self.s3.list_documents(prefix=prefix))
        self.logger.info(f"Found {len(s3_documents)} documents in S3")
        
        # Get processed document titles from Supabase
        processed_titles = set(self.supabase.get_processed_documents())
        self.logger.info(f"Found {len(processed_titles)} processed documents in Supabase")
        
        # Filter unprocessed documents
        unprocessed = []
        for doc in s3_documents:
            if doc.filename not in processed_titles:
                unprocessed.append(doc)
        
        self.logger.info(f"Found {len(unprocessed)} unprocessed documents")
        return unprocessed
    
    def get_processed_documents(self, prefix: str = "") -> List[Document]:
        """
        Get documents that have been processed
        Returns list of Document objects that have been processed
        """
        # Get all documents from S3
        s3_documents = list(self.s3.list_documents(prefix=prefix))
        
        # Get processed document titles from Supabase
        processed_titles = set(self.supabase.get_processed_documents())
        
        # Filter processed documents
        processed = []
        for doc in s3_documents:
            if doc.filename in processed_titles:
                doc.processed = True
                processed.append(doc)
        
        return processed
    
    def get_document_with_metadata(self, file_id: str) -> Optional[Dict]:
        """Get document with its metadata from Supabase"""
        try:
            metadata = self.supabase.get_document_metadata(file_id)
            if not metadata:
                return None
            
            # Try to find corresponding S3 document
            s3_key = metadata.get('s3_key')
            if s3_key and self.s3.document_exists(s3_key):
                s3_metadata = self.s3.get_document_metadata(s3_key)
                return {
                    'supabase_metadata': metadata,
                    's3_metadata': s3_metadata,
                    'file_id': file_id
                }
            
            return {'supabase_metadata': metadata, 'file_id': file_id}
        except Exception as e:
            self.logger.error(f"Error getting document with metadata: {e}")
            return None
    
    def sync_document_status(self, document: Document, metadata: Dict) -> bool:
        """
        DEPRECATED: This method is no longer used.
        Document status is not synced to Supabase - only sent to n8n webhook.
        Supabase is only used for reading processed document list for comparison.
        """
        self.logger.warning("sync_document_status is deprecated and does nothing")
        return True
    
    def export_document_list(self, documents: List[Document], format: str = 'json') -> Dict:
        """Export document list in specified format"""
        if format == 'json':
            return {
                'total': len(documents),
                'export_timestamp': self._get_current_timestamp(),
                'documents': [doc.to_dict() for doc in documents]
            }
        elif format == 'csv':
            # Return CSV-formatted data
            import csv
            import io
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header
            writer.writerow([
                'file_id', 'filename', 's3_key', 'file_size', 'last_modified',
                'machine_names', 'document_type', 'processing_status'
            ])
            
            # Write data rows
            for doc in documents:
                writer.writerow([
                    doc.file_id,
                    doc.filename,
                    doc.s3_key,
                    doc.file_size,
                    doc.last_modified.isoformat(),
                    ','.join(doc.machine_names) if doc.machine_names else '',
                    doc.document_type or '',
                    doc.processing_status
                ])
            
            return {
                'total': len(documents),
                'export_timestamp': self._get_current_timestamp(),
                'csv_data': output.getvalue()
            }
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def get_statistics(self) -> Dict:
        """Get processing statistics"""
        try:
            # Get S3 statistics
            s3_docs = list(self.s3.list_documents())
            s3_stats = {
                'total_in_s3': len(s3_docs),
                'size_distribution': self._calculate_size_distribution(s3_docs),
                'file_types': self._count_file_types(s3_docs)
            }
            
            # Get Supabase statistics
            supabase_stats = self.supabase.get_processing_statistics()
            
            # Get cache statistics
            cache_stats = self.result_cache.get_cache_stats()
            
            # Calculate processing progress
            processed_count = supabase_stats.get('total_documents', 0)
            total_count = len(s3_docs)
            
            return {
                's3_statistics': s3_stats,
                'supabase_statistics': supabase_stats,
                'cache_statistics': cache_stats,
                'processing_progress': {
                    'total_documents': total_count,
                    'processed_documents': processed_count,
                    'unprocessed_documents': total_count - processed_count,
                    'completion_percentage': (processed_count / total_count * 100) if total_count > 0 else 0
                }
            }
        except Exception as e:
            self.logger.error(f"Error getting statistics: {e}")
            return {}
    
    def _calculate_size_distribution(self, documents: List[Document]) -> Dict:
        """Calculate file size distribution"""
        size_ranges = {
            'small': 0,      # < 1MB
            'medium': 0,     # 1MB - 10MB
            'large': 0,      # 10MB - 100MB
            'xlarge': 0      # > 100MB
        }
        
        for doc in documents:
            size_mb = doc.file_size / (1024 * 1024)
            if size_mb < 1:
                size_ranges['small'] += 1
            elif size_mb < 10:
                size_ranges['medium'] += 1
            elif size_mb < 100:
                size_ranges['large'] += 1
            else:
                size_ranges['xlarge'] += 1
        
        return size_ranges
    
    def _count_file_types(self, documents: List[Document]) -> Dict:
        """Count documents by file extension"""
        file_types = {}
        for doc in documents:
            ext = doc.filename.split('.')[-1].lower() if '.' in doc.filename else 'no_extension'
            file_types[ext] = file_types.get(ext, 0) + 1
        return file_types
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime
        return datetime.utcnow().isoformat()
    
    def process_document(self, document: Document, metadata: Dict) -> Dict:
        """Process a document using the appropriate processor"""
        try:
            self.logger.info(f"Starting process_document for {document.filename}")
            
            # Validate metadata
            self.logger.info(f"Validating metadata for {document.filename}")
            validation_errors = self.validate_document_metadata(metadata)
            if validation_errors:
                self.logger.error(f"Validation failed for {document.filename}: {validation_errors}")
                return {
                    'success': False,
                    'error': 'Validation failed',
                    'validation_errors': validation_errors
                }
            
            # Update document with metadata
            self.logger.info(f"Updating document metadata for {document.filename}")
            document.document_type = metadata['document_type']
            document.machine_names = metadata['machine_names']
            
            # Check cache first
            cached_result = self.result_cache.get(document, metadata)
            if cached_result:
                self.logger.info(f"Using cached result for {document.filename}")
                
                # Still send webhook for cached results
                webhook_result = None
                if self.n8n_client:
                    try:
                        webhook_result = self.n8n_client.send_document_processed(
                            document, cached_result, metadata
                        )
                        if webhook_result['success']:
                            self.logger.info(f"n8n webhook sent successfully for cached {document.filename}")
                        else:
                            self.logger.error(f"n8n webhook failed for cached {document.filename}: {webhook_result.get('error')}")
                    except Exception as webhook_error:
                        self.logger.error(f"Error sending n8n webhook for cached result: {webhook_error}")
                        webhook_result = {'success': False, 'error': str(webhook_error)}
                
                return {
                    'success': True,
                    'document_id': document.file_id,
                    'processing_result': cached_result,
                    'processor_used': 'Cache',
                    'webhook_result': webhook_result,
                    'from_cache': True
                }
            
            # Get document content from S3
            self.logger.info(f"Downloading document from S3: {document.s3_key}")
            content = self.s3.download_document(document.s3_key)
            if not content:
                self.logger.error(f"Failed to download document from S3: {document.s3_key}")
                return {
                    'success': False,
                    'error': 'Failed to download document content from S3'
                }
            self.logger.info(f"Successfully downloaded document from S3, content size: {len(content)} bytes")
            
            # Get appropriate processor based on processing method
            processing_method = metadata.get('processing_method', 'markdown')  # Default to markdown
            self.logger.info(f"Getting processor for processing method: {processing_method} (document type: {document.document_type})")
            processor = processor_factory.get_processor_by_method(processing_method)
            
            # Process the document
            self.logger.info(f"Starting document processing with {processor.__class__.__name__}")
            result = processor.process(document, content)
            self.logger.info(f"Document processing completed for {document.filename}")
            
            # Cache the result
            self.result_cache.set(document, metadata, result, processor.__class__.__name__)
            
            # Update processing status
            processing_status = 'completed' if result['processing_info']['success'] else 'failed'
            
            # Note: Results are NOT stored in Supabase - only sent to n8n webhook
            # Supabase is only used for reading processed document list for comparison
            
            self.logger.info(f"Successfully processed document {document.filename} with {processor.__class__.__name__}")
            
            # Send n8n webhook notification if configured
            webhook_result = None
            if self.n8n_client:
                try:
                    webhook_result = self.n8n_client.send_document_processed(
                        document, result, metadata
                    )
                    if webhook_result['success']:
                        self.logger.info(f"n8n webhook sent successfully for {document.filename}")
                    else:
                        self.logger.error(f"n8n webhook failed for {document.filename}: {webhook_result.get('error')}")
                except Exception as webhook_error:
                    self.logger.error(f"Error sending n8n webhook: {webhook_error}")
                    webhook_result = {'success': False, 'error': str(webhook_error)}
            
            return {
                'success': True,
                'document_id': document.file_id,
                'processing_result': result,
                'processor_used': processor.__class__.__name__,
                'webhook_result': webhook_result,
                'from_cache': False
            }
            
        except Exception as e:
            self.logger.error(f"Error processing document {document.filename}: {str(e)}")
            
            # Note: Error status is NOT stored in Supabase - only sent to n8n webhook
            # Supabase is only used for reading processed document list for comparison
            
            # Send error notification to n8n webhook if configured
            webhook_result = None
            if self.n8n_client:
                try:
                    webhook_result = self.n8n_client.send_processing_error(
                        document, metadata, str(e)
                    )
                    if webhook_result['success']:
                        self.logger.info(f"n8n error webhook sent for {document.filename}")
                    else:
                        self.logger.error(f"n8n error webhook failed for {document.filename}: {webhook_result.get('error')}")
                except Exception as webhook_error:
                    self.logger.error(f"Error sending n8n error webhook: {webhook_error}")
                    webhook_result = {'success': False, 'error': str(webhook_error)}
            
            return {
                'success': False,
                'error': str(e),
                'document_id': document.file_id,
                'webhook_result': webhook_result
            }
    
    def batch_process_documents(self, documents_with_metadata: List[Dict], 
                                max_concurrent_documents: int = 3,
                                max_concurrent_processors: int = 2,
                                progress_callback=None) -> Dict:
        """Process multiple documents in batch with optimized concurrency"""
        # Import here to avoid circular imports
        from core.batch_processor import BatchProcessorBuilder
        
        # Create optimized batch processor
        batch_processor = (BatchProcessorBuilder(self)
                          .with_document_concurrency(max_concurrent_documents)
                          .with_processor_concurrency(max_concurrent_processors)
                          .with_progress_callback(progress_callback)
                          .build())
        
        # Process batch
        batch_result = batch_processor.process_batch(documents_with_metadata)
        
        # Send batch completion notification to n8n if configured
        if self.n8n_client:
            try:
                batch_summary = {
                    'batch_processing_complete': True,
                    'total_processed': batch_result.total_documents,
                    'successful_count': len(batch_result.successful),
                    'failed_count': len(batch_result.failed),
                    'processing_time': batch_result.processing_time,
                    'success_rate': batch_result.success_rate,
                    'cache_hit_rate': batch_result.cache_hit_rate,
                    'successful_documents': batch_result.successful,
                    'failed_documents': batch_result.failed,
                    'timestamp': self._get_current_timestamp()
                }
                
                batch_webhook_result = self.n8n_client.send_webhook(batch_summary)
                if batch_webhook_result['success']:
                    self.logger.info("Batch completion webhook sent successfully")
                else:
                    self.logger.error(f"Batch completion webhook failed: {batch_webhook_result.get('error')}")
                    
            except Exception as webhook_error:
                self.logger.error(f"Error sending batch completion webhook: {webhook_error}")
        
        # Convert BatchResult to legacy format for compatibility
        return {
            'successful': batch_result.successful,
            'failed': batch_result.failed,
            'total_processed': batch_result.total_documents,
            'processing_time': batch_result.processing_time,
            'success_rate': batch_result.success_rate,
            'cache_hit_rate': batch_result.cache_hit_rate,
            'cache_hits': batch_result.cache_hits,
            'cache_misses': batch_result.cache_misses
        }
    
    def get_processing_results(self, document_id: str) -> Optional[Dict]:
        """Get processing results for a document"""
        try:
            metadata = self.supabase.get_document_metadata(document_id)
            if not metadata:
                return None
            
            return {
                'document_id': document_id,
                'processing_status': metadata.get('processing_status'),
                'processing_result': metadata.get('processing_result'),
                'processing_error': metadata.get('processing_error'),
                'processed_at': metadata.get('updated_at')
            }
        except Exception as e:
            self.logger.error(f"Error getting processing results: {e}")
            return None
    
    def validate_document_metadata(self, metadata: Dict) -> List[str]:
        """Validate document metadata"""
        errors = []
        
        if not metadata.get('machine_names'):
            errors.append("Machine names are required")
        elif not isinstance(metadata['machine_names'], list):
            errors.append("Machine names must be a list")
        
        if not metadata.get('document_type'):
            errors.append("Document type is required")
        elif metadata.get('document_type') not in ["manual", "diagram", "sparepartslist", "spreadsheet", "plain_document"]:
            errors.append(f"Unsupported document type: {metadata.get('document_type')}")
        
        # Validate processing method
        processing_method = metadata.get('processing_method', 'markdown')
        if not processor_factory.supports_processing_method(processing_method):
            errors.append(f"Unsupported processing method: {processing_method}")
        
        return errors
    
    def clear_cache(self) -> None:
        """Clear result cache"""
        self.result_cache.clear_all()
        self.logger.info("Result cache cleared")
    
    def cleanup_cache(self) -> int:
        """Cleanup expired cache entries"""
        return self.result_cache.cleanup_expired()
    
    def create_batch_processor(self, max_concurrent_documents: int = 3,
                              max_concurrent_processors: int = 2,
                              progress_callback=None):
        """Create a batch processor with custom configuration"""
        from core.batch_processor import BatchProcessorBuilder
        
        return (BatchProcessorBuilder(self)
                .with_document_concurrency(max_concurrent_documents)
                .with_processor_concurrency(max_concurrent_processors)
                .with_progress_callback(progress_callback)
                .build())