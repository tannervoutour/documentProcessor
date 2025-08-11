import streamlit as st
import pandas as pd
import asyncio
from typing import List, Dict, Optional
from datetime import datetime
import time

from config.connections import get_s3_client, get_supabase_client, init_logging
from core.document_manager import DocumentManager
from models.document import Document
from ui.components.document_list import DocumentListComponent
from ui.components.metadata_editor import MetadataEditor
from ui.components.progress_tracker import ProgressTracker
from ui.components.cache_manager import CacheManagerComponent
from ui.utils import UIUtils

class DocumentProcessorApp:
    """Main Streamlit application for document processing"""
    
    def __init__(self):
        self.init_app()
    
    def init_app(self):
        """Initialize the application"""
        # Initialize logging
        init_logging()
        
        # Initialize session state
        UIUtils.init_session_state()
        
        # Apply custom CSS
        UIUtils.apply_custom_css()
        
        # Initialize clients
        try:
            self.s3_client = get_s3_client()
            self.supabase_client = get_supabase_client()
            self.doc_manager = DocumentManager(self.s3_client, self.supabase_client)
        except Exception as e:
            st.error(f"Failed to initialize clients: {e}")
            st.stop()
    
    def run(self):
        """Main application entry point"""
        
        # Main header
        st.markdown('<h1 class="main-header">📄 S3 Document Processor</h1>', unsafe_allow_html=True)
        
        # Sidebar
        self.render_sidebar()
        
        # Main content area
        if st.session_state.get('processing_active', False):
            self.render_processing_view()
        else:
            self.render_main_view()
    
    def render_sidebar(self):
        """Render sidebar controls"""
        with st.sidebar:
            st.header("🔧 Controls")
            
            # Refresh button
            if st.button("🔄 Refresh Documents", type="primary"):
                self.refresh_documents()
            
            # Last refresh time
            if st.session_state.get('last_refresh'):
                last_refresh = st.session_state.last_refresh
                st.write(f"Last refresh: {UIUtils.format_time_ago(last_refresh)}")
            
            st.divider()
            
            # Document filters
            st.subheader("📋 Filters")
            
            # Document type filter
            doc_types = ["All", "manual", "diagram", "sparepartslist", "spreadsheet", "plain_document"]
            st.session_state.filter_type = st.selectbox(
                "Document Type",
                doc_types,
                index=doc_types.index(st.session_state.get('filter_type', 'All'))
            )
            
            # Show processed/unprocessed toggle
            show_processed = st.checkbox(
                "Show Processed Documents",
                value=st.session_state.get('show_processed', False)
            )
            st.session_state.show_processed = show_processed
            
            st.divider()
            
            # Statistics
            self.render_sidebar_stats()
            
            st.divider()
            
            # Processing tips
            MetadataEditor.render_processing_tips()
    
    def render_sidebar_stats(self):
        """Render sidebar statistics"""
        st.subheader("📊 Statistics")
        
        try:
            stats = self.doc_manager.get_statistics()
            if stats:
                progress = stats.get('processing_progress', {})
                
                st.metric("Total Documents", progress.get('total_documents', 0))
                st.metric("Processed", progress.get('processed_documents', 0))
                st.metric("Unprocessed", progress.get('unprocessed_documents', 0))
                
                # Progress bar
                total = progress.get('total_documents', 0)
                processed = progress.get('processed_documents', 0)
                if total > 0:
                    progress_pct = processed / total
                    st.progress(progress_pct, text=f"{progress_pct*100:.1f}% Complete")
        except Exception as e:
            st.error(f"Error loading statistics: {e}")
    
    def render_main_view(self):
        """Render main document processing view"""
        # Create tabs for different sections
        tab1, tab2, tab3 = st.tabs(["📄 Documents", "🗃️ Cache Management", "📊 Statistics"])
        
        with tab1:
            self.render_documents_tab()
        
        with tab2:
            self.render_cache_management_tab()
        
        with tab3:
            self.render_statistics_tab()
    
    def render_documents_tab(self):
        """Render the documents tab"""
        # Document list section
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.header("📄 Documents")
            self.render_document_section()
        
        with col2:
            st.header("⚙️ Processing")
            self.render_processing_section()
    
    def render_cache_management_tab(self):
        """Render the cache management tab"""
        try:
            # Get all documents for cache management
            all_documents = (
                self.doc_manager.get_processed_documents() + 
                self.doc_manager.get_unprocessed_documents()
            )
            
            # Initialize cache manager
            cache_manager = CacheManagerComponent(self.doc_manager.result_cache)
            
            # Render cache management interface
            cache_manager.render_full_cache_manager(all_documents)
            
        except Exception as e:
            st.error(f"Error loading cache management: {e}")
    
    def render_statistics_tab(self):
        """Render the statistics tab"""
        st.header("📊 System Statistics")
        
        try:
            # Get statistics
            stats = self.doc_manager.get_statistics()
            
            if stats:
                # Processing progress
                st.subheader("📈 Processing Progress")
                progress = stats.get('processing_progress', {})
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Documents", progress.get('total_documents', 0))
                
                with col2:
                    st.metric("Processed", progress.get('processed_documents', 0))
                
                with col3:
                    st.metric("Unprocessed", progress.get('unprocessed_documents', 0))
                
                with col4:
                    total = progress.get('total_documents', 0)
                    processed = progress.get('processed_documents', 0)
                    if total > 0:
                        progress_pct = (processed / total) * 100
                        st.metric("Progress", f"{progress_pct:.1f}%")
                
                # Progress bar
                if total > 0:
                    st.progress(processed / total, text=f"Processing Progress: {progress_pct:.1f}%")
                
                # Document type distribution
                type_dist = stats.get('document_type_distribution', {})
                if type_dist:
                    st.subheader("📋 Document Types")
                    type_cols = st.columns(len(type_dist))
                    for i, (doc_type, count) in enumerate(type_dist.items()):
                        with type_cols[i]:
                            st.metric(doc_type.title(), count)
                
                # Cache statistics
                st.subheader("📦 Cache Statistics")
                cache_stats = self.doc_manager.result_cache.get_cache_stats()
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Cache Entries", cache_stats.get('total_entries', 0))
                
                with col2:
                    st.metric("Cache Size", f"{cache_stats.get('cache_size_mb', 0):.1f} MB")
                
                with col3:
                    st.metric("Max Age", f"{cache_stats.get('max_age_hours', 0)} hours")
            
            else:
                st.info("No statistics available. Try refreshing the document list.")
        
        except Exception as e:
            st.error(f"Error loading statistics: {e}")
    
    def render_document_section(self):
        """Render document list and management section"""
        # Get documents based on filters
        try:
            if st.session_state.show_processed:
                documents = self.doc_manager.get_processed_documents()
                st.subheader(f"Processed Documents ({len(documents)})")
            else:
                documents = self.doc_manager.get_unprocessed_documents()
                st.subheader(f"Unprocessed Documents ({len(documents)})")
            
            # Filter by type
            if st.session_state.filter_type != "All":
                # Note: This would need document type to be stored in the Document model
                # For now, we'll show all documents
                pass
            
            # Document summary
            DocumentListComponent.render_document_summary(documents)
            
            # Document table
            if documents:
                selected_docs = DocumentListComponent.render_document_table(
                    documents,
                    key="main_document_table"
                )
                
                # Update session state
                st.session_state.selected_docs = selected_docs
                
                # Batch actions
                if selected_docs:
                    st.divider()
                    batch_changes = DocumentListComponent.render_batch_actions(selected_docs)
                    if batch_changes:
                        self.apply_batch_changes(batch_changes)
            
            else:
                st.info("No documents found. Try refreshing or adjusting filters.")
        
        except Exception as e:
            st.error(f"Error loading documents: {e}")
    
    def render_processing_section(self):
        """Render processing controls and queue"""
        selected_docs = st.session_state.get('selected_docs', [])
        
        if not selected_docs:
            st.info("Select documents to process")
            return
        
        # Show selected documents count
        st.write(f"**Selected:** {len(selected_docs)} documents")
        
        # Validation
        errors = UIUtils.validate_document_selection(selected_docs)
        if errors:
            st.error("Please fix the following issues:")
            for error in errors:
                st.markdown(f"• {error}")
            return
        
        # Processing preview
        DocumentListComponent.render_processing_preview(selected_docs)
        
        # Process button
        if st.button("🚀 Process Selected Documents", type="primary"):
            self.start_processing(selected_docs)
        
        # Recent processing results
        if st.session_state.get('processing_results'):
            st.subheader("Recent Results")
            results = st.session_state.processing_results[-5:]  # Last 5 results
            
            for result in results:
                status_icon = "✅" if result.get('success', False) else "❌"
                document_name = result.get('document', 'Unknown')
                
                with st.expander(f"{status_icon} {document_name}"):
                    if result.get('success', False):
                        st.success("Processing completed successfully")
                    else:
                        st.error(f"Error: {result.get('error', 'Unknown error')}")
    
    def render_processing_view(self):
        """Render active processing view"""
        st.header("🔄 Processing Documents")
        
        # Processing status
        ProgressTracker.render_processing_status(
            status="processing",
            current_document=st.session_state.get('current_document'),
            elapsed_time=self.get_processing_elapsed_time()
        )
        
        # Progress tracking
        processing_results = st.session_state.get('processing_results', [])
        total_docs = st.session_state.get('total_processing_docs', 0)
        
        ProgressTracker.render_batch_progress(
            processing_results,
            len(processing_results),
            total_docs
        )
        
        # Processing log
        log_entries = st.session_state.get('processing_log', [])
        ProgressTracker.render_real_time_log(log_entries)
        
        # Cancel button
        if st.button("⏹️ Cancel Processing", type="secondary"):
            self.cancel_processing()
    
    def refresh_documents(self):
        """Refresh document list"""
        with st.spinner("Refreshing documents..."):
            try:
                # Clear cached documents
                if 'documents' in st.session_state:
                    del st.session_state['documents']
                
                # Update timestamp
                st.session_state.last_refresh = datetime.now()
                
                UIUtils.add_log_entry("INFO", "Document list refreshed")
                st.success("Documents refreshed successfully!")
                
            except Exception as e:
                st.error(f"Error refreshing documents: {e}")
                UIUtils.add_log_entry("ERROR", f"Failed to refresh documents: {e}")
    
    def apply_batch_changes(self, changes: Dict):
        """Apply batch changes to selected documents"""
        try:
            selected_docs = st.session_state.get('selected_docs', [])
            
            for doc in selected_docs:
                if changes.get('machines'):
                    doc['Machine Names'] = ', '.join(changes['machines'])
                
                if changes.get('document_type'):
                    doc['Document Type'] = changes['document_type']
            
            # Update session state
            st.session_state.selected_docs = selected_docs
            
            UIUtils.add_log_entry("INFO", f"Applied batch changes to {len(selected_docs)} documents")
            st.success("Batch changes applied!")
            
        except Exception as e:
            st.error(f"Error applying batch changes: {e}")
            UIUtils.add_log_entry("ERROR", f"Failed to apply batch changes: {e}")
    
    def start_processing(self, selected_docs: List[Dict]):
        """Start processing selected documents"""
        try:
            # Validate documents
            errors = UIUtils.validate_document_selection(selected_docs)
            if errors:
                st.error("Cannot start processing with validation errors")
                return
            
            # Prepare documents for processing
            prepared_docs = UIUtils.prepare_documents_for_processing(selected_docs)
            
            # Initialize processing state
            st.session_state.processing_active = True
            st.session_state.processing_results = []
            st.session_state.processing_start_time = time.time()
            st.session_state.total_processing_docs = len(prepared_docs)
            
            UIUtils.add_log_entry("INFO", f"Started processing {len(prepared_docs)} documents")
            
            # Process documents using the actual document processing pipeline
            self.process_documents_real(prepared_docs)
            
        except Exception as e:
            st.error(f"Error starting processing: {e}")
            UIUtils.add_log_entry("ERROR", f"Failed to start processing: {e}")
    
    def process_documents_real(self, documents: List[Dict]):
        """Process documents using the actual document processing pipeline"""
        try:
            # Convert prepared documents to Document objects and metadata
            documents_with_metadata = []
            
            for doc_data in documents:
                doc_info = doc_data['document']
                metadata = doc_data['metadata']
                
                # Create Document object from document info
                document = Document(
                    s3_key=doc_info['s3_key'],
                    filename=doc_info['filename'],
                    file_size=doc_info['file_size'],
                    last_modified=datetime.now(),  # We'll use current time as placeholder
                    etag=doc_info['etag']
                )
                
                documents_with_metadata.append({
                    'document': document,
                    'metadata': metadata
                })
            
            # Process documents in batches for better UI responsiveness
            results = []
            total_docs = len(documents_with_metadata)
            
            UIUtils.add_log_entry("INFO", f"Starting processing of {total_docs} documents")
            
            # Create progress placeholder
            progress_placeholder = st.empty()
            
            for i, doc_with_metadata in enumerate(documents_with_metadata):
                document = doc_with_metadata['document']
                metadata = doc_with_metadata['metadata']
                
                # Update current document in session state
                st.session_state.current_document = document.filename
                
                # Update progress
                progress_placeholder.progress(
                    (i + 1) / total_docs, 
                    text=f"Processing {document.filename} ({i + 1}/{total_docs})"
                )
                
                UIUtils.add_log_entry("INFO", f"Processing {document.filename}")
                
                # Process the document
                start_time = time.time()
                try:
                    UIUtils.add_log_entry("INFO", f"Starting document processing for {document.filename}")
                    result = self.doc_manager.process_document(document, metadata)
                    processing_time = time.time() - start_time
                    UIUtils.add_log_entry("INFO", f"Document processing completed for {document.filename} in {processing_time:.1f}s")
                    
                    if result['success']:
                        UIUtils.add_log_entry("SUCCESS", f"Successfully processed {document.filename}")
                        results.append({
                            'success': True,
                            'document': document.filename,
                            'processing_time': processing_time,
                            'processor_used': result.get('processor_used', 'Unknown'),
                            'webhook_result': result.get('webhook_result')
                        })
                    else:
                        UIUtils.add_log_entry("ERROR", f"Failed to process {document.filename}: {result.get('error', 'Unknown error')}")
                        results.append({
                            'success': False,
                            'document': document.filename,
                            'processing_time': processing_time,
                            'error': result.get('error', 'Unknown error')
                        })
                        
                except Exception as e:
                    processing_time = time.time() - start_time
                    error_msg = f"Exception processing {document.filename}: {str(e)}"
                    UIUtils.add_log_entry("ERROR", error_msg)
                    results.append({
                        'success': False,
                        'document': document.filename,
                        'processing_time': processing_time,
                        'error': str(e)
                    })
                
                # Update session state with current results
                st.session_state.processing_results = results
                
                # Brief pause to allow UI updates
                time.sleep(0.1)
            
            # Processing complete
            st.session_state.processing_active = False
            st.session_state.current_document = None
            
            # Clear progress placeholder
            progress_placeholder.empty()
            
            # Show completion summary
            successful_count = sum(1 for r in results if r['success'])
            failed_count = len(results) - successful_count
            
            UIUtils.add_log_entry("INFO", f"Processing complete! {successful_count} successful, {failed_count} failed")
            
            if failed_count == 0:
                st.success(f"Processing completed successfully! All {successful_count} documents processed.")
            else:
                st.warning(f"Processing completed with {failed_count} failures out of {len(results)} documents.")
            
            # Show detailed results
            st.subheader("Processing Results")
            for result in results:
                status_icon = "✅" if result['success'] else "❌"
                with st.expander(f"{status_icon} {result['document']}"):
                    if result['success']:
                        st.success("Processing completed successfully")
                        st.write(f"**Processing time:** {result['processing_time']:.1f}s")
                        st.write(f"**Processor used:** {result.get('processor_used', 'Unknown')}")
                        
                        webhook_result = result.get('webhook_result')
                        if webhook_result:
                            if webhook_result.get('success'):
                                st.info("✅ Webhook sent successfully to n8n")
                            else:
                                st.error(f"❌ Webhook failed: {webhook_result.get('error', 'Unknown error')}")
                        else:
                            st.info("ℹ️ No webhook configured")
                    else:
                        st.error(f"Processing failed: {result.get('error', 'Unknown error')}")
                        st.write(f"**Processing time:** {result['processing_time']:.1f}s")
                        
        except Exception as e:
            st.session_state.processing_active = False
            st.session_state.current_document = None
            error_msg = f"Error during document processing: {str(e)}"
            UIUtils.add_log_entry("ERROR", error_msg)
            st.error(error_msg)
            
            # Still update results with the error
            st.session_state.processing_results = [{
                'success': False,
                'document': 'Processing Error',
                'error': str(e)
            }]
    
    def simulate_processing(self, documents: List[Dict]):
        """Deprecated: Use process_documents_real instead"""
        # This method is deprecated - redirecting to real processing
        self.process_documents_real(documents)
    
    def cancel_processing(self):
        """Cancel active processing"""
        st.session_state.processing_active = False
        st.session_state.processing_cancelled = True
        
        UIUtils.add_log_entry("WARNING", "Processing cancelled by user")
        st.warning("Processing cancelled")
    
    def get_processing_elapsed_time(self) -> Optional[float]:
        """Get elapsed processing time"""
        start_time = st.session_state.get('processing_start_time')
        if start_time:
            return time.time() - start_time
        return None

def main():
    """Main entry point"""
    app = DocumentProcessorApp()
    app.run()

if __name__ == "__main__":
    main()