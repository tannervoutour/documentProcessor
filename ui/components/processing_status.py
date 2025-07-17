"""
Processing status component for Phase 3 document processing.
"""

import streamlit as st
from typing import Dict, List, Optional
import json
from datetime import datetime


class ProcessingStatus:
    """Component for displaying document processing status and results"""
    
    @staticmethod
    def render_processing_dashboard(processing_stats: Dict) -> None:
        """Render processing dashboard with statistics"""
        st.subheader("📊 Processing Dashboard")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Documents",
                processing_stats.get('total_documents', 0),
                help="Total documents in S3"
            )
        
        with col2:
            st.metric(
                "Processed",
                processing_stats.get('processed_documents', 0),
                help="Documents successfully processed"
            )
        
        with col3:
            st.metric(
                "Failed",
                processing_stats.get('failed_documents', 0),
                help="Documents that failed processing"
            )
        
        with col4:
            completion_pct = processing_stats.get('completion_percentage', 0)
            st.metric(
                "Completion",
                f"{completion_pct:.1f}%",
                help="Processing completion percentage"
            )
        
        # Progress bar
        if processing_stats.get('total_documents', 0) > 0:
            progress = completion_pct / 100
            st.progress(progress, text=f"Processing Progress: {completion_pct:.1f}%")
    
    @staticmethod
    def render_processor_breakdown(processor_stats: Dict) -> None:
        """Render breakdown by processor type"""
        st.subheader("🔧 Processor Breakdown")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**DataLabs Processor**")
            datalabs_count = processor_stats.get('DataLabsProcessor', 0)
            st.metric("Manuals & Spreadsheets", datalabs_count)
            
        with col2:
            st.write("**PyMuPDF Processor**")
            pymupdf_count = processor_stats.get('PyMuPDFProcessor', 0)
            st.metric("Diagrams & Parts Lists", pymupdf_count)
    
    @staticmethod
    def render_document_processing_result(result: Dict) -> None:
        """Render processing result for a single document"""
        if not result:
            st.warning("No processing result available")
            return
        
        processing_info = result.get('processing_info', {})
        document_metadata = result.get('document_metadata', {})
        
        # Status indicator
        if processing_info.get('success'):
            st.success("✅ Processing Successful")
        else:
            st.error("❌ Processing Failed")
            if processing_info.get('error'):
                st.error(f"Error: {processing_info['error']}")
            return
        
        # Document information
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Document Information**")
            st.write(f"📄 **Filename:** {document_metadata.get('filename', 'Unknown')}")
            st.write(f"📁 **Type:** {document_metadata.get('document_type', 'Unknown')}")
            st.write(f"📄 **Pages:** {document_metadata.get('total_pages', 0)}")
            st.write(f"🔤 **Words:** {document_metadata.get('total_words', 0):,}")
        
        with col2:
            st.write("**Processing Details**")
            st.write(f"⚙️ **Processor:** {processing_info.get('processor', 'Unknown')}")
            st.write(f"✅ **Pages Processed:** {processing_info.get('pages_processed', 0)}")
            st.write(f"📝 **Pages with Content:** {processing_info.get('pages_with_content', 0)}")
            if processing_info.get('processing_time'):
                st.write(f"⏱️ **Processing Time:** {processing_info['processing_time']}")
        
        # Additional metadata for different processors
        if document_metadata.get('processing_method') == 'datalabs_markdown':
            st.write("**DataLabs Processing**")
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"🖼️ **Images:** {document_metadata.get('total_images', 0)}")
            with col2:
                st.write(f"📊 **Tables:** {document_metadata.get('total_tables', 0)}")
        
        elif document_metadata.get('processing_method') == 'pymupdf_text_extraction':
            st.write("**PyMuPDF Processing**")
            st.write(f"📄 **Format:** Simple text extraction with page identifiers")
    
    @staticmethod
    def render_page_preview(pages: List[Dict], max_pages: int = 3) -> None:
        """Render preview of processed pages"""
        if not pages:
            st.warning("No page content available")
            return
        
        st.subheader("📄 Page Preview")
        
        for i, page in enumerate(pages[:max_pages]):
            with st.expander(f"Page {page.get('page_number', i+1)} - {page.get('page_id', 'Unknown')}"):
                content = page.get('content', '')
                if content:
                    # Show first 500 characters
                    preview_content = content[:500]
                    if len(content) > 500:
                        preview_content += "..."
                    
                    st.text_area(
                        f"Content Preview (Page {page.get('page_number', i+1)})",
                        preview_content,
                        height=150,
                        disabled=True
                    )
                    
                    # Show metadata
                    metadata = page.get('metadata', {})
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.write(f"**Characters:** {metadata.get('character_count', 0)}")
                    with col2:
                        st.write(f"**Words:** {metadata.get('word_count', 0)}")
                    with col3:
                        st.write(f"**Has Content:** {'✅' if metadata.get('has_content') else '❌'}")
                    
                    # Show images/tables for DataLabs results
                    if metadata.get('image_count', 0) > 0:
                        st.write(f"🖼️ **Images:** {metadata['image_count']}")
                    if metadata.get('table_count', 0) > 0:
                        st.write(f"📊 **Tables:** {metadata['table_count']}")
                else:
                    st.write("No content available for this page")
        
        if len(pages) > max_pages:
            st.info(f"Showing {max_pages} of {len(pages)} pages. Full results available in processing data.")
    
    @staticmethod
    def render_processing_queue(queue_items: List[Dict]) -> None:
        """Render processing queue status"""
        if not queue_items:
            st.info("No documents in processing queue")
            return
        
        st.subheader("⏳ Processing Queue")
        
        for item in queue_items:
            with st.container():
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.write(f"📄 **{item.get('filename', 'Unknown')}**")
                    st.write(f"Type: {item.get('document_type', 'Unknown')}")
                
                with col2:
                    status = item.get('status', 'pending')
                    if status == 'processing':
                        st.write("🔄 Processing...")
                    elif status == 'pending':
                        st.write("⏳ Pending")
                    elif status == 'completed':
                        st.write("✅ Completed")
                    elif status == 'failed':
                        st.write("❌ Failed")
                
                with col3:
                    processor = item.get('processor', 'Unknown')
                    if 'DataLabs' in processor:
                        st.write("🔬 DataLabs")
                    elif 'PyMuPDF' in processor:
                        st.write("📄 PyMuPDF")
                    else:
                        st.write(f"⚙️ {processor}")
                
                st.divider()
    
    @staticmethod
    def render_error_summary(errors: List[Dict]) -> None:
        """Render processing errors summary"""
        if not errors:
            st.success("No processing errors! 🎉")
            return
        
        st.subheader("❌ Processing Errors")
        
        for error in errors:
            with st.expander(f"Error: {error.get('filename', 'Unknown')}"):
                st.error(f"**Error Message:** {error.get('error', 'Unknown error')}")
                st.write(f"**Document Type:** {error.get('document_type', 'Unknown')}")
                st.write(f"**Processor:** {error.get('processor', 'Unknown')}")
                st.write(f"**Timestamp:** {error.get('timestamp', 'Unknown')}")
                
                if error.get('details'):
                    st.write("**Error Details:**")
                    st.code(json.dumps(error['details'], indent=2))
    
    @staticmethod
    def render_processing_controls() -> Dict[str, bool]:
        """Render processing control buttons"""
        st.subheader("🎛️ Processing Controls")
        
        col1, col2, col3 = st.columns(3)
        
        controls = {}
        
        with col1:
            controls['start_processing'] = st.button(
                "▶️ Start Processing",
                help="Start processing selected documents",
                type="primary"
            )
        
        with col2:
            controls['pause_processing'] = st.button(
                "⏸️ Pause Processing",
                help="Pause current processing operations"
            )
        
        with col3:
            controls['clear_queue'] = st.button(
                "🗑️ Clear Queue",
                help="Clear all pending processing tasks"
            )
        
        return controls
    
    @staticmethod
    def render_export_options(processing_results: List[Dict]) -> None:
        """Render export options for processing results"""
        if not processing_results:
            return
        
        st.subheader("📤 Export Results")
        
        col1, col2 = st.columns(2)
        
        with col1:
            export_format = st.selectbox(
                "Export Format",
                options=["JSON", "CSV", "Excel"],
                help="Select format for exporting results"
            )
        
        with col2:
            include_content = st.checkbox(
                "Include Page Content",
                value=False,
                help="Include full page content in export (larger file size)"
            )
        
        if st.button("📥 Export Results"):
            # This would trigger export functionality
            st.success(f"Exporting {len(processing_results)} results in {export_format} format...")
            # Implementation would go here
            
    @staticmethod
    def render_processing_tips() -> None:
        """Render processing tips and best practices"""
        with st.expander("💡 Processing Tips"):
            st.write("""
            **Processing Performance:**
            - DataLabs processing takes longer but provides rich markdown with images
            - PyMuPDF processing is faster for simple text extraction
            - High priority documents are processed first
            
            **Document Types:**
            - Choose the correct document type for optimal processing
            - Manuals get full markdown treatment with image descriptions
            - Diagrams get simple text extraction with page identifiers
            
            **Troubleshooting:**
            - Check processing errors for failed documents
            - Verify document type matches content
            - Ensure machine names are correctly specified
            """)
    
    @staticmethod
    def format_processing_time(seconds: float) -> str:
        """Format processing time in human-readable format"""
        if seconds < 60:
            return f"{seconds:.1f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"