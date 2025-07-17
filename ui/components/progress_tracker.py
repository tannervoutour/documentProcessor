import streamlit as st
from typing import Dict, List, Optional
from datetime import datetime
import time

class ProgressTracker:
    """Component for tracking document processing progress"""
    
    @staticmethod
    def render_progress_bar(current: int, total: int, label: str = "Progress") -> None:
        """Render a progress bar"""
        if total > 0:
            progress = current / total
            st.progress(progress, text=f"{label}: {current}/{total} ({progress*100:.1f}%)")
        else:
            st.progress(0, text=f"{label}: 0/0")
    
    @staticmethod
    def render_processing_status(
        status: str,
        current_document: Optional[str] = None,
        elapsed_time: Optional[float] = None
    ) -> None:
        """Render current processing status"""
        status_colors = {
            "pending": "🟡",
            "processing": "🔄",
            "completed": "✅",
            "failed": "❌",
            "cancelled": "⏹️"
        }
        
        color = status_colors.get(status, "⚪")
        st.write(f"{color} **Status:** {status.title()}")
        
        if current_document:
            st.write(f"📄 **Current Document:** {current_document}")
        
        if elapsed_time:
            st.write(f"⏱️ **Elapsed Time:** {elapsed_time:.1f}s")
    
    @staticmethod
    def render_batch_progress(
        batch_results: List[Dict],
        current_index: int = 0,
        total_count: int = 0
    ) -> None:
        """Render batch processing progress"""
        st.subheader("Batch Processing Progress")
        
        if total_count > 0:
            ProgressTracker.render_progress_bar(current_index, total_count, "Documents Processed")
        
        # Results summary
        if batch_results:
            completed = sum(1 for r in batch_results if r.get('success', False))
            failed = sum(1 for r in batch_results if not r.get('success', False))
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Completed", completed, delta=None)
            with col2:
                st.metric("Failed", failed, delta=None)
            with col3:
                st.metric("Remaining", total_count - len(batch_results))
        
        # Recent results
        if batch_results:
            st.subheader("Recent Results")
            
            # Show last 5 results
            for result in batch_results[-5:]:
                status_icon = "✅" if result.get('success', False) else "❌"
                document_name = result.get('document', 'Unknown')
                
                with st.expander(f"{status_icon} {document_name}"):
                    if result.get('success', False):
                        st.success("Processing completed successfully")
                        if 'processing_time' in result:
                            st.write(f"Processing time: {result['processing_time']:.1f}s")
                    else:
                        st.error(f"Processing failed: {result.get('error', 'Unknown error')}")
    
    @staticmethod
    def render_processing_queue(queue: List[Dict]) -> None:
        """Render processing queue"""
        st.subheader(f"Processing Queue ({len(queue)} documents)")
        
        if not queue:
            st.info("No documents in queue")
            return
        
        # Group by document type
        by_type = {}
        for doc in queue:
            doc_type = doc.get('document_type', 'unknown')
            if doc_type not in by_type:
                by_type[doc_type] = []
            by_type[doc_type].append(doc)
        
        # Display queue by type
        for doc_type, docs in by_type.items():
            with st.expander(f"{doc_type.title()} ({len(docs)} documents)"):
                for doc in docs:
                    st.write(f"📄 **{doc['filename']}**")
                    st.write(f"   Machines: {', '.join(doc.get('machine_names', []))}")
                    st.write(f"   Size: {doc.get('file_size', 0) / (1024*1024):.1f} MB")
                    st.write("")
    
    @staticmethod
    def render_processing_metrics(metrics: Dict) -> None:
        """Render processing performance metrics"""
        st.subheader("Processing Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Documents",
                metrics.get('total_documents', 0)
            )
        
        with col2:
            st.metric(
                "Success Rate",
                f"{metrics.get('success_rate', 0):.1f}%"
            )
        
        with col3:
            avg_time = metrics.get('avg_processing_time', 0)
            st.metric(
                "Avg Processing Time",
                f"{avg_time:.1f}s"
            )
        
        with col4:
            st.metric(
                "Total Processing Time",
                f"{metrics.get('total_processing_time', 0):.1f}s"
            )
    
    @staticmethod
    def render_real_time_log(log_entries: List[Dict], max_entries: int = 10) -> None:
        """Render real-time processing log"""
        st.subheader("Processing Log")
        
        if not log_entries:
            st.info("No log entries yet")
            return
        
        # Create a container for auto-scrolling
        log_container = st.container()
        
        with log_container:
            # Show recent entries (newest first)
            for entry in log_entries[-max_entries:]:
                timestamp = entry.get('timestamp', datetime.now())
                level = entry.get('level', 'INFO')
                message = entry.get('message', '')
                
                # Format timestamp
                time_str = timestamp.strftime("%H:%M:%S") if isinstance(timestamp, datetime) else str(timestamp)
                
                # Color code by level
                if level == 'ERROR':
                    st.error(f"[{time_str}] {message}")
                elif level == 'WARNING':
                    st.warning(f"[{time_str}] {message}")
                elif level == 'SUCCESS':
                    st.success(f"[{time_str}] {message}")
                else:
                    st.info(f"[{time_str}] {message}")
    
    @staticmethod
    def render_cancellation_controls(processing_active: bool) -> bool:
        """Render cancellation controls"""
        if processing_active:
            st.subheader("Processing Controls")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("⏸️ Pause Processing", type="secondary"):
                    st.session_state.processing_paused = True
                    return True
            
            with col2:
                if st.button("⏹️ Cancel Processing", type="secondary"):
                    st.session_state.processing_cancelled = True
                    return True
        
        return False
    
    @staticmethod
    def render_completion_summary(results: List[Dict], total_time: float) -> None:
        """Render processing completion summary"""
        st.subheader("Processing Complete!")
        
        successful = [r for r in results if r.get('success', False)]
        failed = [r for r in results if not r.get('success', False)]
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Documents", len(results))
        
        with col2:
            st.metric("Successful", len(successful))
        
        with col3:
            st.metric("Failed", len(failed))
        
        # Performance metrics
        st.write(f"**Total Processing Time:** {total_time:.1f} seconds")
        if results:
            avg_time = total_time / len(results)
            st.write(f"**Average Time per Document:** {avg_time:.1f} seconds")
        
        # Success details
        if successful:
            with st.expander(f"✅ Successful Documents ({len(successful)})"):
                for result in successful:
                    st.write(f"• {result.get('document', 'Unknown')}")
        
        # Failure details
        if failed:
            with st.expander(f"❌ Failed Documents ({len(failed)})"):
                for result in failed:
                    st.write(f"• **{result.get('document', 'Unknown')}**: {result.get('error', 'Unknown error')}")
    
    @staticmethod
    def initialize_session_state():
        """Initialize session state for progress tracking"""
        if 'processing_active' not in st.session_state:
            st.session_state.processing_active = False
        
        if 'processing_paused' not in st.session_state:
            st.session_state.processing_paused = False
        
        if 'processing_cancelled' not in st.session_state:
            st.session_state.processing_cancelled = False
        
        if 'batch_results' not in st.session_state:
            st.session_state.batch_results = []
        
        if 'processing_log' not in st.session_state:
            st.session_state.processing_log = []
        
        if 'processing_start_time' not in st.session_state:
            st.session_state.processing_start_time = None