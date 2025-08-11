import streamlit as st
from typing import Dict, List, Optional
from datetime import datetime
import time

class UIUtils:
    """Utility functions for the Streamlit UI"""
    
    @staticmethod
    def init_session_state():
        """Initialize session state variables"""
        defaults = {
            'documents': [],
            'selected_docs': [],
            'processing_status': {},
            'processing_active': False,
            'processing_results': [],
            'processing_log': [],
            'last_refresh': None,
            'filter_type': 'All',
            'show_processed': False,
            'recent_metadata': []
        }
        
        for key, default_value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value
    
    @staticmethod
    def add_log_entry(level: str, message: str):
        """Add entry to processing log"""
        if 'processing_log' not in st.session_state:
            st.session_state.processing_log = []
        
        entry = {
            'timestamp': datetime.now(),
            'level': level,
            'message': message
        }
        
        st.session_state.processing_log.append(entry)
        
        # Keep only last 100 entries
        if len(st.session_state.processing_log) > 100:
            st.session_state.processing_log = st.session_state.processing_log[-100:]
    
    @staticmethod
    def save_recent_metadata(metadata: Dict):
        """Save metadata to recent list"""
        if 'recent_metadata' not in st.session_state:
            st.session_state.recent_metadata = []
        
        # Check if similar metadata already exists
        for existing in st.session_state.recent_metadata:
            if (existing.get('document_type') == metadata.get('document_type') and
                existing.get('processing_method') == metadata.get('processing_method') and
                existing.get('machine_names') == metadata.get('machine_names')):
                return  # Don't add duplicates
        
        # Add to front of list
        st.session_state.recent_metadata.insert(0, metadata)
        
        # Keep only last 10 entries
        if len(st.session_state.recent_metadata) > 10:
            st.session_state.recent_metadata = st.session_state.recent_metadata[:10]
    
    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """Format file size in human readable format"""
        if size_bytes == 0:
            return "0 B"
        
        units = ['B', 'KB', 'MB', 'GB']
        size = size_bytes
        unit_index = 0
        
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1
        
        return f"{size:.1f} {units[unit_index]}"
    
    @staticmethod
    def format_time_ago(timestamp: datetime) -> str:
        """Format timestamp as 'time ago' string"""
        if not timestamp:
            return "Unknown"
        
        if not isinstance(timestamp, datetime):
            return str(timestamp)
        
        now = datetime.now(timestamp.tzinfo) if timestamp.tzinfo else datetime.now()
        diff = now - timestamp
        
        if diff.days > 0:
            return f"{diff.days} day{'s' if diff.days != 1 else ''} ago"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        else:
            return "Just now"
    
    @staticmethod
    def show_success_message(message: str, duration: int = 5):
        """Show success message with auto-dismiss"""
        success_placeholder = st.empty()
        success_placeholder.success(message)
        time.sleep(duration)
        success_placeholder.empty()
    
    @staticmethod
    def show_error_message(message: str, duration: int = 10):
        """Show error message with auto-dismiss"""
        error_placeholder = st.empty()
        error_placeholder.error(message)
        time.sleep(duration)
        error_placeholder.empty()
    
    @staticmethod
    def create_download_link(content: str, filename: str, mime_type: str = "text/plain") -> str:
        """Create a download link for content"""
        import base64
        b64 = base64.b64encode(content.encode()).decode()
        href = f'<a href="data:{mime_type};base64,{b64}" download="{filename}">Download {filename}</a>'
        return href
    
    @staticmethod
    def apply_custom_css():
        """Apply custom CSS styling"""
        st.markdown("""
        <style>
        .main-header {
            font-size: 2.5rem;
            font-weight: bold;
            color: #1f77b4;
            margin-bottom: 1rem;
        }
        
        .metric-container {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem 0;
        }
        
        .status-pending {
            color: #ff9800;
        }
        
        .status-processing {
            color: #2196f3;
        }
        
        .status-completed {
            color: #4caf50;
        }
        
        .status-failed {
            color: #f44336;
        }
        
        .document-card {
            border: 1px solid #ddd;
            border-radius: 0.5rem;
            padding: 1rem;
            margin: 0.5rem 0;
            background-color: #fafafa;
        }
        
        .processing-log {
            background-color: #1e1e1e;
            color: #ffffff;
            padding: 1rem;
            border-radius: 0.5rem;
            font-family: monospace;
            max-height: 400px;
            overflow-y: auto;
        }
        </style>
        """, unsafe_allow_html=True)
    
    @staticmethod
    def render_document_card(document: Dict, show_metadata: bool = False):
        """Render a document card"""
        filename = document.get('filename', 'Unknown')
        size = document.get('file_size', 0)
        doc_type = document.get('document_type', 'Unknown')
        machines = document.get('machine_names', [])
        
        with st.container():
            st.markdown(f"""
            <div class="document-card">
                <h4>{filename}</h4>
                <p><strong>Size:</strong> {UIUtils.format_file_size(size)}</p>
                {f'<p><strong>Type:</strong> {doc_type}</p>' if show_metadata else ''}
                {f'<p><strong>Machines:</strong> {", ".join(machines)}</p>' if show_metadata and machines else ''}
            </div>
            """, unsafe_allow_html=True)
    
    @staticmethod
    def validate_document_selection(selected_docs: List[Dict]) -> List[str]:
        """Validate selected documents and return errors"""
        errors = []
        
        if not selected_docs:
            errors.append("No documents selected")
            return errors
        
        for doc in selected_docs:
            doc_errors = []
            
            # Check machine names
            machine_names = doc.get('Machine Names', '').strip()
            if not machine_names:
                doc_errors.append("Machine names required")
            
            # Check document type
            doc_type = doc.get('Document Type', '').strip()
            if not doc_type:
                doc_errors.append("Document type required")
            elif doc_type not in ['manual', 'diagram', 'sparepartslist', 'spreadsheet', 'plain_document']:
                doc_errors.append("Invalid document type")
            
            # Check processing method
            processing_method = doc.get('Processing Method', '').strip()
            if not processing_method:
                doc_errors.append("Processing method required")
            elif processing_method not in ['markdown', 'plain_text']:
                doc_errors.append("Invalid processing method")
            
            if doc_errors:
                errors.append(f"**{doc['Filename']}**: {', '.join(doc_errors)}")
        
        return errors
    
    @staticmethod
    def prepare_documents_for_processing(selected_docs: List[Dict]) -> List[Dict]:
        """Prepare selected documents for processing"""
        processed_docs = []
        
        for doc in selected_docs:
            machine_names = doc.get('Machine Names', '').strip()
            machines = [name.strip() for name in machine_names.split(',') if name.strip()]
            
            processed_doc = {
                'document': {
                    'file_id': doc.get('file_id'),
                    'filename': doc.get('Filename'),
                    's3_key': doc.get('s3_key'),
                    'file_size': doc.get('Size (MB)', 0) * 1024 * 1024,  # Convert back to bytes
                    'etag': doc.get('etag')
                },
                'metadata': {
                    'machine_names': machines,
                    'document_type': doc.get('Document Type'),
                    'processing_method': doc.get('Processing Method', 'markdown'),
                    'basic': False
                }
            }
            
            processed_docs.append(processed_doc)
        
        return processed_docs
    
    @staticmethod
    def render_stats_cards(stats: Dict):
        """Render statistics cards"""
        if not stats:
            return
        
        progress = stats.get('processing_progress', {})
        s3_stats = stats.get('s3_statistics', {})
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Documents",
                s3_stats.get('total_in_s3', 0)
            )
        
        with col2:
            st.metric(
                "Processed",
                progress.get('processed_documents', 0)
            )
        
        with col3:
            st.metric(
                "Unprocessed",
                progress.get('unprocessed_documents', 0)
            )
        
        with col4:
            completion = progress.get('completion_percentage', 0)
            st.metric(
                "Completion",
                f"{completion:.1f}%"
            )