import streamlit as st
import pandas as pd
from typing import List, Dict, Optional
from models.document import Document

class DocumentListComponent:
    """Component for displaying and editing document lists"""
    
    @staticmethod
    def render_document_table(documents: List[Document], key: str = "document_table") -> List[Dict]:
        """
        Render an editable document table
        Returns list of selected documents with metadata
        """
        if not documents:
            st.info("No documents found. Click 'Refresh Document List' to load documents.")
            return []
        
        # Convert documents to DataFrame
        df_data = []
        for doc in documents:
            df_data.append({
                'Select': False,
                'Filename': doc.filename,
                'Size (MB)': round(doc.file_size / (1024 * 1024), 2),
                'Last Modified': doc.last_modified,
                'Machine Names': '',
                'Document Type': 'manual',
                'Processing Method': 'markdown',
                'Status': doc.processing_status,
                's3_key': doc.s3_key,
                'file_id': doc.file_id,
                'etag': doc.etag
            })
        
        df = pd.DataFrame(df_data)
        
        # Configure column display
        column_config = {
            "Select": st.column_config.CheckboxColumn(
                "Select",
                help="Select documents to process",
                width="small"
            ),
            "Filename": st.column_config.TextColumn(
                "Filename",
                help="Document filename",
                disabled=True,
                width="large"
            ),
            "Size (MB)": st.column_config.NumberColumn(
                "Size (MB)",
                help="File size in megabytes",
                disabled=True,
                width="small"
            ),
            "Last Modified": st.column_config.DatetimeColumn(
                "Last Modified",
                help="Last modification date",
                disabled=True,
                width="medium"
            ),
            "Machine Names": st.column_config.TextColumn(
                "Machine Names",
                help="Comma-separated list of machine names",
                width="large"
            ),
            "Document Type": st.column_config.SelectboxColumn(
                "Document Type",
                help="Type of document (affects payload structure)",
                options=["manual", "diagram", "sparepartslist", "spreadsheet", "plain_document"],
                required=True,
                width="medium"
            ),
            "Processing Method": st.column_config.SelectboxColumn(
                "Processing Method",
                help="Processing method (independent of document type)",
                options=["markdown", "plain_text"],
                required=True,
                width="medium",
                default="markdown"
            ),
            "Status": st.column_config.TextColumn(
                "Status",
                help="Processing status",
                disabled=True,
                width="small"
            ),
            "s3_key": st.column_config.TextColumn(
                "S3 Key",
                help="S3 object key",
                disabled=True,
                width="large"
            ),
            "file_id": st.column_config.TextColumn(
                "File ID",
                help="Unique file identifier",
                disabled=True,
                width="large"
            )
        }
        
        # Hide internal columns
        hidden_columns = ["s3_key", "file_id", "etag"]
        
        # Display editable dataframe
        edited_df = st.data_editor(
            df,
            column_config=column_config,
            hide_index=True,
            num_rows="fixed",
            key=key,
            use_container_width=True,
            column_order=["Select", "Filename", "Size (MB)", "Last Modified", "Machine Names", "Document Type", "Processing Method", "Status"]
        )
        
        # Return selected documents
        selected_docs = edited_df[edited_df['Select']].to_dict('records')
        return selected_docs
    
    @staticmethod
    def render_document_summary(documents: List[Document]) -> None:
        """Render document summary statistics"""
        if not documents:
            return
        
        # Calculate statistics
        total_docs = len(documents)
        total_size = sum(doc.file_size for doc in documents)
        total_size_mb = total_size / (1024 * 1024)
        
        # File type distribution
        file_types = {}
        for doc in documents:
            ext = doc.filename.split('.')[-1].lower() if '.' in doc.filename else 'unknown'
            file_types[ext] = file_types.get(ext, 0) + 1
        
        # Display metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Documents", total_docs)
        
        with col2:
            st.metric("Total Size", f"{total_size_mb:.1f} MB")
        
        with col3:
            # Show most common file type
            if file_types:
                most_common = max(file_types.items(), key=lambda x: x[1])
                st.metric("Most Common Type", f".{most_common[0]} ({most_common[1]})")
        
        # File type breakdown
        if file_types:
            st.write("**File Types:**")
            type_cols = st.columns(len(file_types))
            for i, (ext, count) in enumerate(file_types.items()):
                with type_cols[i]:
                    st.write(f"`.{ext}`: {count}")
    
    @staticmethod
    def render_batch_actions(selected_docs: List[Dict]) -> Optional[Dict]:
        """Render batch action controls"""
        if not selected_docs:
            return None
        
        st.subheader(f"Batch Actions ({len(selected_docs)} selected)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Batch machine names
            batch_machines = st.text_input(
                "Apply to All Selected",
                placeholder="Enter machine names (comma-separated)",
                help="Apply these machine names to all selected documents"
            )
        
        with col2:
            # Batch document type
            batch_type = st.selectbox(
                "Apply Document Type",
                options=["No Change", "manual", "diagram", "sparepartslist", "spreadsheet", "plain_document"],
                help="Apply this document type to all selected documents"
            )
        
        # Apply batch changes
        if st.button("Apply Batch Changes", type="secondary"):
            if batch_machines or batch_type != "No Change":
                return {
                    'machines': batch_machines.split(',') if batch_machines else None,
                    'document_type': batch_type if batch_type != "No Change" else None
                }
        
        return None
    
    @staticmethod
    def validate_selected_documents(selected_docs: List[Dict]) -> List[str]:
        """Validate selected documents for processing"""
        errors = []
        
        for doc in selected_docs:
            doc_errors = []
            
            if not doc.get('Machine Names', '').strip():
                doc_errors.append("Machine names required")
            
            if not doc.get('Document Type'):
                doc_errors.append("Document type required")
            
            if doc_errors:
                errors.append(f"**{doc['Filename']}**: {', '.join(doc_errors)}")
        
        return errors
    
    @staticmethod
    def render_processing_preview(selected_docs: List[Dict]) -> None:
        """Render a preview of documents to be processed"""
        if not selected_docs:
            return
        
        st.subheader("Processing Preview")
        
        # Group by document type
        by_type = {}
        for doc in selected_docs:
            doc_type = doc.get('Document Type', 'unknown')
            if doc_type not in by_type:
                by_type[doc_type] = []
            by_type[doc_type].append(doc)
        
        # Display grouped documents
        for doc_type, docs in by_type.items():
            with st.expander(f"{doc_type.title()} Documents ({len(docs)})"):
                for doc in docs:
                    machines = doc.get('Machine Names', '').strip()
                    st.write(f"• **{doc['Filename']}** → {machines}")
                    st.write(f"  Size: {doc['Size (MB)']} MB")
                    st.write("")