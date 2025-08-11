import streamlit as st
from typing import List, Dict, Optional

class MetadataEditor:
    """Component for editing document metadata"""
    
    @staticmethod
    def render_document_type_selector(
        current_type: str = "manual",
        key: str = "doc_type",
        help_text: str = "Select the document type for payload formatting"
    ) -> str:
        """Render document type selector"""
        return st.selectbox(
            "Document Type",
            options=["manual", "diagram", "sparepartslist", "spreadsheet", "plain_document"],
            index=["manual", "diagram", "sparepartslist", "spreadsheet", "plain_document"].index(current_type),
            key=key,
            help=help_text
        )
    
    @staticmethod
    def render_processing_method_selector(
        current_method: str = "markdown",
        key: str = "processing_method",
        help_text: str = "Select the processing method"
    ) -> str:
        """Render processing method selector"""
        return st.selectbox(
            "Processing Method",
            options=["markdown", "plain_text"],
            index=["markdown", "plain_text"].index(current_method),
            key=key,
            help=help_text,
            format_func=lambda x: "DataLabs (Markdown + Images)" if x == "markdown" else "PyMuPDF (Plain Text)"
        )
    
    @staticmethod
    def render_machine_names_input(
        current_names: List[str] = None,
        key: str = "machine_names",
        help_text: str = "Enter machine names separated by commas"
    ) -> List[str]:
        """Render machine names input"""
        current_text = ', '.join(current_names) if current_names else ""
        
        machines_text = st.text_input(
            "Machine Names",
            value=current_text,
            key=key,
            help=help_text,
            placeholder="e.g., HPM-Ironer1, HPM-Ironer2, CSP"
        )
        
        # Parse and clean machine names
        if machines_text.strip():
            machines = [name.strip() for name in machines_text.split(',')]
            machines = [name for name in machines if name]  # Remove empty strings
            return machines
        return []
    
    @staticmethod
    def render_metadata_form(
        document_filename: str,
        current_metadata: Dict = None
    ) -> Optional[Dict]:
        """Render a complete metadata editing form"""
        current_metadata = current_metadata or {}
        
        st.subheader(f"Edit Metadata: {document_filename}")
        
        with st.form(f"metadata_form_{document_filename}"):
            # Document type
            doc_type = MetadataEditor.render_document_type_selector(
                current_type=current_metadata.get('document_type', 'manual'),
                key=f"type_{document_filename}",
                help_text="Select the document type (affects payload structure)"
            )
            
            # Processing method
            processing_method = MetadataEditor.render_processing_method_selector(
                current_method=current_metadata.get('processing_method', 'markdown'),
                key=f"method_{document_filename}",
                help_text="Select the processing method independently of document type"
            )
            
            # Machine names
            machines = MetadataEditor.render_machine_names_input(
                current_names=current_metadata.get('machine_names', []),
                key=f"machines_{document_filename}",
                help_text="Enter all machine names associated with this document"
            )
            
            # Processing options preview
            st.subheader("Processing Configuration")
            
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Document Type:**")
                st.code(doc_type)
                st.caption("This determines the payload structure and document-specific fields")
            
            with col2:
                st.write("**Processing Method:**")
                if processing_method == "markdown":
                    st.code("DataLabs (Markdown + Images)")
                    st.caption("Rich markdown formatting with image descriptions")
                else:
                    st.code("PyMuPDF (Plain Text)")
                    st.caption("Simple text extraction with page identifiers")
            
            priority = st.selectbox(
                "Processing Priority",
                options=["normal", "high", "low"],
                index=0,
                help="Set processing priority"
            )
            
            # Submit button
            if st.form_submit_button("Update Metadata", type="primary"):
                # Validate
                errors = []
                if not machines:
                    errors.append("At least one machine name is required")
                if not doc_type:
                    errors.append("Document type is required")
                
                if errors:
                    for error in errors:
                        st.error(error)
                    return None
                
                return {
                    'document_type': doc_type,
                    'processing_method': processing_method,
                    'machine_names': machines,
                    'priority': priority
                }
        
        return None
    
    @staticmethod
    def render_batch_metadata_editor(selected_count: int) -> Optional[Dict]:
        """Render batch metadata editor"""
        if selected_count == 0:
            return None
        
        st.subheader(f"Batch Edit Metadata ({selected_count} documents)")
        
        with st.form("batch_metadata_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                # Common document type
                apply_type = st.checkbox("Apply Document Type to All")
                doc_type = st.selectbox(
                    "Document Type",
                    options=["manual", "diagram", "sparepartslist", "spreadsheet", "plain_document"],
                    disabled=not apply_type,
                    help="Document type affects payload structure"
                )
                
                # Common processing method
                apply_method = st.checkbox("Apply Processing Method to All")
                processing_method = st.selectbox(
                    "Processing Method",
                    options=["markdown", "plain_text"],
                    disabled=not apply_method,
                    help="Processing method is independent of document type",
                    format_func=lambda x: "DataLabs (Markdown + Images)" if x == "markdown" else "PyMuPDF (Plain Text)"
                )
                
                # Common machines
                apply_machines = st.checkbox("Apply Machine Names to All")
                machines_text = st.text_input(
                    "Machine Names",
                    placeholder="Enter machine names (comma-separated)",
                    disabled=not apply_machines
                )
            
            with col2:
                # Processing information
                st.info("Choose processing method independently of document type:")
                st.write("• **Markdown** → DataLabs (Rich formatting + Images)")
                st.write("• **Plain Text** → PyMuPDF (Simple text extraction)")
                st.caption("Document type only affects payload structure, not processing method")
                
                apply_priority = st.checkbox("Apply Priority Setting")
                priority = st.selectbox(
                    "Priority",
                    options=["normal", "high", "low"],
                    disabled=not apply_priority
                )
            
            # Submit button
            if st.form_submit_button("Apply to All Selected", type="primary"):
                result = {}
                
                if apply_type:
                    result['document_type'] = doc_type
                
                if apply_method:
                    result['processing_method'] = processing_method
                
                if apply_machines and machines_text.strip():
                    machines = [name.strip() for name in machines_text.split(',')]
                    machines = [name for name in machines if name]
                    result['machine_names'] = machines
                
                if apply_priority:
                    result['priority'] = priority
                
                return result if result else None
        
        return None
    
    @staticmethod
    def render_metadata_preview(metadata: Dict) -> None:
        """Render metadata preview"""
        st.subheader("Metadata Preview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Document Type:**")
            st.code(metadata.get('document_type', 'Not set'))
            
            st.write("**Machine Names:**")
            machines = metadata.get('machine_names', [])
            if machines:
                for machine in machines:
                    st.write(f"• {machine}")
            else:
                st.write("None set")
        
        with col2:
            st.write("**Processing Method:**")
            processing_method = metadata.get('processing_method', 'markdown')
            if processing_method == 'markdown':
                st.code("DataLabs (Markdown + Images)")
            else:
                st.code("PyMuPDF (Plain Text)")
            
            st.write(f"**Priority:** {metadata.get('priority', 'normal')}")
    
    @staticmethod
    def validate_metadata(metadata: Dict) -> List[str]:
        """Validate metadata and return list of errors"""
        errors = []
        
        if not metadata.get('document_type'):
            errors.append("Document type is required")
        
        if not metadata.get('machine_names'):
            errors.append("At least one machine name is required")
        elif not isinstance(metadata['machine_names'], list):
            errors.append("Machine names must be a list")
        elif len(metadata['machine_names']) == 0:
            errors.append("At least one machine name is required")
        
        # Validate document type
        valid_types = ["manual", "diagram", "sparepartslist", "spreadsheet", "plain_document"]
        if metadata.get('document_type') not in valid_types:
            errors.append(f"Document type must be one of: {', '.join(valid_types)}")
        
        # Validate processing method
        valid_methods = ["markdown", "plain_text"]
        if metadata.get('processing_method') not in valid_methods:
            errors.append(f"Processing method must be one of: {', '.join(valid_methods)}")
        
        return errors
    
    @staticmethod
    def render_validation_errors(errors: List[str]) -> None:
        """Render validation errors"""
        if errors:
            st.error("Please fix the following errors:")
            for error in errors:
                st.write(f"• {error}")
    
    @staticmethod
    def render_processing_tips() -> None:
        """Render processing tips and guidance"""
        with st.expander("📋 Processing Tips"):
            st.write("""
            **Document Types (affects payload structure):**
            - **Manual**: Operating manuals, instruction guides
            - **Sparepartslist**: Parts lists, component catalogs  
            - **Spreadsheet**: Excel files, CSV data files
            - **Diagram**: Wiring diagrams, schematics, technical drawings
            - **Plain_document**: Simple documents, small files
            
            **Processing Methods (independent of document type):**
            - **Markdown (DataLabs)**: Rich markdown formatting with image descriptions
              → Best for detailed documents needing rich formatting
            - **Plain Text (PyMuPDF)**: Simple text extraction with page identifiers
              → Fast processing for simple text extraction
            
            **Machine Names:**
            - Use the exact machine names as they appear in your system
            - Separate multiple machines with commas
            - Examples: `HPM-Ironer1`, `CSP`, `Feeder1-Feeder2`
            
            **Configuration:**
            - **Document Type**: Determines payload structure and document-specific fields
            - **Processing Method**: Determines how the document is processed (DataLabs vs PyMuPDF)
            - **Priority**: High priority documents are processed first
            """)
    
    @staticmethod
    def render_recent_metadata() -> None:
        """Render recently used metadata for quick selection"""
        if 'recent_metadata' not in st.session_state:
            st.session_state.recent_metadata = []
        
        if st.session_state.recent_metadata:
            st.subheader("Recent Metadata")
            
            for i, metadata in enumerate(st.session_state.recent_metadata[:5]):  # Show last 5
                with st.expander(f"Template {i+1}: {metadata.get('document_type', 'Unknown')}"):
                    st.write(f"**Type:** {metadata.get('document_type')}")
                    st.write(f"**Machines:** {', '.join(metadata.get('machine_names', []))}")
                    if st.button(f"Use Template {i+1}", key=f"template_{i}"):
                        return metadata
        
        return None