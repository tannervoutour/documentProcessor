"""
Cache management component for the document processor UI.
"""

import streamlit as st
from typing import List, Dict, Optional
from models.document import Document
from core.result_cache import ResultCache
import pandas as pd


class CacheManagerComponent:
    """Component for managing document processing cache"""
    
    def __init__(self, cache: ResultCache):
        self.cache = cache
    
    def render_cache_stats(self) -> None:
        """Render cache statistics"""
        st.subheader("📦 Cache Statistics")
        
        try:
            stats = self.cache.get_cache_stats()
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Entries", stats.get('total_entries', 0))
            
            with col2:
                st.metric("Cache Size", f"{stats.get('cache_size_mb', 0)} MB")
            
            with col3:
                st.metric("Max Age", f"{stats.get('max_age_hours', 0)} hours")
            
            # Type distribution
            type_dist = stats.get('type_distribution', {})
            if type_dist:
                st.write("**Document Types in Cache:**")
                for doc_type, count in type_dist.items():
                    st.write(f"• {doc_type or 'Unknown'}: {count}")
        
        except Exception as e:
            st.error(f"Error loading cache statistics: {e}")
    
    def render_cache_actions(self) -> None:
        """Render cache management actions"""
        st.subheader("🧹 Cache Actions")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Clear Failed Results", type="secondary"):
                cleared = self.cache.clear_failed_results()
                st.success(f"Cleared {cleared} failed cache entries")
                st.rerun()
        
        with col2:
            if st.button("Clear Expired", type="secondary"):
                cleared = self.cache.cleanup_expired()
                st.success(f"Cleared {cleared} expired cache entries")
                st.rerun()
        
        with col3:
            if st.button("Clear All Cache", type="primary"):
                if st.session_state.get('confirm_clear_all', False):
                    self.cache.clear_all()
                    st.success("All cache entries cleared")
                    st.session_state.confirm_clear_all = False
                    st.rerun()
                else:
                    st.session_state.confirm_clear_all = True
                    st.warning("Click again to confirm clearing all cache")
    
    def render_document_cache_manager(self, documents: List[Document]) -> None:
        """Render document-specific cache management"""
        if not documents:
            return
        
        st.subheader("🗂️ Document Cache Management")
        
        # Create DataFrame for cache status
        cache_data = []
        for doc in documents:
            # Check if document has cached results
            has_cache, success = self._has_cached_result(doc)
            
            cache_info = "No cache"
            if has_cache:
                cache_info = "✅ Success" if success else "❌ Failed"
            
            cache_data.append({
                'Filename': doc.filename,
                'Cache Status': cache_info,
                'Size (MB)': round(doc.file_size / (1024 * 1024), 2),
                'Clear Cache': False,
                'doc_obj': doc,
                'doc_etag': doc.etag  # Store etag for cache operations
            })
        
        if not cache_data:
            st.info("No documents available for cache management")
            return
        
        df = pd.DataFrame(cache_data)
        
        # Configure columns
        column_config = {
            "Filename": st.column_config.TextColumn(
                "Filename",
                help="Document filename",
                disabled=True,
                width="large"
            ),
            "Cache Status": st.column_config.TextColumn(
                "Cache Status",
                help="Current cache status",
                disabled=True,
                width="medium"
            ),
            "Size (MB)": st.column_config.NumberColumn(
                "Size (MB)",
                help="File size in megabytes",
                disabled=True,
                width="small"
            ),
            "Clear Cache": st.column_config.CheckboxColumn(
                "Clear Cache",
                help="Select to clear cache for this document",
                width="small"
            ),
            "doc_obj": st.column_config.TextColumn(
                "Document Object",
                help="Internal document object",
                disabled=True,
                width="small"
            ),
            "doc_etag": st.column_config.TextColumn(
                "Document ETag",
                help="Internal document etag",
                disabled=True,
                width="small"
            )
        }
        
        # Display editable dataframe
        edited_df = st.data_editor(
            df,
            column_config=column_config,
            hide_index=True,
            num_rows="fixed",
            key="cache_management_table",
            use_container_width=True,
            column_order=["Filename", "Cache Status", "Size (MB)", "Clear Cache"],
            disabled=["doc_obj", "doc_etag"]
        )
        
        # Process clear cache selections
        to_clear = edited_df[edited_df['Clear Cache']].to_dict('records')
        
        if to_clear:
            if st.button(f"Clear Cache for {len(to_clear)} Selected Documents", type="primary"):
                cleared_count = 0
                for item in to_clear:
                    filename = item['Filename']
                    doc_etag = item['doc_etag']
                    try:
                        # Clear cache by etag
                        self._clear_cache_by_etag(doc_etag)
                        cleared_count += 1
                    except Exception as e:
                        st.error(f"Error clearing cache for {filename}: {e}")
                
                st.success(f"Cleared cache for {cleared_count} documents")
                st.rerun()
    
    def _get_cache_key_for_doc(self, document: Document) -> str:
        """Get cache key for a document (simplified version)"""
        # This is a simplified version - in reality we'd need the metadata
        # For now, we'll check all cache entries for this document's etag
        for cache_key, entry in self.cache._memory_cache.items():
            if entry.document_etag == document.etag:
                return cache_key
        return ""
    
    def _has_cached_result(self, document: Document) -> tuple[bool, bool]:
        """Check if document has cached result and if it was successful"""
        for cache_key, entry in self.cache._memory_cache.items():
            if entry.document_etag == document.etag:
                processing_info = entry.processing_result.get('processing_info', {})
                success = processing_info.get('success', False)
                return True, success
        return False, False
    
    def _clear_cache_by_etag(self, etag: str) -> None:
        """Clear cache entries by document etag"""
        keys_to_remove = []
        for cache_key, entry in self.cache._memory_cache.items():
            if entry.document_etag == etag:
                keys_to_remove.append(cache_key)
        
        for cache_key in keys_to_remove:
            self.cache._invalidate_entry(cache_key)
    
    def render_cache_details(self) -> None:
        """Render detailed cache information"""
        if st.checkbox("Show Cache Details"):
            st.subheader("🔍 Cache Details")
            
            try:
                # Get cache entries
                entries = []
                for cache_key, entry in self.cache._memory_cache.items():
                    processing_info = entry.processing_result.get('processing_info', {})
                    
                    entries.append({
                        'Cache Key': cache_key[:16] + "...",
                        'Filename': entry.filename,
                        'Document Type': entry.document_type,
                        'Success': "✅" if processing_info.get('success', False) else "❌",
                        'Processor': processing_info.get('processor', 'Unknown'),
                        'Created': entry.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                        'Size (MB)': round(entry.file_size / (1024 * 1024), 2)
                    })
                
                if entries:
                    df = pd.DataFrame(entries)
                    st.dataframe(df, use_container_width=True)
                else:
                    st.info("No cache entries found")
            
            except Exception as e:
                st.error(f"Error loading cache details: {e}")
    
    def render_full_cache_manager(self, documents: List[Document]) -> None:
        """Render the complete cache management interface"""
        st.header("🗃️ Cache Management")
        
        # Cache statistics
        self.render_cache_stats()
        
        st.divider()
        
        # Cache actions
        self.render_cache_actions()
        
        st.divider()
        
        # Document-specific cache management
        self.render_document_cache_manager(documents)
        
        st.divider()
        
        # Cache details
        self.render_cache_details()