from supabase import create_client, Client
from typing import List, Dict, Optional
import logging
from datetime import datetime

class SupabaseClient:
    """Handles all Supabase operations"""
    
    def __init__(self, url: str, key: str):
        self.client: Client = create_client(url, key)
        self.logger = logging.getLogger(__name__)
    
    def get_processed_documents(self) -> List[str]:
        """Get list of all processed document titles"""
        try:
            response = self.client.table('document_metadata').select('title').execute()
            return [doc['title'] for doc in response.data if doc.get('title')]
        except Exception as e:
            self.logger.error(f"Error fetching processed documents: {e}")
            raise
    
    def check_document_exists(self, file_id: str) -> bool:
        """Check if a specific document has been processed"""
        try:
            response = self.client.table('document_metadata').select('id').eq('id', file_id).execute()
            return len(response.data) > 0
        except Exception as e:
            self.logger.error(f"Error checking document existence: {e}")
            raise
    
    def get_document_metadata(self, file_id: str) -> Optional[Dict]:
        """Retrieve metadata for a specific document"""
        try:
            response = self.client.table('document_metadata').select('*').eq('id', file_id).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            self.logger.error(f"Error fetching document metadata: {e}")
            raise
    
    def insert_document_metadata(self, document_data: Dict) -> Dict:
        """Insert new document metadata"""
        try:
            response = self.client.table('document_metadata').insert(document_data).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            self.logger.error(f"Error inserting document metadata: {e}")
            raise
    
    def update_document_metadata(self, file_id: str, updates: Dict) -> Dict:
        """Update existing document metadata"""
        try:
            response = self.client.table('document_metadata').update(updates).eq('id', file_id).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            self.logger.error(f"Error updating document metadata: {e}")
            raise
    
    def get_documents_by_type(self, document_type: str) -> List[Dict]:
        """Get documents by document type"""
        try:
            response = self.client.table('document_metadata').select('*').eq('document_type', document_type).execute()
            return response.data
        except Exception as e:
            self.logger.error(f"Error fetching documents by type: {e}")
            raise
    
    def get_documents_by_machine(self, machine_name: str) -> List[Dict]:
        """Get documents by machine name"""
        try:
            response = self.client.table('document_metadata').select('*').contains('machine_names', [machine_name]).execute()
            return response.data
        except Exception as e:
            self.logger.error(f"Error fetching documents by machine: {e}")
            raise
    
    def get_processing_statistics(self) -> Dict:
        """Get processing statistics"""
        try:
            # Total documents
            total_response = self.client.table('document_metadata').select('id', count='exact').execute()
            total = total_response.count
            
            # Documents by type
            type_response = self.client.table('document_metadata').select('document_type', count='exact').execute()
            
            # Documents by status
            status_response = self.client.table('document_metadata').select('processing_status', count='exact').execute()
            
            return {
                'total_documents': total,
                'by_type': self._count_by_field(type_response.data, 'document_type'),
                'by_status': self._count_by_field(status_response.data, 'processing_status')
            }
        except Exception as e:
            self.logger.error(f"Error fetching processing statistics: {e}")
            raise
    
    def _count_by_field(self, data: List[Dict], field: str) -> Dict:
        """Count records by field value"""
        counts = {}
        for record in data:
            value = record.get(field, 'unknown')
            counts[value] = counts.get(value, 0) + 1
        return counts
    
    def delete_document(self, file_id: str) -> bool:
        """Delete document metadata"""
        try:
            response = self.client.table('document_metadata').delete().eq('id', file_id).execute()
            return len(response.data) > 0
        except Exception as e:
            self.logger.error(f"Error deleting document: {e}")
            raise
    
    def health_check(self) -> bool:
        """Check Supabase connection health"""
        try:
            response = self.client.table('document_metadata').select('id').limit(1).execute()
            return True
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False