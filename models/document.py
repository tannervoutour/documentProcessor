from dataclasses import dataclass
from typing import List, Optional, Dict
from datetime import datetime

@dataclass
class Document:
    """Represents a document in the system"""
    s3_key: str
    filename: str
    file_size: int
    last_modified: datetime
    etag: str  # S3 ETag for unique identification
    # Metadata to be filled by UI
    machine_names: Optional[List[str]] = None
    document_type: Optional[str] = None
    processed: bool = False
    processing_status: str = "pending"
    
    @property
    def file_id(self) -> str:
        """Generate unique file ID from S3 ETag"""
        return self.etag
    
    def to_dict(self) -> Dict:
        """Convert document to dictionary"""
        return {
            's3_key': self.s3_key,
            'filename': self.filename,
            'file_size': self.file_size,
            'last_modified': self.last_modified.isoformat() if isinstance(self.last_modified, datetime) else self.last_modified,
            'etag': self.etag,
            'machine_names': self.machine_names,
            'document_type': self.document_type,
            'processed': self.processed,
            'processing_status': self.processing_status,
            'file_id': self.file_id
        }
    
    @classmethod
    def from_s3_object(cls, s3_obj: Dict) -> 'Document':
        """Create Document from S3 object"""
        return cls(
            s3_key=s3_obj['Key'],
            filename=s3_obj['Key'].split('/')[-1],
            file_size=s3_obj['Size'],
            last_modified=s3_obj['LastModified'],
            etag=s3_obj['ETag'].strip('"')  # Remove quotes from ETag
        )

@dataclass
class ProcessedDocument:
    """Represents a processed document result"""
    document: Document
    content: Dict
    metadata: Dict
    success: bool
    error: Optional[str] = None
    processing_time: Optional[float] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'document': self.document.to_dict(),
            'content': self.content,
            'metadata': self.metadata,
            'success': self.success,
            'error': self.error,
            'processing_time': self.processing_time
        }