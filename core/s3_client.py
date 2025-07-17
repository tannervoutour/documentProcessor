import boto3
from typing import List, Dict, Generator, Optional
import logging
from botocore.exceptions import ClientError
from models.document import Document

class S3Client:
    """Handles all S3 operations"""
    
    def __init__(self, bucket_name: str, aws_config: Dict):
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3', **aws_config)
        self.logger = logging.getLogger(__name__)
    
    def list_documents(self, prefix: str = "", batch_size: int = 1000) -> Generator[Document, None, None]:
        """
        List all documents in S3 bucket with pagination
        Yields Document objects
        """
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    # Skip directories
                    if obj['Key'].endswith('/'):
                        continue
                    
                    # Skip hidden files
                    if obj['Key'].split('/')[-1].startswith('.'):
                        continue
                        
                    yield Document.from_s3_object(obj)
                    
        except ClientError as e:
            self.logger.error(f"Error listing documents: {e}")
            raise
    
    def download_document(self, s3_key: str) -> bytes:
        """Download document content from S3"""
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response['Body'].read()
        except ClientError as e:
            self.logger.error(f"Error downloading {s3_key}: {e}")
            raise
    
    def get_document_metadata(self, s3_key: str) -> Dict:
        """Get document metadata from S3"""
        try:
            response = self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'content_type': response.get('ContentType', 'application/octet-stream'),
                'etag': response['ETag'].strip('"')
            }
        except ClientError as e:
            self.logger.error(f"Error getting metadata for {s3_key}: {e}")
            raise
    
    def generate_presigned_url(self, s3_key: str, expiration: int = 3600) -> str:
        """Generate presigned URL for document access"""
        try:
            return self.s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': s3_key},
                ExpiresIn=expiration
            )
        except ClientError as e:
            self.logger.error(f"Error generating presigned URL for {s3_key}: {e}")
            raise
    
    def document_exists(self, s3_key: str) -> bool:
        """Check if document exists in S3"""
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    def get_bucket_info(self) -> Dict:
        """Get bucket information"""
        try:
            response = self.s3.head_bucket(Bucket=self.bucket_name)
            return {
                'bucket_name': self.bucket_name,
                'region': response.get('ResponseMetadata', {}).get('HTTPHeaders', {}).get('x-amz-bucket-region', 'unknown')
            }
        except ClientError as e:
            self.logger.error(f"Error getting bucket info: {e}")
            raise