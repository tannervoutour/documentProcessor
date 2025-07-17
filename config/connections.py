import logging
from typing import Optional
from core.s3_client import S3Client
from core.supabase_client import SupabaseClient
from config.settings import (
    S3_BUCKET_NAME,
    SUPABASE_URL,
    SUPABASE_KEY,
    get_aws_config,
    validate_config
)

# Global client instances
_s3_client: Optional[S3Client] = None
_supabase_client: Optional[SupabaseClient] = None

def get_s3_client() -> S3Client:
    """Get S3 client instance (singleton)"""
    global _s3_client
    
    if _s3_client is None:
        validate_config()
        
        _s3_client = S3Client(
            bucket_name=S3_BUCKET_NAME,
            aws_config=get_aws_config()
        )
        
        # Test connection
        try:
            bucket_info = _s3_client.get_bucket_info()
            logging.info(f"Connected to S3 bucket: {bucket_info}")
        except Exception as e:
            logging.error(f"Failed to connect to S3: {e}")
            raise
    
    return _s3_client

def get_supabase_client() -> SupabaseClient:
    """Get Supabase client instance (singleton)"""
    global _supabase_client
    
    if _supabase_client is None:
        validate_config()
        
        _supabase_client = SupabaseClient(
            url=SUPABASE_URL,
            key=SUPABASE_KEY
        )
        
        # Test connection
        try:
            health_check = _supabase_client.health_check()
            if health_check:
                logging.info("Connected to Supabase successfully")
            else:
                logging.warning("Supabase connection test failed")
        except Exception as e:
            logging.error(f"Failed to connect to Supabase: {e}")
            raise
    
    return _supabase_client

def reset_connections():
    """Reset all connection instances (useful for testing)"""
    global _s3_client, _supabase_client
    _s3_client = None
    _supabase_client = None

def test_connections() -> dict:
    """Test all connections and return status"""
    results = {}
    
    # Test S3 connection
    try:
        s3_client = get_s3_client()
        bucket_info = s3_client.get_bucket_info()
        results['s3'] = {
            'status': 'connected',
            'bucket_name': bucket_info['bucket_name'],
            'region': bucket_info.get('region', 'unknown')
        }
    except Exception as e:
        results['s3'] = {
            'status': 'failed',
            'error': str(e)
        }
    
    # Test Supabase connection
    try:
        supabase_client = get_supabase_client()
        health_check = supabase_client.health_check()
        results['supabase'] = {
            'status': 'connected' if health_check else 'failed',
            'url': SUPABASE_URL
        }
    except Exception as e:
        results['supabase'] = {
            'status': 'failed',
            'error': str(e)
        }
    
    return results

def init_logging():
    """Initialize logging configuration"""
    from config.settings import LOG_LEVEL
    
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('document_processor.log')
        ]
    )
    
    # Set specific loggers
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)