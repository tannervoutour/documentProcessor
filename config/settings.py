import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# DataLabs Configuration
DATALABS_API_KEY = os.getenv("DATALABS_API_KEY")

# n8n Configuration
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")
N8N_API_KEY = os.getenv("N8N_API_KEY")

# Application Settings
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MAX_CONCURRENT_UPLOADS = int(os.getenv("MAX_CONCURRENT_UPLOADS", "5"))
CACHE_DIR = os.getenv("CACHE_DIR", ".cache")

# Validation
def validate_config():
    """Validate required configuration"""
    required_vars = [
        ("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID),
        ("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY),
        ("S3_BUCKET_NAME", S3_BUCKET_NAME),
        ("SUPABASE_URL", SUPABASE_URL),
        ("SUPABASE_KEY", SUPABASE_KEY),
    ]
    
    missing = [var for var, val in required_vars if not val]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

# AWS Config dictionary
def get_aws_config():
    """Get AWS configuration dictionary"""
    return {
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "region_name": AWS_REGION,
    }

# Settings class for easy access
class Settings:
    """Settings class for easy access to configuration"""
    
    # AWS S3
    AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY
    AWS_REGION = AWS_REGION
    S3_BUCKET_NAME = S3_BUCKET_NAME
    
    # Supabase
    SUPABASE_URL = SUPABASE_URL
    SUPABASE_KEY = SUPABASE_KEY
    SUPABASE_SERVICE_KEY = SUPABASE_SERVICE_KEY
    
    # DataLabs
    DATALABS_API_KEY = DATALABS_API_KEY
    
    # n8n
    N8N_WEBHOOK_URL = N8N_WEBHOOK_URL
    N8N_API_KEY = N8N_API_KEY
    
    # Application
    LOG_LEVEL = LOG_LEVEL
    BATCH_SIZE = BATCH_SIZE
    MAX_CONCURRENT_UPLOADS = MAX_CONCURRENT_UPLOADS
    CACHE_DIR = CACHE_DIR

# Global settings instance
settings = Settings()