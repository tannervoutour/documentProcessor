# Document Processing System

A comprehensive document processing system that processes various document types from S3 storage, extracts content using AI services, and provides a Streamlit UI for management and monitoring.

## Features

- **S3 Integration**: Automatic document discovery and processing from S3 buckets
- **AI Document Processing**: Support for multiple document types (manuals, diagrams, spreadsheets)
- **Streamlit UI**: Web interface for document management and monitoring
- **Metadata Management**: Supabase integration for document metadata and tracking
- **Webhook Integration**: n8n webhook support for workflow automation
- **Circuit Breaker Pattern**: Resilient API calls with automatic retry logic
- **Batch Processing**: Efficient processing of multiple documents
- **Real-time Monitoring**: Live processing status and progress tracking

## Quick Start

### Prerequisites

- Python 3.8 or higher
- AWS S3 bucket with documents
- Supabase project
- DataLabs API access (optional)
- n8n instance (optional)

### Installation

#### Option 1: Automatic Setup (Recommended)

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd documentProcessor
   ```

2. **Run the setup script**
   ```bash
   python setup.py
   ```

3. **Edit environment variables**
   ```bash
   nano .env  # Add your actual credentials
   ```

4. **Set up Supabase database** (see SQL below)

5. **Start the application**
   ```bash
   ./start.sh
   ```

#### Option 2: Manual Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd documentProcessor
   ```

2. **Create virtual environment (recommended)**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your actual credentials
   nano .env
   ```

5. **Set up Supabase database**
   
   Create the required table in your Supabase project:
   ```sql
   CREATE TABLE document_metadata (
       id TEXT PRIMARY KEY,
       s3_key TEXT NOT NULL,
       filename TEXT NOT NULL,
       file_size BIGINT NOT NULL,
       last_modified TIMESTAMP WITH TIME ZONE NOT NULL,
       etag TEXT,
       content_type TEXT,
       title TEXT,
       machine_names TEXT[],
       document_type TEXT CHECK (document_type IN ('manual', 'diagram', 'sparepartslist', 'spreadsheet')),
       created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
       updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
       processing_attempts INTEGER DEFAULT 0,
       last_processing_attempt TIMESTAMP WITH TIME ZONE,
       processing_error TEXT
   );

   -- Add indexes for performance
   CREATE INDEX idx_document_metadata_s3_key ON document_metadata(s3_key);
   CREATE INDEX idx_document_metadata_type ON document_metadata(document_type);
   CREATE INDEX idx_document_metadata_created_at ON document_metadata(created_at);
   ```

6. **Test the setup**
   ```bash
   python main.py test
   ```

7. **Start the UI**
   ```bash
   streamlit run streamlit_app.py
   ```
   
   The UI will be available at: http://localhost:8501

## Environment Configuration

Edit the `.env` file with your actual credentials:

```bash
# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-s3-bucket-name

# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_supabase_anon_key
SUPABASE_SERVICE_KEY=your_supabase_service_key

# n8n Configuration (optional)
N8N_WEBHOOK_URL=https://your-n8n-instance.app.n8n.cloud/webhook/your-webhook-id
N8N_API_KEY=your_n8n_api_key

# DataLabs Configuration (optional)
DATALABS_API_KEY=your_datalabs_api_key
DATALABS_BASE_URL=https://api.datalabs.com

# Application Settings
LOG_LEVEL=INFO
BATCH_SIZE=100
MAX_CONCURRENT_UPLOADS=5
CACHE_DIR=.cache
```

## Running the Application

### Start the UI Server
```bash
# Start in foreground
streamlit run streamlit_app.py --server.port 8501

# Start in background
nohup streamlit run streamlit_app.py --server.headless true --server.port 8501 > streamlit.log 2>&1 &
```

### Command Line Interface
```bash
# Test connections
python main.py test

# List documents
python main.py list

# Get statistics
python main.py stats

# Process a specific document
python main.py process --document "filename.pdf"
```

## Usage

### Web Interface

1. **Access the UI**: Open http://localhost:8501 in your browser
2. **Document Management**: Browse, filter, and manage documents
3. **Metadata Editing**: Edit document types and machine names
4. **Processing Queue**: Monitor document processing status
5. **Statistics**: View processing metrics and system health

### Document Processing

The system supports various document types:

- **Manuals**: Processed using DataLabs API for content extraction
- **Diagrams**: Processed using PyMuPDF for text and image extraction
- **Spreadsheets**: Processed using pandas for data extraction
- **Spare Parts Lists**: Processed using specialized parsers

### Webhook Integration

Configure n8n webhooks to receive processing notifications:

1. Set up n8n workflow with webhook trigger
2. Configure N8N_WEBHOOK_URL in .env
3. Processing results will be sent to the webhook automatically

## Project Structure

```
documentProcessor/
├── config/                 # Configuration management
│   ├── connections.py      # Database and API connections
│   └── settings.py         # Application settings
├── core/                   # Core business logic
│   ├── batch_processor.py  # Batch processing logic
│   ├── circuit_breaker.py  # Circuit breaker implementation
│   ├── document_manager.py # Document management
│   ├── result_cache.py     # Result caching
│   ├── s3_client.py        # S3 operations
│   ├── supabase_client.py  # Supabase operations
│   └── webhook_manager.py  # Webhook handling
├── integration/            # External integrations
│   └── n8n_webhook.py     # n8n webhook client
├── models/                 # Data models
│   └── document.py        # Document data structures
├── orchestration/          # Processing orchestration
│   └── processing_queue.py # Queue management
├── processors/             # Document processors
│   ├── base_processor.py  # Base processor interface
│   ├── datalabs_processor.py # DataLabs API processor
│   ├── processor_factory.py # Processor factory
│   └── pymupdf_processor.py # PDF processor
├── ui/                     # Streamlit UI components
│   ├── components/         # UI components
│   ├── app.py             # Main UI application
│   └── utils.py           # UI utilities
├── utils/                  # Utility functions
│   ├── content_utils.py   # Content processing utilities
│   └── error_handler.py   # Error handling
├── main.py                # CLI interface
├── streamlit_app.py       # UI entry point
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## Development

### Running Tests

```bash
# Run archived tests (in archive/tests/)
python archive/tests/test_phase1.py
python archive/tests/test_streamlit.py
```

### Adding New Processors

1. Create a new processor in `processors/` inheriting from `BaseProcessor`
2. Register it in `processor_factory.py`
3. Add document type mapping in configuration

### Logging

Logs are written to console and log files. Configure log level via `LOG_LEVEL` environment variable:

- DEBUG: Detailed debugging information
- INFO: General information messages
- WARNING: Warning messages
- ERROR: Error messages only

## Troubleshooting

### Common Issues

1. **Connection Issues**
   - Verify AWS credentials and S3 bucket permissions
   - Check Supabase URL and keys
   - Ensure network connectivity

2. **Processing Failures**
   - Check DataLabs API key and quota
   - Verify document formats are supported
   - Review processing logs for errors

3. **UI Issues**
   - Check if Streamlit is running on the correct port
   - Verify environment variables are loaded
   - Clear browser cache if needed

### Getting Help

- Check the logs: `tail -f streamlit.log`
- Review error messages in the UI
- Verify all environment variables are set correctly
- Test individual components using the CLI

## License

This project is private and proprietary. Unauthorized use is prohibited.