#!/bin/bash

# Start script for Document Processing System

echo "🚀 Starting Document Processing System..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run setup.py first."
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "❌ .env file not found. Please run setup.py first."
    exit 1
fi

# Start Streamlit app
echo "🌐 Starting Streamlit UI on http://localhost:8501"
echo "Press Ctrl+C to stop the server"

streamlit run streamlit_app.py --server.headless true --server.port 8501