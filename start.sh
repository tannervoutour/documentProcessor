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
echo "Running in background with nohup. Check streamlit.log for output."
echo "To stop the server, use: pkill -f streamlit"

nohup streamlit run streamlit_app.py --server.headless true --server.port 8501 > streamlit.log 2>&1 &

echo "Server started with PID: $!"
echo "Log file: streamlit.log"