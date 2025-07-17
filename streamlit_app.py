#!/usr/bin/env python3
"""
Streamlit application entry point for S3 Document Processor
"""

import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ui.app import main

if __name__ == "__main__":
    main()