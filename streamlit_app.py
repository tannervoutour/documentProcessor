#!/usr/bin/env python3
"""
Streamlit application entry point for S3 Document Processor
"""

import sys
import os
import streamlit as st

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure Streamlit page settings first
st.set_page_config(
    page_title="S3 Document Processor",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)

from ui.app import main

if __name__ == "__main__":
    main()