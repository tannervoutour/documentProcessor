#!/usr/bin/env python3
"""
Setup script for Document Processing System
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def run_command(cmd, description):
    """Run a command and handle errors"""
    print(f"\n🔧 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        if e.stderr:
            print(f"Error: {e.stderr}")
        return None

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        sys.exit(1)
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")

def setup_environment():
    """Set up the project environment"""
    print("🚀 Setting up Document Processing System")
    print("=" * 50)
    
    # Check Python version
    check_python_version()
    
    # Check if virtual environment exists
    venv_path = Path("venv")
    if not venv_path.exists():
        print("\n🔧 Creating virtual environment...")
        run_command("python -m venv venv", "Create virtual environment")
    else:
        print("\n✅ Virtual environment already exists")
    
    # Activate virtual environment and install dependencies
    if os.name == 'nt':  # Windows
        pip_path = "venv\\Scripts\\pip"
        python_path = "venv\\Scripts\\python"
    else:  # Unix/Linux/Mac
        pip_path = "venv/bin/pip"
        python_path = "venv/bin/python"
    
    # Install dependencies
    if run_command(f"{pip_path} install -r requirements.txt", "Install dependencies"):
        print("✅ All dependencies installed successfully")
    else:
        print("❌ Failed to install dependencies")
        sys.exit(1)
    
    # Set up environment file
    env_file = Path(".env")
    env_example = Path(".env.example")
    
    if not env_file.exists() and env_example.exists():
        print("\n🔧 Setting up environment file...")
        shutil.copy(env_example, env_file)
        print("✅ Environment file created from template")
        print("⚠️  Please edit .env file with your actual credentials")
    elif env_file.exists():
        print("\n✅ Environment file already exists")
    else:
        print("\n❌ .env.example file not found")
    
    # Test the setup
    print("\n🧪 Testing setup...")
    test_result = run_command(f"{python_path} main.py test", "Test connections")
    
    if test_result:
        print("\n🎉 Setup completed successfully!")
        print("\nNext steps:")
        print("1. Edit .env file with your actual credentials")
        print("2. Set up Supabase database table (see README.md)")
        print("3. Run: streamlit run streamlit_app.py")
        print("4. Open http://localhost:8501 in your browser")
    else:
        print("\n⚠️  Setup completed but tests failed")
        print("Please check your .env configuration and try again")

if __name__ == "__main__":
    setup_environment()