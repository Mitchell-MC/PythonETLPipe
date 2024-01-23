#!/bin/bash

# Check if the virtual environment folder exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python -m venv venv  # Create a virtual environment
fi

# Activate the virtual environment
source venv/Scripts/activate

# Install dependencies
pip install -r requirements.txt

