import json
import os
from pathlib import Path
import logging
from logger_config import logger

PROGRESS_FILE = Path("progress_data.json")

def save_progress(data):
    """Save progress data to JSON file"""
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving progress data: {e}")

def load_progress():
    """Load progress data from JSON file"""
    try:
        if PROGRESS_FILE.exists():
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading progress data: {e}")
    
    # Return default progress data if file doesn't exist or error occurs
    return {
        'total': 0,
        'success': 0,
        'failed': 0,
        'status': 'Menunggu',
        'batch_size': 2,
        'total_batches': 0,
        'current_batch': 0,
        'logs': []
    }

def clear_progress():
    """Clear progress data"""
    try:
        if PROGRESS_FILE.exists():
            os.remove(PROGRESS_FILE)
    except Exception as e:
        logger.error(f"Error clearing progress data: {e}")
