#!/usr/bin/env python3
"""Download e-commerce dataset from Kaggle"""

import sys
import os
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import kaggle
from src.utils.logger import logger
from src.utils.config import settings

def check_kaggle_credentials():
    """Check if Kaggle credentials are properly set"""
    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    
    if not kaggle_json.exists():
        logger.error("Kaggle credentials not found!")
        logger.info("Please follow these steps:")
        logger.info("1. Go to https://www.kaggle.com/account")
        logger.info("2. Click 'Create New API Token'")
        logger.info("3. Save kaggle.json to ~/.kaggle/")
        return False
    
    # Check permissions
    if oct(kaggle_json.stat().st_mode)[-3:] != '600':
        logger.warning("Setting correct permissions for kaggle.json")
        kaggle_json.chmod(0o600)
    
    return True

def download_ecommerce_data():
    """Download e-commerce dataset"""
    # Dataset: E-Commerce Data by Carrie
    dataset = "carrie1/ecommerce-data"
    
    settings.data_path_raw.mkdir(parents=True, exist_ok=True)
    
    try:
        logger.info(f"Downloading dataset: {dataset}")
        logger.info("This dataset contains 540k+ retail transactions from UK")
        
        kaggle.api.dataset_download_files(
            dataset,
            path=settings.data_path_raw,
            unzip=True
        )
        
        # Check downloaded files
        files = list(settings.data_path_raw.glob("*.csv"))
        if files:
            for file in files:
                size_mb = file.stat().st_size / (1024 * 1024)
                logger.info(f"Downloaded: {file.name} ({size_mb:.1f} MB)")
                
                # Rename to standard name
                if "data.csv" in file.name.lower():
                    new_name = settings.data_path_raw / "ecommerce_data.csv"
                    file.rename(new_name)
                    logger.info(f"Renamed to: {new_name.name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to download: {e}")
        logger.info("Make sure you have accepted dataset terms on Kaggle")
        return False

def main():
    logger.info("=== E-Commerce Dataset Download ===")
    
    if not check_kaggle_credentials():
        sys.exit(1)
    
    if download_ecommerce_data():
        logger.success("✅ Download complete!")
    else:
        logger.error("❌ Download failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
