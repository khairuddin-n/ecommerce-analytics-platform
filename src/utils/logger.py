"""Logging configuration module"""

import sys
import os
from pathlib import Path
from loguru import logger
from src.utils.config import settings

# Remove default logger
logger.remove()

# Always add console logger
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level=settings.log_level,
    colorize=True
)

# Try to add file logger with error handling
try:
    log_dir = Path(settings.project_root) / "logs"
    log_dir.mkdir(exist_ok=True, parents=True)
    
    # Check if we can write to the directory
    test_file = log_dir / ".test_write"
    try:
        test_file.touch()
        test_file.unlink()
        
        # Add rotating file logger
        logger.add(
            log_dir / "pipeline_{time:YYYY-MM-DD}.log",
            rotation="00:00",  # New file every day
            retention="10 days",
            level="DEBUG",  # Capture everything in file
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
        )
        
        # Add a separate error log
        logger.add(
            log_dir / "errors_{time:YYYY-MM-DD}.log",
            rotation="00:00",
            retention="30 days",
            level="ERROR",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
        )
        
        logger.info(f"Logging initialized. Log files in: {log_dir}")
        
    except Exception as e:
        logger.warning(f"Cannot write to log directory {log_dir}: {e}")
        logger.warning("File logging disabled, using console only")
        
except Exception as e:
    logger.warning(f"Failed to setup file logging: {e}")
    logger.warning("Continuing with console logging only")

# Export configured logger
__all__ = ["logger"]