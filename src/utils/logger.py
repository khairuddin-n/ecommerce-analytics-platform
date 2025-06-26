"""Logging configuration module"""

import sys
from loguru import logger
from src.utils.config import settings

# Remove default logger
logger.remove()

# Add custom logger with formatting
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level=settings.log_level,
    colorize=True
)

# Add file logger for production
if settings.env == "production":
    logger.add(
        "logs/app_{time}.log",
        rotation="500 MB",
        retention="10 days",
        level="INFO"
    )

# Export configured logger
__all__ = ["logger"]
