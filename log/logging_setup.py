import sys
from loguru import logger

def setup_logging(log_level="INFO", log_file="vestige.log"):
    logger.remove()
    fmt = "{time:YYYY-MM-DD HH:mm:ss} - {file}:{line} - {level} - {message}"
    
    logger.add(sys.stdout, format=fmt, level=log_level)
    logger.add(log_file, format=fmt, level=log_level, rotation="2 MB", retention=10)
    
    logger.info("Logging configured successfully.")
    