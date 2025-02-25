from loguru import logger
from config import LOG_FILE_PATH

logger.add(LOG_FILE_PATH, rotation="1 week", level="WARNING", 
           format='{{"time": "{time}", "level": "{level}", "message": "{message}"}}')

logger.info("Logger initialized")