from loguru import logger
from config import LOG_FILE_PATH

logger.level("EXECUTION_TIME", no=27, color="<blue>")

logger.add(LOG_FILE_PATH, rotation="1 week", level="EXECUTION_TIME", 
           format='{{"time": "{time}", "level": "{level}", "message": "{message}"}}')

logger.success("Logger initialized")