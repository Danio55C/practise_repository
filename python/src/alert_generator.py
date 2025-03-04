import random
import pandas as pd
import numpy as np
from loguru import logger


alert_names = ["Memory Leak", "Network Issue", "Too Many Connections", "Database Connection Lost", "Missing Index Warning", "Inconsistent Data Found"]
severity_levels = [ "Warning", "Error", "Critical Error"]

start_date = pd.Timestamp.now() - pd.Timedelta(days=1) 
end_date = pd.Timestamp.now()

random_dates = pd.to_datetime(np.random.uniform(start_date.value, end_date.value, 30))

def generate_alert():
    try:
        alert_name = random.choice(alert_names)
        severity_level = random.choice(severity_levels)
        timestamp = random.choice(random_dates)
        message = f"An {severity_level} level error has occurred: {alert_name} on {str(timestamp)}"
    
        return {
            "alert_name": alert_name, 
            "severity_level": severity_level, 
            "timestamp": timestamp, 
            "message": message
        }
    except Exception as e:
        logger.error(f"An error has ocured while trying to generate alerts:{e}")
        return None