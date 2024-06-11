import logging
import logging.config
import yaml

# Function to generate logger from config file
def get_logger() -> logging.Logger:
    with open('cda_api/config/logger.yml') as log_config_file:
        log_config = yaml.safe_load(log_config_file)
    logging.config.dictConfig(log_config)
    logger = logging.getLogger("simple")
    return logger


def is_float(element):
    if element is None: 
        return False
    try:
        float(element)
        return True
    except ValueError:
        return False
    
def is_int(element):
    if element is None: 
        return False
    try:
        int(element)
        return True
    except ValueError:
        return False