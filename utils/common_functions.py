import os
from src.logger import get_logger
import yaml
import sys
from src.exceptions import CustomException
import pandas as pd

logger = get_logger(__name__)

def read_yaml(file_path):
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"YAML file not found: {file_path}")
        
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
            logger.info(f"Successfully read the YAML file: {file_path}")
            return config
        
    except Exception as e:
        logger.error(f"Error reading YAML file: {file_path}")
        raise CustomException(f"Failed to read the YAML file: {file_path}", sys) from e