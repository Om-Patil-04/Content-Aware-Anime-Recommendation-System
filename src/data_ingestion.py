import os
from google.cloud import storage
from src.logger import get_logger
from config.paths_config import RAW_DIR, CONFIG_PATH
from src.exceptions import CustomException
from utils.common_functions import read_yaml

logger = get_logger(__name__)


class DataIngestion:
    def __init__(self, config):
        self.config = config["data_ingestion"]
        self.bucket_name = self.config["bucket_name"]
        self.file_names = self.config["bucket_file_names"]

        os.makedirs(RAW_DIR, exist_ok=True)

        logger.info(f"DataIngestion initialized | Bucket: {self.bucket_name} | Files: {self.file_names}")
            
    def download_files_from_gcs(self):
        try:
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)

            for file_name in self.file_names:
                file_path = os.path.join(RAW_DIR, file_name)
                blob = bucket.blob(file_name)

                logger.info(f"Downloading {file_name} from GCS")
                blob.download_to_filename(file_path)

                logger.info(f"File successfully saved to {file_path}")

        except Exception as e:
            logger.exception("Error occurred while downloading data from GCS")
            raise CustomException("Data ingestion failed during download", e)

    def run(self):
        try:
            logger.info("Starting data ingestion stage")
            self.download_files_from_gcs()
            logger.info("Data ingestion stage completed successfully")

        except Exception as e:
            logger.exception("Data ingestion pipeline failed")
            raise CustomException("Data ingestion pipeline failed", e)

        finally:
            logger.info("Data ingestion process finished")


if __name__ == "__main__":
    config = read_yaml(CONFIG_PATH)
    data_ingestion = DataIngestion(config)
    data_ingestion.run()
