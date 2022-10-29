import logging
import os
from google.cloud import logging as gl
from google.cloud import storage

def file_in_folders(event, context):
    if os.getenv("env") != "local":
        google_logging = gl.Client()
        google_logging.setup_logging()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler()
        ]
    )
    logging.info("Info!")
    storage_client = storage.Client()
    blobs = storage_client.list_blobs("tse_data", prefix="raw")
    logging.info("Blobs:")
    matches = [file.name for file in blobs if
                '.json' in file.name and 'demo' in file.name]
    for match in matches:
        logging.info(match)
    return

if __name__ == '__main__':
    file_in_folders("event", context="context")