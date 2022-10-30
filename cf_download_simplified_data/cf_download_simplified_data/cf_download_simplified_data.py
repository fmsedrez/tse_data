import logging
import os
import base64
import datetime
import requests
import json
import pytz
from google.cloud import logging as gl
from google.cloud import storage


def download_simplified_data(event, context):
    setup_logging()
    logging.info("Starting function")
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message_json = json.loads(pubsub_message)
    blob_name = message_json.get('jsonPayload').get('name')
    blob_folder = message_json.get('jsonPayload').get('folder')
    blob_type = message_json.get('jsonPayload').get('type')
    blob_datetime = message_json.get('jsonPayload').get('file_datetime')
    storage_client = storage.Client()
    blobs = storage_client.list_blobs("tse_data", prefix=blob_folder)
    logging.info("Blobs:")
    logging.info(blobs)
    matches = [file.name for file in blobs if blob_name in file.name]
    if matches:
        logging.info(f"File {blob_name} already in folder {blob_folder},"
                     f" skipped!")
        return
    base_url = message_json.get('jsonPayload').get('tse_url')
    tse_name = message_json.get('jsonPayload').get('tse_name')
    data = requests.get(base_url + tse_name)
    bucket = storage_client.bucket("tse_data")
    blob = bucket.blob(blob_folder + "/" + blob_name)
    blob.upload_from_string(data.content)
    info_message = json.dumps(
        {'action': "process", 'type': blob_type,
         'name': blob_name, 'folder': blob_folder,
         'tse_name': tse_name,
         'tse_url': base_url,
         'file_datetime': blob_datetime
         })
    logging.info(info_message)
    return

def setup_logging():
    if os.getenv("env") != "local":
        google_logging = gl.Client()
        google_logging.setup_logging()
    else:
        logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler()
        ]
    )

if __name__ == '__main__':
    download_simplified_data("event", context="context")