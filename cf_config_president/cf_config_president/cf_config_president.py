import logging
import os
import requests
import datetime
import pytz
import json
from google.cloud import logging as gl
from google.cloud import storage

def config_president(event, context):
    setup_logging()
    logging.info("Starting function")
    logging.info(event["attributes"])
    scheduler_attributes = event["attributes"]
    data = requests.get(scheduler_attributes.get("url"))
    storage_client = storage.Client()
    bucket = storage_client.bucket("tse_data")
    ct = datetime.datetime.now(pytz.timezone('America/Sao_Paulo'))
    date_stamp = ct.strftime("%Y-%m-%d %H:%M:%S")
    blob_name = date_stamp + "_" + scheduler_attributes.get("name")
    blob_folder = "downloads"
    blob = bucket.blob(blob_folder + "/" + blob_name)
    blob.upload_from_string(data.content)
    info_message = json.dumps(
        {'action': "process", 'type': "config" ,
         'name': blob_name, 'folder': blob_folder})
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
    config_president("event", context="context")