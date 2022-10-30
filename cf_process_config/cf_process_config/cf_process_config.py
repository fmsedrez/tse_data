import logging
import os
import base64
import datetime
import json
from google.cloud import logging as gl
from google.cloud import storage


def process_config(event, context):
    setup_logging()
    logging.info("Starting function")
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message_json = json.loads(pubsub_message)
    blob_name = message_json.get('jsonPayload').get('name')
    storage_client = storage.Client()
    bucket = storage_client.bucket("tse_data")
    blob_folder = message_json.get('jsonPayload').get('folder')
    blob = bucket.blob(blob_folder + "/" + blob_name)
    contents = blob.download_as_string()
    json_data = json.loads(contents)
    files = json_data.get("arq")
    logging.info(files)
    list_files = list(filter(lambda x: x["nm"].endswith("r.json"), files))
    logging.info(list_files)
    for file in list_files:
        file_date = datetime.datetime.strptime(file["dh"], "%d/%m/%Y %H:%M:%S")

        info_message = json.dumps(
            {'action': "download", 'type': "dados-simplificados" ,
             'name': file_date.strftime("%Y-%m-%d %H:%M:%S") +
                     "_" + file["nm"], 'folder': "simplified_data"})
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
    process_config("event", context="context")