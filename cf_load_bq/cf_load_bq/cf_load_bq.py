import logging
import os
import base64
import datetime
import json
import pytz
from google.cloud import logging as gl
from google.cloud import storage
from google.cloud import bigquery


def load_bq(event, context):
    setup_logging()
    logging.info("Starting function")
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message_json = json.loads(pubsub_message)
    blob_name = message_json.get('jsonPayload').get('name')
    blob_folder = message_json.get('jsonPayload').get('folder')
    blob_datetime = message_json.get('jsonPayload').get('file_datetime')
    blob_tse_name = message_json.get('jsonPayload').get('tse_name')
    storage_client = storage.Client()
    bucket = storage_client.bucket("tse_data")
    blob = bucket.blob(blob_folder + "/" + blob_name)
    contents = blob.download_as_string()
    json_data = json.loads(contents)
    logging.info(json_data)
    client = bigquery.Client()
    table_id = "brazilian-elections.simplified_data.presidential_election"
    ct = datetime.datetime.now(pytz.timezone('America/Sao_Paulo'))
    date_stamp = ct.strftime("%Y-%m-%d %H:%M:%S")
    rows_to_insert = [
        {
            "file_datetime": blob_datetime,
            "file_name": blob_name,
            "file_data": json.dumps(json_data),
            "tse_name": blob_tse_name,
            "folder": blob_folder,
            "datetime": date_stamp
         }
    ]
    errors = client.insert_rows_json(table_id,
                                     rows_to_insert)  # Make an API request.
    if errors:
        logging.error(f"Encountered errors while inserting rows: {errors}")
        return
    logging.info(f"New rows have been added, data from {blob_name} add to BQ.")
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
    load_bq("event", context="context")