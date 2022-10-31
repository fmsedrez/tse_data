import logging
import os
import base64
import json
from google.cloud import logging as gl
from google.cloud import firestore
from google.cloud import bigquery


def load_firebase(event, context):
    setup_logging()
    logging.info("Starting function")
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message_json = json.loads(pubsub_message)
    client_bq = bigquery.Client()
    client_fs = firestore.Client()
    table_id = message_json.get('jsonPayload').get('table_id')
    collection = message_json.get('jsonPayload').get('collection')
    document = message_json.get('jsonPayload').get('document')
    query_job = client_bq.query(
        f"""
        WITH candidates_array AS (
            SELECT JSON_EXTRACT_ARRAY(file_data, '$.cand') AS candidate
            FROM {table_id}
            ORDER BY file_datetime 
            DESC LIMIT 1
        )
        SELECT candidates_array.candidate
        FROM candidates_array;
        """
    )
    results = query_job.result()
    logging.info(results)
    if not results:
        logging.error(f"Encountered errors while get rows")
        return
    for row in results:
        dict_row = dict(row.items())
        candidates = dict_row['candidate']
        return_candidate = []
        for candidate in candidates:
            json_struct = json.loads(candidate)
            name = json_struct.get('nm')
            percentage = json_struct.get('pvap')
            return_candidate.append({'name': name,'percentage': percentage})
        client_fs.collection(collection).document(document).set({'candidate': return_candidate})
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
    load_firebase("event", context="context")