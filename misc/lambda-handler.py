# Simple lambda function to process clickstream data from kinesis data stream sent to lambda

import base64
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    for record in event["Records"]:
        payload = json.loads(base64.b64decode(record["kinesis"]["data"]))
        logger.info(
            "clickstream event | session=%s type=%s element=%s id=%s path=%s pos=(%s,%s) ts=%s",
            payload.get("session_id"),
            payload.get("event_type"),
            payload.get("element"),
            payload.get("element_id"),
            payload.get("path"),
            payload.get("x"),
            payload.get("y"),
            payload.get("timestamp"),
        )

    return {"statusCode": 200, "processed": len(event["Records"])}
