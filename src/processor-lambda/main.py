"""
Processes SQS queue messages containing Azure resource log file paths.
Downloads each log file, converts the events to CloudTrail format, and
ingests events to CloudTrail Lake. Events which aren't ingested successfully
are delivered to a dead-letter SQS queue. 

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import os
import json
import uuid
import logging
from datetime import datetime, timezone
from dateutil.parser import parse
import boto3
from azure.storage.blob import BlobServiceClient
from botocore.exceptions import ClientError
from typing import Optional

# Logging settings
logging.basicConfig()
logger = logging.getLogger()
if 'LOG_LEVEL' in os.environ:
    logger.setLevel(os.environ['LOG_LEVEL'])
else:
    logger.setLevel(logging.INFO)

# CloudTrail Lake settings
CLOUDTRAIL_LAKE_CHANNEL_ARN = os.environ['CLOUDTRAIL_LAKE_CHANNEL_ARN']
MAX_AUDIT_EVENTS_PER_INGESTION = 100

# Azure settings
AZURE_STORAGE_CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']
AZ_STORAGE_CONTAINER_NAME = os.environ['AZURE_STORAGE_CONTAINER_NAME']

# Queue settings
FAILED_INGESTION_EVENTS_SQS_URL = os.environ['FAILED_INGESTION_EVENTS_SQS_URL']
LOG_FILES_TO_PROCESS_SQS_URL = os.environ['LOG_FILES_TO_PROCESS_SQS_URL']

# Boto3 resources
session = boto3.Session()
cloudtrail_client = session.client('cloudtrail-data')
sqs_client = session.client('sqs')

def get_user_identity_details(azure_log_entry: dict) -> dict:
    """
    Extract user identity details from Azure log entry
    :param azure_log_entry: Azure log entry
    :return: Dictionary containing user identity information
    """
    identity = azure_log_entry.get('identity', {})
    claims = identity.get('claims', {})
    
    # Get principal ID from either object identifier or email
    principal_id = claims.get("http://schemas.microsoft.com/identity/claims/objectidentifier")
    if not principal_id:
        principal_id = claims.get('http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress')
    
    return {
        "type": claims.get('idtyp', 'NO_USER_TYPE'),
        "principalId": principal_id,
        "details": identity
    }

def create_audit_event(azure_log_entry: dict) -> dict:
    """
    Transforms an Azure resource log entry into a CloudTrail Lake custom audit event
    :param azure_log_entry: Azure log entry
    :return: CloudTrail Lake custom audit event
    """
    # Set a default caller IP in case the event doesn't contain one
    # CT Lake requires a valid IP in the sourceIPAddress field
    DEFAULT_CALLER_IP = '1.0.0.0'
    RECIPIENT_ACCOUNT_ID = CLOUDTRAIL_LAKE_CHANNEL_ARN.split(':')[4]
    logger.debug("Received entry: %s", azure_log_entry)
    uid = str(uuid.uuid4())
    try:
        # Extract operation details
        operation_name = azure_log_entry.get('operationName', 'DEFAULT_EVENT_NAME')
        event_source = operation_name.split("/")[0] if operation_name else 'AZURE-LOGS'
        
        # Get timestamp
        event_time = parse(
            azure_log_entry.get('time', datetime.now(timezone.utc))
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Build response elements
        response_elements = {
            "statusCode": azure_log_entry.get('resultType', 'DEFAULT_STATUS_CODE'),
            "eventCategory": azure_log_entry.get('category', 'DEFAULT_CATEGORY'),
            "message": azure_log_entry.get('resultDescription', '')
        }
        
        custom_audit_event = {
            "version": "1.0",
            "userIdentity": get_user_identity_details(azure_log_entry),
            "eventSource": event_source,
            "eventName": operation_name,
            "eventTime": event_time,
            "UID": uid,
            "responseElements": response_elements,
            "errorCode": azure_log_entry.get('resultType', 'DEFAULT_ERROR_CODE'),
            "errorMessage": azure_log_entry.get('resultSignature', 'DEFAULT_ERROR_MSG'),
            "sourceIPAddress": azure_log_entry.get('callerIpAddress', DEFAULT_CALLER_IP),
            "recipientAccountId": RECIPIENT_ACCOUNT_ID,
            "additionalEventData": azure_log_entry.get('properties', {})
        }

        audit_event = {
            "eventData": json.dumps(custom_audit_event),
            "id": uid
        }
        logger.info("Transformed entry: %s", audit_event)
    except (KeyError, ValueError, TypeError) as err:
        logger.error("Failed to transform entry: %s", err)
        raise
    return audit_event


def batch_generator(events, batch_size):
    """
    Generator function to yield batches of events
    :param events: List of events to process
    :param batch_size: Size of each batch
    :yield: Batch of events
    """
    for i in range(0, len(events), batch_size):
        yield events[i:i + batch_size]


def ingest_audit_events(events: list) -> dict:
    """
    Ingests a batch of custom audit events into CloudTrail Lake
    :param events: List of custom audit events
    :return: Ingestion result
    """
    logger.debug("Ingesting custom events: %s", events)
    try:
        all_failed = []
        all_successful = []

        for batch in batch_generator(events, MAX_AUDIT_EVENTS_PER_INGESTION):
            output = cloudtrail_client.put_audit_events(
                auditEvents=batch,
                channelArn=CLOUDTRAIL_LAKE_CHANNEL_ARN)

            all_failed.extend(output['failed'])
            all_successful.extend(output['successful'])
            events = events[MAX_AUDIT_EVENTS_PER_INGESTION:]

        result = "ok" if len(all_failed) == 0 else "fail"
        logger.debug("Put audit events result: %s", result)
        return {
            'result': result, 
            'failed': all_failed, 
            'successful': all_successful
        }

    except ClientError as err:
        logger.error("Failed to put events: %s", err)
        raise

def send_to_queue(payload: dict, unique_id: str) -> bool:
    """
    Sends a payload to an SQS queue
    :param payload: Payload to send
    :param unique_id: Message deduplication ID
    :return: True if successful, else False
    """
    try:
        sqs_response = sqs_client.send_message(
            QueueUrl=FAILED_INGESTION_EVENTS_SQS_URL,
            MessageBody=json.dumps(payload),
            MessageDeduplicationId=unique_id,
            MessageGroupId="failed_ingestion_events"
            )
        logger.info("Sent event to failure queue %s: %s", FAILED_INGESTION_EVENTS_SQS_URL, sqs_response)
    except ClientError as err:
        logger.error("Failed to send event to failure queue %s: %s", FAILED_INGESTION_EVENTS_SQS_URL, err)
        raise

def process_log_file(storage_connection_string: str, container_name: str, log_file: str) -> bool:
    """
    Downloads and processes a single Azure log file
    :param storage_connection_string: Azure Storage connection string
    :param container_name: Azure Blob Storage container name
    :param log_file: Name of the log file to process
    """
    try:
        # Download the blob
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=log_file)
        file_content = blob_client.download_blob(encoding='UTF-8').readall()
        logger.info("Downloaded %s", log_file)
    except Exception as err:
        logger.error("Failed to download %s: %s", log_file, err)
        raise
    
    # Process each line and create audit events in CT schema
    audit_events = {}
    for line in file_content.splitlines():
        try:
            log_line = json.loads(line)
            audit_event = create_audit_event(log_line)
            audit_events[audit_event['id']] = audit_event
        except Exception as err:
            logger.error("Failed to process line: %s", err)
            # Enqueue events that failed to be transformed to CT schema
            send_to_queue(line, str(uuid.uuid4()))
    try:
        # Ingest events to CloudTrail
        ingest_result = ingest_audit_events([*audit_events.values()])
        logger.info("Ingested %s audit events into CloudTrail Lake channel [%s]", 
                    str(len(ingest_result['successful'])), CLOUDTRAIL_LAKE_CHANNEL_ARN)
    except Exception as err:
        logger.error("Failed to ingest events: %s", err)
        raise

    # Handle failed events
    if ingest_result['result'] != "ok":
        logger.warning("The following audit events were not ingested to CloudTrail Lake successfully: %s", 
                      ingest_result['failed'])
        if FAILED_INGESTION_EVENTS_SQS_URL and len(ingest_result['failed']) > 0:
            failed_ids = [e['id'] for e in ingest_result['failed']]
            for failed_id in failed_ids:
                send_to_queue(audit_events[failed_id], failed_id)
    
    return True if ingest_result['result'] == "ok" else False

def lambda_handler(event, lambda_context):
    """
    Main Lambda handler that coordinates the aggregation and processing of log files
    """ 
    logger.info("Received event: %s", event)
    for message in event['Records']:
        log_file = json.loads(message['body']).get('log_file')
        logger.info("Processing log file: %s", log_file)
        # Process the log file. Delete it from the queue if processed successfully.
        if process_log_file(
            storage_connection_string=AZURE_STORAGE_CONNECTION_STRING,
            container_name=AZ_STORAGE_CONTAINER_NAME,
            log_file=log_file
        ):
            sqs_client.delete_message(
                QueueUrl=LOG_FILES_TO_PROCESS_SQS_URL,
                ReceiptHandle=message['receiptHandle']
            )
        else:
            raise Exception("Failed to process log file: %s", log_file)
