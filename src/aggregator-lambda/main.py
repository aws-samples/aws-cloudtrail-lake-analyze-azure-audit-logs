"""
Aggregates Azure resource log files not processed since last invocation, 
and enqueues each log file path to an SQS queue for processing. 

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
import logging
from datetime import datetime, timezone
from dateutil.parser import parse
import boto3
from azure.storage.blob import BlobServiceClient


# Logging settings
logging.basicConfig()
logger = logging.getLogger()
if 'LOG_LEVEL' in os.environ:
    logger.setLevel(os.environ['LOG_LEVEL'])
else:
    logger.setLevel(logging.INFO)

# Azure settings
AZURE_STORAGE_CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']
AZ_STORAGE_CONTAINER_NAME = os.environ['AZURE_STORAGE_CONTAINER_NAME']

# Queue settings
LOG_FILES_TO_PROCESS_SQS_URL = os.environ['LOG_FILES_TO_PROCESS_SQS_URL']

# DynamoDB settings
PROCESSED_TIMESTAMP_TABLE = os.environ['PROCESSED_TIMESTAMP_TABLE']

# Boto3 resources
session = boto3.Session()
sqs_client = session.client('sqs')
dynamodb = session.resource('dynamodb')
processed_timestamp_table = dynamodb.Table(PROCESSED_TIMESTAMP_TABLE)


def send_to_queue(log_file: str) -> bool:
    """
    Sends a payload to an SQS queue
    :param payload: Payload to send
    :param unique_id: Message deduplication ID
    :return: True if successful, else False
    """
    try:
        sqs_response = sqs_client.send_message(
            QueueUrl=LOG_FILES_TO_PROCESS_SQS_URL,
            MessageBody=json.dumps({'log_file': log_file}),
            MessageDeduplicationId=log_file,
            MessageGroupId="log_files_to_process"
            )
        logger.info("Sent file %s to queue for processing: %s", log_file, sqs_response)
    except Exception as err:
        logger.error("Failed to send file %s to queue %s: %s", log_file, LOG_FILES_TO_PROCESS_SQS_URL, err)
        raise err
    return True

def get_azure_log_files(storage_connection_string: str, container_name: str) -> list:
    """
    Retrieves a list of Azure log files from blob storage and discards any that were not modified
    since the last invocation.
    :param storage_connection_string: Azure Storage connection string
    :param container_name: Azure Blob Storage container name
    :return: List of log file paths
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    except Exception as e:
        logger.error("Failed to connect to Azure Storage: %s", e)
        raise
    logger.info("Connected to Azure Storage")
    start_date = get_last_processed_timestamp()
    logger.info("Last processed timestamp: %s", start_date)
    container_client = blob_service_client.get_container_client(container_name)
    blobs = container_client.list_blobs()
    log_files = [blob.name for blob in blobs if blob.name.endswith("/PT1H.json") and blob.last_modified >= start_date]
    logger.info("Found %s log files", str(len(log_files)))
    return log_files

def get_last_processed_timestamp():
    """
    Get the timestamp for the last successful log file ingestion
    :return Timestamp of previous invocation
    """
    try:
        response = processed_timestamp_table.get_item(
            Key={'service': 'azure-logs'},
            ProjectionExpression='#t',
            ExpressionAttributeNames = {'#t': 'timestamp'}
        )
        if 'Item' not in response:
            raise KeyError('Timestamp not found in last processed table.')
        timestamp = response['Item']['timestamp']
        return parse(timestamp).replace(tzinfo=timezone.utc)
    except KeyError:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    except Exception as e:
        logger.error("Unable to fetch last processed time from DynamoDB: %s", e)
        raise

def update_last_processed_timestamp(timestamp):
    """
    Update the timestamp for the last successful log file ingestion
    """
    try:
        processed_timestamp_table.put_item(
            Item={
                'service': 'azure-logs',
                'timestamp': timestamp
            }
        )
    except Exception as e:
        logger.error("Unable to update last processed time in DynamoDB: %s", e)
        raise

def lambda_handler(event, lambda_context):
    """
    Main Lambda handler that aggregates log files and sends them
    to a queue for processing.
    """
    try:
        # Fetch all log files
        log_files = get_azure_log_files(AZURE_STORAGE_CONNECTION_STRING, AZ_STORAGE_CONTAINER_NAME)

        # Submit each log file to a queue for processing
        for log_file in log_files:
            try:
                send_to_queue(log_file)
                logger.info("Sent %s to queue for processing", log_file)
            except Exception as e:
                logger.error("Failed to send %s to queue for processing: %s", log_file, e)

        # Update the last processed time in DynamoDB
        update_last_processed_timestamp(datetime.now(timezone.utc).isoformat())
        logger.info("Updated last processed timestamp: %s", datetime.now(timezone.utc).isoformat())

        return {
            'statusCode': 200,
            'body': f'Initiated processing of {len(log_files)} log files'
        }
    except Exception as e:
        logger.error("Failed to process log files: %s", e)
        return {
            'statusCode': 500,
            'body': f'Failed to process log files: {e}'
        }
