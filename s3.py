import sys
import json
import re
import logging
import boto3
import psycopg2
from psycopg2 import extras
from psycopg2.errors import UndefinedTable
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv,
                          ['ARGS_YOU_WANT_FOR_GULE'])
logging_level = args.get("LOG_LEVEL", "")
logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(logging_level))
logging.basicConfig(level=logging.getLevelName(logging_level))
s3_res = boto3.resource('s3')


def read_from_s3(bucket, key):
    """Utility function to read from s3
    Args:
        bucket (string): bucket
        key (string): key
    Raises:
        Exception: S3 Exception
    Returns:
        [str]: data
    """
    try:
        s3_client = boto3.client('s3')
        data = s3_client.get_object(Bucket=bucket, Key=key)
        data = data['Body'].read()
        return data
    except Exception as error:
        error_message = "Exception occurred in  ::\
             read_from_s3 :: {error}".format(error=str(error))
        logger.error(error_message)
        raise Exception(error_message) from error


def parse_s3_url(s3_url):
    """Utility function to parse s3 url
    Args:
        s3_url (string): S3 url
    Returns:
        bucket, file_key: bucket and file_key
    """
    bucket, file_key = re.match(r"s3:\/\/(.+?)\/(.+)", s3_url).groups()
    return bucket, file_key


def write_to_s3(bucket, key, res_json):
    """Utility function to S3 write
    Args:
        bucket (string): bucket
        key (string): key
        res_json (string): res_json
    Raises:
        Exception: S3 exception
    Returns:
        dict: Response
    """
    try:
        s3object = s3_res.Object(bucket, key)
        s3object.put(Body=(bytes(json.dumps(res_json).encode('UTF-8'))))
        return True
    except Exception as error:
        error_message = "Exception occurred in  :: \
            write_to_s3 :: {error}".format(error=str(error))
        logger.error(error_message)
        raise Exception(error_message) from error


def main():
    """Main method"""
    logger.info('Message you wanted')
    try:
        logger.info('include all the function calls')
    except UndefinedTable as error:
        exception_message = "Exceptional Message"
        logger.error(exception_message)
