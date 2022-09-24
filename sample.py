import boto3
import json

S3 = boto3.client("s3")
s3_res = boto3.resource('s3')
trigger_bucket = os.environ.get("TRIGGER_BUCKET_NAME", "")

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
    logger.info("Entered write to s3 method.")
    try:
        s3object = s3_res.Object(bucket, key)
        s3object.put(Body=(bytes(json.dumps(res_json).encode('UTF-8'))))
        return True
    except Exception as error:
        error_message = "Exception occurred in hierarchy_update_lambda :: \
            write_to_s3 :: {error}".format(error=str(error))
        logger.error(error_message)
        raise Exception(error_message) from error
    finally:
        logger.info("Exited the write_to_s3 method.")



write_to_s3(trigger_bucket, trigger_key_path, response)