import json
import os
from .services import FileService


# import requests
def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    
    print("Received event: " + json.dumps(event, indent=2))
    
    fs = FileService(os.environ.get("BUCKET_NAME"), os.environ.get("BUCKET_PREFIX"))
    min_date = event['queryStringParameters']['min_date']
    
    if min_date is None:
        min_date = '2023-01-01'
    
    # Get buckets
    file_list = fs.list_files(min_date)
    

    return {
        "statusCode": 200,
        "body": json.dumps({
            "file_list": file_list
        }),
    }
