import json
import os
from .services import FileService
import base64


def encodeResponseBase64(body):
    return base64.b64encode(body).decode('utf-8')

def responseBuilder(statusCode: int, body: str, isBase64Encoded: bool = False, headers: dict = {}):
    return {
        "statusCode": statusCode,
        "body": body,
        "isBase64Encoded": isBase64Encoded,
        "headers": headers
    }


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
    # file_list = fs.list_files(min_date)
    memory_file_bytes = fs.download_and_zip(min_date)
    
    # Convert to base64 string
    memory_file = encodeResponseBase64(memory_file_bytes)
    zip_name = 'Sample.zip'
    
    return responseBuilder(statusCode=200, 
        body=memory_file, 
        isBase64Encoded=True, 
        headers={
            "Content-Type": "application/zip",
            "Content-Disposition": f"attachment; filename={zip_name}"
        }
    )
    
    # return  {
    #     "statusCode": 200,
    #     "body": memory_file,
    #     "headers": {
    #         "Content-Type": "application/zip",
    #         "Content-Disposition": f"attachment; filename={zip_name}"
    #     },
    #     'isBase64Encoded': True
    # }
