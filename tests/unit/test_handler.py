import json
import os
import moto
import boto3
import pytest
import base64
import zipfile
import io

from src import app

REGION = 'us-east-1'

"""
Fixture for mocking lambda event
"""
@pytest.fixture()
def apigw_event():
    """ Generates API GW Event"""

    return {
        "body": '{ "test": "body"}',
        "resource": "/{proxy+}",
        "requestContext": {
            "resourceId": "123456",
            "apiId": "1234567890",
            "resourcePath": "/{proxy+}",
            "httpMethod": "POST",
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "accountId": "123456789012",
            "identity": {
                "apiKey": "",
                "userArn": "",
                "cognitoAuthenticationType": "",
                "caller": "",
                "userAgent": "Custom User Agent String",
                "user": "",
                "cognitoIdentityPoolId": "",
                "cognitoIdentityId": "",
                "cognitoAuthenticationProvider": "",
                "sourceIp": "127.0.0.1",
                "accountId": "",
            },
            "stage": "prod",
        },
        "queryStringParameters": {"min_date": "2024-01-01"},
        "headers": {
            "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
            "Accept-Language": "en-US,en;q=0.8",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Mobile-Viewer": "false",
            "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
            "CloudFront-Viewer-Country": "US",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "X-Forwarded-Port": "443",
            "Host": "1234567890.execute-api.us-east-1.amazonaws.com",
            "X-Forwarded-Proto": "https",
            "X-Amz-Cf-Id": "aaaaaaaaaae3VYQb9jd-nvCd-de396Uhbp027Y2JvkCPNLmGJHqlaA==",
            "CloudFront-Is-Tablet-Viewer": "false",
            "Cache-Control": "max-age=0",
            "User-Agent": "Custom User Agent String",
            "CloudFront-Forwarded-Proto": "https",
            "Accept-Encoding": "gzip, deflate, sdch",
        },
        "pathParameters": {"proxy": "/examplepath"},
        "httpMethod": "POST",
        "stageVariables": {"baz": "qux"},
        "path": "/examplepath",
    }

"""
Fixture setting up the moto S3 mock
"""
@pytest.fixture(scope="session")
def mock_session() -> boto3.Session:
    with moto.mock_aws():
        mock_session = boto3.Session(
            aws_access_key_id="mock_access_key",
            aws_secret_access_key="mock_secret_key",
            region_name=REGION
        )
        
        bucket_name = 'tapiwam-data-src'
        s3 = mock_session.resource('s3')
        s3.create_bucket(Bucket=bucket_name)
        
        # Add a sample test files under test/
        s3.Object(bucket_name, 'test/test1.txt').put(Body='hello world 1')
        s3.Object(bucket_name, 'test/test2.txt').put(Body='hello world 2')
        s3.Object(bucket_name, 'test/test3.txt').put(Body='hello world 3')
        
        
        yield mock_session

"""
Fixture to set environment variables
"""
@pytest.fixture(autouse=True)
def mock_env_vars():
    os.environ['BUCKET_NAME'] = 'tapiwam-data-src'
    os.environ['BUCKET_PREFIX'] = 'test'
    
"""
Test lambda handler
"""
@pytest.mark.unit
def test_lambda_handler(apigw_event, mock_session):

    ret = app.lambda_handler(apigw_event, "")
    # data = json.loads(ret["body"])
    

    assert ret["statusCode"] == 200
    
    # Check headers for zip
    assert "Content-Type" in ret["headers"]
    assert ret["headers"]["Content-Type"] == "application/zip"
    
    # Check body for zip base64 string
    body = ret["body"]
    assert body is not None
    
    # Decode base64 body and check files in zip
    print("Decoding body.")
    body = base64.b64decode(body)
    print("Decoded body.")
    body = zipfile.ZipFile(io.BytesIO(body))
    print("Files in zip: " + str(body.namelist()))
    assert body.namelist() == ['test/test1.txt', 'test/test2.txt', 'test/test3.txt']
    