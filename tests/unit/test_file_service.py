
import moto
import boto3
import pytest
from src.services.file_service import FileService

REGION = 'us-east-1'

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
        
        bucket_name = 'test_bucket_name'
        s3 = mock_session.resource('s3')
        s3.create_bucket(Bucket=bucket_name)
        yield mock_session
        

"""
Test file service
"""
@pytest.mark.unit
def test_file_service(mock_session):
    file_service = FileService(bucket_name='test_bucket_name', prefix='test_prefix')
    assert file_service.get_buckets() == ['test_bucket_name']