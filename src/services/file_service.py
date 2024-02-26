import boto3
import zipfile
import io
from io import BytesIO
from botocore.exceptions import ClientError
import traceback
import base64

"""
File Service class that will have methods to help with files. These will include
- Fetch files form S3 past a given date and for a given prefix
- Zip files from S3 into a single file
"""
class FileService:
    
    def __init__(self, bucket_name: str, prefix: str) -> None:
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3_client = boto3.client('s3')
        
    def list_files(self, min_date: str):
        print(f"Listing files from S3. Bucket: {self.bucket_name}, Prefix: {self.prefix}, Min Date: {min_date}")
        s3 = self.s3_client
        
        # Fetch file list from S3
        response = s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.prefix) 
                                    #   StartAfter=min_date)
                                    
        # Extratc contents keys from response
        response = [item['Key'] for item in response['Contents']]
        
        print(f"Fetched file list from S3. Bucket: {self.bucket_name}, Prefix: {self.prefix}, Min Date: {min_date}")

        # Return the list of files
        return response

    # Function to download files from list in S3 and return them in a zip
    def download_files(self, min_date: str, file_list: list):
        print(f"Downloading files from S3. Bucket: {self.bucket_name}, File List: {file_list}")
        s3 = self.s3_client
        
        # Download files from S3
        s3_items = []
        for file in file_list:
            print("Processing file: " + file)
            item = {}
            item['Key'] = file
            obj = s3.get_object(Bucket=self.bucket_name, Key=file)
            # s3_items.append(obj)
            item['Body'] = obj['Body'].read()
            s3_items.append(item)
            print(f"Downloaded file from S3. Bucket: {self.bucket_name}, File: {file}")
            
        # Return the list of files
        print(f"Downloaded files from S3. Bucket: {self.bucket_name}, File List: {file_list}")
        return s3_items

    def zip_files(self, s3_items: list):
        print(f"Zipping files. Bucket: {self.bucket_name}, File List: {len(s3_items)}")
        
        # Zip files
        memory_file = io.BytesIO()
        with zipfile.ZipFile(memory_file, 'w') as zipf:
            for item in s3_items:
                zipf.writestr(item['Key'], item['Body'])
        memory_file.seek(0)
        
        # Convert to base64 string
        memory_file = base64.b64encode(memory_file.getvalue())
        
        print(f"Zipped files to memory file. Bucket: {self.bucket_name}, File List: {len(s3_items)}")
        return memory_file
    
    def download_and_zip(self, min_date: str):
        try:
            file_list = self.list_files(min_date)
            s3_items = self.download_files(min_date, file_list)
            memory_file = self.zip_files(s3_items)

        except Exception as e:
            print(f"Couldn't download files. Error: {e}")
            traceback.print_exc()
            raise
        
        return memory_file
    
    def get_buckets(self):
        s3_client = self.s3_client
        buckets =[]

        try:
            # Call S3 to list buckets
            response = s3_client.list_buckets()
            
            # Add buckets to list
            for bucket in response['Buckets']:
                buckets += {bucket["Name"]}

        except ClientError:
            print("Couldn't get buckets.")
            raise

        return buckets