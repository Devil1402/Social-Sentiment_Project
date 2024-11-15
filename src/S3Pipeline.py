import boto3
from botocore.exceptions import ClientError
import pandas as pd
import logging
import io


class S3Pipeline:
    def __init__(self):
        """
        Initialize S3 client using AWS's credential chain.

        Credentials are safely loaded from (in order):
        1. Environment variables
        2. Shared credential file (~/.aws/credentials)
        3. AWS IAM role for EC2/ECS/Lambda
        """
        self.logger = logging.getLogger(__name__)
        try:
            self.s3_client = boto3.client("s3")
            self.logger.info("Successfully initialized S3 client")
        except Exception as e:
            self.logger.error(f"Failed to initialize S3 client: {str(e)}")
            raise

    def upload_file(self, file_path, bucket_name, object_name=None):
        """
        Upload a file to S3 bucket

        :param file_path: File to upload
        :param bucket_name: Bucket to upload to
        :param object_name: S3 object name. If not specified, file_path is used
        :return: True if file was uploaded, else False
        """
        if object_name is None:
            object_name = file_path

        try:
            self.s3_client.upload_file(file_path, bucket_name, object_name)
            self.logger.info(
                f"Successfully uploaded {file_path} to {bucket_name}/{object_name}"
            )
            return True
        except ClientError as e:
            self.logger.error(f"Failed to upload file: {str(e)}")
            return False

    def download_file(self, bucket_name, object_name, file_path):
        """
        Download a file from S3 bucket

        :param bucket_name: Bucket to download from
        :param object_name: S3 object name to download
        :param file_path: Local path to save the file
        :return: True if file was downloaded, else False
        """
        try:
            self.s3_client.download_file(bucket_name, object_name, file_path)
            self.logger.info(
                f"Successfully downloaded {bucket_name}/{object_name} to {file_path}"
            )
            return True
        except ClientError as e:
            self.logger.error(f"Failed to download file: {str(e)}")
            return False

    def read_csv_to_dataframe(self, bucket_name, object_key, **pandas_kwargs):
        """
        Read a CSV file from S3 directly into a pandas DataFrame

        :param bucket_name: Name of the S3 bucket
        :param object_key: Path to the CSV file in the bucket
        :param pandas_kwargs: Additional arguments to pass to pd.read_csv
        :return: pandas DataFrame or None if operation fails
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)

            csv_content = response["Body"].read()

            df = pd.read_csv(io.BytesIO(csv_content), **pandas_kwargs)

            self.logger.info(f"Successfully read CSV from {bucket_name}/{object_key}")
            return df

        except ClientError as e:
            self.logger.error(f"Failed to read CSV from S3: {str(e)}")
            return None
        except pd.errors.EmptyDataError:
            self.logger.error("CSV file is empty")
            return None
        except Exception as e:
            self.logger.error(f"Error processing CSV file: {str(e)}")
            return None

    def list_buckets(self):
        """
        List all S3 buckets

        :return: List of bucket names
        """
        try:
            response = self.s3_client.list_buckets()
            buckets = [bucket["Name"] for bucket in response["Buckets"]]
            self.logger.info(f"Successfully retrieved {len(buckets)} buckets")
            return buckets
        except ClientError as e:
            self.logger.error(f"Failed to list buckets: {str(e)}")
            return []

    def list_objects(self, bucket_name, prefix=""):
        """
        List objects in an S3 bucket

        :param bucket_name: Bucket to list objects from
        :param prefix: Only list objects beginning with prefix
        :return: List of object keys
        """
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            objects = []
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if "Contents" in page:
                    objects.extend([obj["Key"] for obj in page["Contents"]])
            self.logger.info(
                f"Successfully listed {len(objects)} objects in {bucket_name}"
            )
            return objects
        except ClientError as e:
            self.logger.error(f"Failed to list objects: {str(e)}")
            return []
