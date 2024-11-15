import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
import pandas as pd
from src.S3Pipeline import S3Pipeline


@pytest.fixture
def mock_boto3_client():
    """Fixture to create a mock boto3 client"""
    with patch("boto3.client") as mock_client:
        s3_client_mock = Mock()
        mock_client.return_value = s3_client_mock
        yield s3_client_mock


@pytest.fixture
def s3_client(mock_boto3_client):
    """Fixture to create an S3Client instance with mocked boto3 client"""
    return S3Pipeline()


class TestS3Client:
    def test_initialization_success(self, mock_boto3_client):
        """Test successful initialization of S3Client"""
        client = S3Pipeline()
        assert client.s3_client == mock_boto3_client

    def test_initialization_failure(self):
        """Test initialization failure of S3Client"""
        with patch("boto3.client", side_effect=Exception("Connection error")):
            with pytest.raises(Exception) as exc_info:
                S3Pipeline()
            assert "Connection error" in str(exc_info.value)

    def test_upload_file_success(self, s3_client, mock_boto3_client):
        """Test successful file upload"""
        mock_boto3_client.upload_file.return_value = None

        result = s3_client.upload_file(
            file_path="test.txt",
            bucket_name="test-bucket",
            object_name="test-object.txt",
        )

        assert result is True
        mock_boto3_client.upload_file.assert_called_once_with(
            "test.txt", "test-bucket", "test-object.txt"
        )

    def test_upload_file_failure(self, s3_client, mock_boto3_client):
        """Test file upload failure"""
        error_response = {"Error": {"Code": "403", "Message": "Access Denied"}}
        mock_boto3_client.upload_file.side_effect = ClientError(
            error_response, "upload_file"
        )

        result = s3_client.upload_file(
            file_path="test.txt",
            bucket_name="test-bucket",
            object_name="test-object.txt",
        )

        assert result is False
        mock_boto3_client.upload_file.assert_called_once()

    def test_download_file_success(self, s3_client, mock_boto3_client):
        """Test successful file download"""
        mock_boto3_client.download_file.return_value = None

        result = s3_client.download_file(
            bucket_name="test-bucket",
            object_name="test-object.txt",
            file_path="local-test.txt",
        )

        assert result is True
        mock_boto3_client.download_file.assert_called_once_with(
            "test-bucket", "test-object.txt", "local-test.txt"
        )

    def test_download_file_failure(self, s3_client, mock_boto3_client):
        """Test file download failure"""
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_boto3_client.download_file.side_effect = ClientError(
            error_response, "download_file"
        )

        result = s3_client.download_file(
            bucket_name="test-bucket",
            object_name="test-object.txt",
            file_path="local-test.txt",
        )

        assert result is False
        mock_boto3_client.download_file.assert_called_once()

    def test_read_csv_success(self, s3_client, mock_boto3_client):
        """Test successful CSV reading"""
        csv_content = b"name,age\nJohn,30\nJane,25"

        mock_body = Mock()
        mock_body.read.return_value = csv_content

        mock_response = {"Body": mock_body}
        mock_boto3_client.get_object.return_value = mock_response

        result_df = s3_client.read_csv_to_dataframe(
            bucket_name="test-bucket", object_key="test.csv"
        )

        expected_df = pd.DataFrame({"name": ["John", "Jane"], "age": [30, 25]})
        pd.testing.assert_frame_equal(result_df, expected_df)
        mock_boto3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="test.csv"
        )

    def test_read_csv_with_pandas_options(self, s3_client, mock_boto3_client):
        """Test CSV reading with custom pandas options"""
        csv_content = b"name,age\nJohn,30\nJane,25"

        mock_body = Mock()
        mock_body.read.return_value = csv_content

        mock_response = {"Body": mock_body}
        mock_boto3_client.get_object.return_value = mock_response

        result_df = s3_client.read_csv_to_dataframe(
            bucket_name="test-bucket", object_key="test.csv", usecols=["name"]
        )

        expected_df = pd.DataFrame({"name": ["John", "Jane"]})
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_read_csv_empty_file(self, s3_client, mock_boto3_client):
        """Test handling of empty CSV file"""
        csv_content = b""

        mock_body = Mock()
        mock_body.read.return_value = csv_content

        mock_response = {"Body": mock_body}
        mock_boto3_client.get_object.return_value = mock_response

        result_df = s3_client.read_csv_to_dataframe(
            bucket_name="test-bucket", object_key="empty.csv"
        )

        assert result_df is None

    def test_read_csv_s3_error(self, s3_client, mock_boto3_client):
        """Test handling of S3 access error"""
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_boto3_client.get_object.side_effect = ClientError(
            error_response, "get_object"
        )

        result_df = s3_client.read_csv_to_dataframe(
            bucket_name="test-bucket", object_key="nonexistent.csv"
        )

        assert result_df is None
        mock_boto3_client.get_object.assert_called_once()

    def test_list_buckets_success(self, s3_client, mock_boto3_client):
        """Test successful bucket listing"""
        mock_response = {"Buckets": [{"Name": "bucket1"}, {"Name": "bucket2"}]}
        mock_boto3_client.list_buckets.return_value = mock_response

        result = s3_client.list_buckets()

        assert result == ["bucket1", "bucket2"]
        mock_boto3_client.list_buckets.assert_called_once()

    def test_list_buckets_failure(self, s3_client, mock_boto3_client):
        """Test bucket listing failure"""
        error_response = {"Error": {"Code": "403", "Message": "Access Denied"}}
        mock_boto3_client.list_buckets.side_effect = ClientError(
            error_response, "list_buckets"
        )

        result = s3_client.list_buckets()

        assert result == []
        mock_boto3_client.list_buckets.assert_called_once()

    def test_list_objects_success(self, s3_client, mock_boto3_client):
        """Test successful object listing"""
        mock_paginator = Mock()
        mock_boto3_client.get_paginator.return_value = mock_paginator

        mock_pages = [
            {"Contents": [{"Key": "object1"}, {"Key": "object2"}]},
            {"Contents": [{"Key": "object3"}]},
        ]
        mock_paginator.paginate.return_value = mock_pages

        result = s3_client.list_objects(bucket_name="test-bucket", prefix="test/")

        assert result == ["object1", "object2", "object3"]
        mock_boto3_client.get_paginator.assert_called_once_with("list_objects_v2")
        mock_paginator.paginate.assert_called_once_with(
            Bucket="test-bucket", Prefix="test/"
        )

    def test_list_objects_empty(self, s3_client, mock_boto3_client):
        """Test object listing with empty bucket"""
        mock_paginator = Mock()
        mock_boto3_client.get_paginator.return_value = mock_paginator

        mock_pages = [{}]
        mock_paginator.paginate.return_value = mock_pages

        result = s3_client.list_objects(bucket_name="test-bucket", prefix="test/")

        assert result == []
        mock_boto3_client.get_paginator.assert_called_once()

    def test_list_objects_failure(self, s3_client, mock_boto3_client):
        """Test object listing failure"""
        # Configure mock to raise an error
        error_response = {"Error": {"Code": "403", "Message": "Access Denied"}}
        mock_boto3_client.get_paginator.side_effect = ClientError(
            error_response, "list_objects_v2"
        )

        result = s3_client.list_objects(bucket_name="test-bucket", prefix="test/")

        assert result == []
        mock_boto3_client.get_paginator.assert_called_once()
