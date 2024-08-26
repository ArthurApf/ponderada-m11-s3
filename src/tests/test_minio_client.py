import pytest
from unittest.mock import patch, MagicMock
from data_pipeline.minio_client import upload_file, download_file, create_bucket_if_not_exists

@patch('data_pipeline.minio_client.minio_client')
def test_upload_file(mock_minio_client):
    mock_bucket = MagicMock()
    mock_minio_client.fput_object = mock_bucket
    upload_file('test-bucket', 'test_file.txt')
    mock_minio_client.fput_object.assert_called_once_with('test-bucket', 'test_file.txt', 'test_file.txt')

@patch('data_pipeline.minio_client.minio_client')
def test_download_file(mock_minio_client):
    mock_bucket = MagicMock()
    mock_minio_client.fget_object = mock_bucket
    download_file('test-bucket', 'test_file.txt', 'local_test_file.txt')
    mock_minio_client.fget_object.assert_called_once_with('test-bucket', 'test_file.txt', 'local_test_file.txt')

@patch('data_pipeline.minio_client.minio_client')
def test_create_bucket_if_not_exists(mock_minio_client):
    mock_minio_client.bucket_exists = MagicMock(return_value=False)
    mock_minio_client.make_bucket = MagicMock()
    create_bucket_if_not_exists('test-bucket')
    mock_minio_client.make_bucket.assert_called_once_with('test-bucket')
