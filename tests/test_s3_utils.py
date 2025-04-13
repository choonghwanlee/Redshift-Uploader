import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from unittest.mock import patch, MagicMock
from uploader.s3_utils import create_s3_bucket, upload_to_s3
from botocore.exceptions import ClientError



@patch("boto3.client")
def test_create_s3_bucket_exists(mock_boto):
    """
    Test that create_s3_bucket does nothing if the bucket already exists.
    
    Expected behavior:
    - head_bucket does not raise an exception
    - The function returns True
    """
    mock_s3 = MagicMock()
    mock_s3.head_bucket.return_value = {}
    mock_boto.return_value = mock_s3

    assert create_s3_bucket("existing-bucket") is True


@patch("boto3.client")
def test_create_s3_bucket_new(mock_boto):
    """
    Test that create_s3_bucket creates a bucket if it does not exist.

    Expected behavior:
    - head_bucket raises ClientError with 404
    - create_bucket and get_waiter().wait() are called
    - The function returns True
    """
    mock_s3 = MagicMock()
    mock_s3.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadBucket"
    )
    mock_s3.create_bucket.return_value = {}
    mock_s3.get_waiter.return_value.wait.return_value = True
    mock_boto.return_value = mock_s3

    assert create_s3_bucket("new-bucket", region="us-east-1") is True


@patch("boto3.client")
def test_upload_to_s3_success(mock_boto):
    """
    Test that upload_to_s3 uploads all CSV files in a directory to S3.

    Expected behavior:
    - Files are detected and uploaded
    - S3 client receives upload_file() call for each CSV
    """
    mock_s3 = MagicMock()
    mock_boto.return_value = mock_s3

    path = Path("tests/test_data")
    path.mkdir(exist_ok=True, parents=True)
    (path / "upload_test.csv").write_text("col1,col2\nval1,val2")

    upload_to_s3(str(path), "test-bucket", region="us-east-1")
    mock_s3.upload_file.assert_called_once()
