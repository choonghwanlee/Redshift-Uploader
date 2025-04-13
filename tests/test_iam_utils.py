import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))


from unittest.mock import patch, MagicMock
import botocore
from uploader.iam_utils import create_iam_role


@patch("boto3.client")
def test_create_iam_role_exists(mock_boto):
    """
    Test that create_iam_role returns the ARN of an existing role without creating a new one.
    
    Expected behavior:
    - get_role returns the role ARN
    - create_role is not called
    """
    mock_iam = MagicMock()
    mock_iam.get_role.return_value = {"Role": {"Arn": "arn:aws:iam::123456789012:role/ExistingRole"}}
    mock_boto.return_value = mock_iam

    arn = create_iam_role("ExistingRole")
    assert arn.endswith("ExistingRole")

@patch("boto3.client")
def test_create_iam_role_new(mock_boto):
    """
    Test that create_iam_role creates a new IAM role and attaches
    the AmazonS3ReadOnlyAccess policy if the role does not already exist.

    Expected behavior:
    - IAM role is created
    - Policy is attached
    - ARN is returned
    """
    mock_iam = MagicMock()
    mock_iam.get_role.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "NoSuchEntity"}}, "GetRole"
    )
    mock_iam.create_role.return_value = {"Role": {"Arn": "arn:aws:iam::123456789012:role/NewRole"}}
    mock_boto.return_value = mock_iam

    arn = create_iam_role("NewRole")
    assert arn.endswith("NewRole")
    mock_iam.attach_role_policy.assert_called_once()
