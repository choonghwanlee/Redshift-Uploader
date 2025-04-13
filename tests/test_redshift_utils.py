import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pytest
from unittest.mock import patch, MagicMock
from uploader.redshift_utils import (
    create_redshift_cluster,
    authorize_redshift_ingress,
    get_redshift_connection,
    create_table_and_copy
)


@patch("boto3.client")
def test_create_redshift_cluster_existing(mock_boto):
    """
    Test that create_redshift_cluster does not attempt to recreate
    the cluster if it already exists.
    
    Expected behavior:
    - The function calls describe_clusters and exits early if the cluster is found.
    - No call to create_cluster is made.
    """
    mock_redshift = MagicMock()
    mock_redshift.describe_clusters.return_value = {"Clusters": [{"ClusterIdentifier": "test-cluster"}]}
    mock_boto.return_value = mock_redshift

    create_redshift_cluster("test-cluster", "testdb", "admin", "pw", "arn:aws:iam::123:role/test", "us-east-1")

    mock_redshift.describe_clusters.assert_called_once()
    mock_redshift.create_cluster.assert_not_called()


@patch("boto3.client")
@patch("requests.get")
def test_authorize_redshift_ingress(mock_requests_get, mock_boto):
    """
    Test that authorize_redshift_ingress correctly retrieves the VPC security group,
    fetches the public IP address, and attempts to create a TCP ingress rule on port 5439.
    
    Expected behavior:
    - Redshift cluster details are fetched.
    - The public IP is resolved.
    - EC2 ingress rule is called with the IP in CIDR format and port range 5439.
    """
    mock_redshift = MagicMock()
    mock_ec2 = MagicMock()
    mock_boto.side_effect = lambda service, region_name=None: {
        "redshift": mock_redshift,
        "ec2": mock_ec2
    }[service]

    mock_redshift.describe_clusters.return_value = {
        "Clusters": [{
            "VpcId": "vpc-abc",
            "VpcSecurityGroups": [{"VpcSecurityGroupId": "sg-123"}]
        }]
    }

    mock_requests_get.return_value.text = "1.2.3.4"

    authorize_redshift_ingress("test-cluster", region="us-east-1")

    mock_ec2.authorize_security_group_ingress.assert_called_once()
    call_args = mock_ec2.authorize_security_group_ingress.call_args[1]
    assert call_args["IpPermissions"][0]["FromPort"] == 5439
    assert call_args["IpPermissions"][0]["ToPort"] == 5439
    assert "1.2.3.4/32" in call_args["IpPermissions"][0]["IpRanges"][0]["CidrIp"]


@patch("psycopg2.connect")
@patch("boto3.client")
def test_get_redshift_connection(mock_boto, mock_connect):
    """
    Test that get_redshift_connection correctly fetches the cluster endpoint
    and opens a psycopg2 connection using the expected parameters.
    
    Expected behavior:
    - describe_clusters returns the correct endpoint and port
    - psycopg2.connect is called with the correct host, dbname, user, and port
    - The returned psycopg2 connection is passed back
    """
    mock_redshift = MagicMock()
    mock_redshift.describe_clusters.return_value = {
        "Clusters": [{
            "Endpoint": {
                "Address": "redshift-cluster.example.com",
                "Port": 5439
            }
        }]
    }
    mock_boto.return_value = mock_redshift
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    result = get_redshift_connection("test-cluster", "testdb", "admin", "pw", "us-east-1")

    mock_connect.assert_called_once_with(
        dbname="testdb",
        user="admin",
        password="pw",
        host="redshift-cluster.example.com",
        port=5439,
        connect_timeout=10
    )
    assert result == mock_conn

@patch("uploader.redshift_utils.requests.get")
@patch("psycopg2.connect")
@patch("boto3.client")
def test_create_table_and_copy(mock_boto, mock_connect, mock_requests_get):
    """
    Test create_table_and_copy() with mocked Redshift, EC2, and psycopg2.
    
    This test validates that:
    - Redshift and EC2 clients are used to authorize ingress
    - psycopg2 is used to connect and execute SQL
    - COPY statement is executed
    """
    # Mock public IP for requests.get()
    mock_requests_get.return_value.text = "123.123.123.123"

    # Mock psycopg2 connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    # Mock Redshift describe_clusters
    mock_redshift = MagicMock()
    mock_redshift.describe_clusters.return_value = {
        "Clusters": [{
            "Endpoint": {"Address": "redshift-cluster.example.com", "Port": 5439},
            "VpcId": "vpc-abc123",
            "VpcSecurityGroups": [{"VpcSecurityGroupId": "sg-abc123"}]
        }]
    }

    # Mock EC2 authorize ingress
    mock_ec2 = MagicMock()
    mock_ec2.authorize_security_group_ingress.return_value = {}

    mock_boto.side_effect = lambda service, region_name=None: {
        "redshift": mock_redshift,
        "ec2": mock_ec2
    }[service]

    # Run the function
    create_table_and_copy(
        table_name="test_table",
        create_sql="CREATE TABLE test_table (id INT);",
        bucket="my-test-bucket",
        filename="test.csv",
        cluster_id="my-redshift-cluster",
        db_name="testdb",
        user="admin",
        password="password123",
        region="us-east-1",
        role_arn="arn:aws:iam::123456789012:role/TestRole"
    )

    # Assert Redshift connection was made
    mock_connect.assert_called_once()
    mock_cursor.execute.assert_any_call("DROP TABLE IF EXISTS test_table")
    mock_cursor.execute.assert_any_call("CREATE TABLE test_table (id INT);")
    assert any("COPY test_table" in str(call.args[0]) for call in mock_cursor.execute.call_args_list)
    mock_conn.commit.assert_called_once()
