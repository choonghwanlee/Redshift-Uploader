import boto3
import json
import botocore


def create_iam_role(role_name):
    """
    Create an IAM role for Redshift with permission to access S3.
    If the role already exists, it will return its ARN.
    """
    iam = boto3.client('iam')

    try:
        # Check if the role already exists
        response = iam.get_role(RoleName=role_name)
        print(f"[IAM] Role '{role_name}' already exists.")
        return response['Role']['Arn']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchEntity':
            raise
        print(f"[IAM] Creating IAM role '{role_name}'...")

    # Define trust policy for Redshift
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {
                "Service": "redshift.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }]
    }

    # Create the role
    try:
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows Redshift to access S3 for COPY operations"
        )
        role_arn = response['Role']['Arn']
        print(f"[IAM] Role '{role_name}' created with ARN: {role_arn}")
    except Exception as e:
        raise RuntimeError(f"[IAM] Failed to create IAM role: {e}")

    # Attach AmazonS3ReadOnlyAccess policy
    try:
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )
        print(f"[IAM] Attached AmazonS3ReadOnlyAccess to role '{role_name}'.")
    except Exception as e:
        raise RuntimeError(f"[IAM] Failed to attach policy: {e}")

    return role_arn
