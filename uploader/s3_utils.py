import boto3
from botocore.exceptions import ClientError
from pathlib import Path



def create_s3_bucket(bucket_name, region=None):
    """
    Create an S3 bucket in a specified region

    Parameters:
    - bucket_name: Name of the bucket to create
    - region: Region to create bucket in, e.g., 'us-east-1'
    
    Returns:
    - True if bucket created, else False
    """
    try:
        if region is None:
            # Get the region from AWS configuration
            region = boto3.session.Session().region_name
        
        s3_client = boto3.client('s3', region_name=region)
        # Check if bucket already exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' already exists")
            return True
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                # Bucket does not exist, create it
                try:
                    if region == 'us-east-1':
                        bucket = s3_client.create_bucket(Bucket=bucket_name)
                    else:
                        bucket = s3_client.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={
                                'LocationConstraint': region
                            }
                        )
                    
                    # Wait for bucket to exist
                    waiter = s3_client.get_waiter('bucket_exists')
                    waiter.wait(
                        Bucket=bucket_name,
                    )
                    
                    print(f"Successfully created bucket '{bucket_name}' in region '{region}'")
                    return True
                    
                except ClientError as create_error:
                    print(f"Error creating bucket: {create_error}")
                    return False
                    
            elif error_code == 403:
                print("Error: Forbidden. This could mean:")
                print("1. The bucket exists but is owned by a different account")
                print("2. Your IAM user doesn't have sufficient permissions")
                return False    
            else:
                print(f"Error checking bucket: {e}")
                return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def get_aws_account_id():
    """
    Get the current AWS account ID
    
    Returns:
    - AWS account ID as string
    """
    try:
        sts = boto3.client('sts')
        return sts.get_caller_identity()['Account']
    except Exception as e:
        print(f"Error getting AWS account ID: {e}")
        return None
    

def upload_to_s3(directory, bucket_name, region):
    """Upload all CSV files in a directory to the specified S3 bucket."""
    s3 = boto3.client('s3', region_name=region)
    directory = Path(directory)

    if not directory.exists():
        raise ValueError(f"[S3] Directory '{directory}' does not exist.")

    files_uploaded = 0
    for file in directory.glob("*.csv"):
        s3_key = file.name
        try:
            s3.upload_file(str(file), bucket_name, s3_key)
            print(f"[S3] Uploaded '{file.name}' to bucket '{bucket_name}' as '{s3_key}'")
            files_uploaded += 1
        except Exception as e:
            print(f"[S3] Error uploading {file.name}: {e}")
    
    if files_uploaded == 0:
        print("[S3] No CSV files found in the directory.")
    else:
        print(f"[S3] Uploaded {files_uploaded} file(s) to S3.")



# Example usage
if __name__ == "__main__":
    bucket_name = "testing-s3-for-redshift-conversion"
    # Get your AWS account ID
    create_s3_bucket(bucket_name, region='us-east-1')
    upload_to_s3('./data', bucket_name = bucket_name, region='us-east-1')
