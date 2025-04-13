import time
import boto3
import psycopg2
import botocore
import requests

def create_redshift_cluster(cluster_id, db_name, user, password, role_arn, region):
    """Creates a Redshift cluster with the provided config if it does not already exist."""
    redshift = boto3.client('redshift', region_name=region)
    
    try:
        redshift.describe_clusters(ClusterIdentifier=cluster_id)
        print(f"[Redshift] Cluster '{cluster_id}' already exists.")
        return
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'ClusterNotFound':
            raise
        print(f"[Redshift] Creating cluster '{cluster_id}'...")

    redshift.create_cluster(
        ClusterIdentifier=cluster_id,
        NodeType='dc2.large',
        MasterUsername=user,
        MasterUserPassword=password,
        DBName=db_name,
        ClusterType='single-node',
        IamRoles=[role_arn],
        PubliclyAccessible=True,

    )

    # Wait until the cluster is available
    waiter = redshift.get_waiter('cluster_available')
    print("[Redshift] Waiting for cluster to become available...")
    waiter.wait(ClusterIdentifier=cluster_id)
    print("[Redshift] Cluster is now available.")

def authorize_redshift_ingress(cluster_id, region="us-east-1"):
    redshift = boto3.client("redshift", region_name=region)
    ec2 = boto3.client("ec2", region_name=region)

    # Step 1: Get Redshift cluster details
    try:
        cluster_info = redshift.describe_clusters(ClusterIdentifier=cluster_id)["Clusters"][0]
    except botocore.exceptions.ClientError as e:
        raise RuntimeError(f"[ERROR] Failed to describe Redshift cluster: {e}")

    vpc_id = cluster_info["VpcId"]
    sg_id = cluster_info["VpcSecurityGroups"][0]["VpcSecurityGroupId"]

    print(f"[INFO] Cluster is in VPC: {vpc_id}")
    print(f"[INFO] Using Security Group: {sg_id}")

    # Step 2: Get your current IP
    try:
        my_ip = requests.get("https://api.ipify.org").text.strip()
        cidr_ip = f"{my_ip}/32"
    except Exception as e:
        raise RuntimeError(f"[ERROR] Failed to fetch public IP: {e}")

    print(f"[INFO] Your IP address: {cidr_ip}")

    # Step 3: Add Inbound Rule
    try:
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5439,
                    "ToPort": 5439,
                    "IpRanges": [
                        {
                            "CidrIp": cidr_ip,
                            "Description": "Allow Redshift access from CLI uploader"
                        }
                    ]
                }
            ]
        )
        print(f"[✅] Ingress rule added: TCP 5439 from {cidr_ip}")
    except botocore.exceptions.ClientError as e:
        if "InvalidPermission.Duplicate" in str(e):
            print(f"[INFO] Ingress rule for {cidr_ip} already exists.")
        else:
            raise RuntimeError(f"[ERROR] Could not add ingress rule: {e}")


def get_redshift_connection(cluster_id, db_name, user, password, region):
    """Fetch the connection info and return a psycopg2 connection."""
    redshift = boto3.client('redshift', region_name=region)
    response = redshift.describe_clusters(ClusterIdentifier=cluster_id)
    cluster_info = response['Clusters'][0]

    host = cluster_info['Endpoint']['Address']
    port = cluster_info['Endpoint']['Port']

    conn = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port,
        connect_timeout=10  
    )
    return conn

def create_table_and_copy(table_name, create_sql, bucket, filename, cluster_id, db_name, user, password, region, role_arn):
    """Create a Redshift table and load data from S3."""
    print(f"[Redshift] Creating Inbound rule for '{cluster_id}' to enable Redshift access...")
    authorize_redshift_ingress(cluster_id, region)
    print(f"[Redshift] Connecting to cluster '{cluster_id}' to create table and load data...")
    conn = get_redshift_connection(cluster_id, db_name, user, password, region)
    cur = conn.cursor()

    try:
        cur.execute(f'DROP TABLE IF EXISTS {table_name}')
        cur.execute(create_sql)
        print(f"[Redshift] Created table: {table_name}")

        copy_sql = f"""
            COPY {table_name}
            FROM 's3://{bucket}/{filename}'
            IAM_ROLE '{role_arn}'
            FORMAT AS CSV
            ACCEPTINVCHARS
            EMPTYASNULL
            BLANKSASNULL
            IGNOREHEADER 1
            MAXERROR 100;
        """
        cur.execute(copy_sql)
        conn.commit()
        print(f"[Redshift] Loaded data into {table_name} from S3.")
    except Exception as e:
        print(f"[Redshift] Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()