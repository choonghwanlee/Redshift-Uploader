import click
from pathlib import Path
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))


from uploader.iam_utils import create_iam_role
from uploader.redshift_utils import create_redshift_cluster, create_table_and_copy
from uploader.schema_generator import infer_schema_and_generate_sql
from uploader.s3_utils import create_s3_bucket, upload_to_s3


@click.command()
@click.option('--directory', required=True, type=click.Path(exists=True), help='Directory containing CSV files')
@click.option('--bucket', required=True, help='S3 bucket name to create/use')
@click.option('--cluster-id', required=True, help='Redshift cluster identifier')
@click.option('--db-name', required=True, help='Redshift database name')
@click.option('--user', required=True, help='Redshift master username')
@click.option('--password', required=True, help='Redshift master password')
@click.option('--role-name', default='RedshiftS3AccessRole', help='IAM Role name for Redshift to access S3')
@click.option('--region', default='us-east-1', help='AWS region (default: us-east-1)')
def main(directory, bucket, cluster_id, db_name, user, password, role_name, region):
    print("=== Step 1: Create or Verify S3 Bucket ===")
    create_s3_bucket(bucket, region)

    print("=== Step 2: Create or Reuse IAM Role ===")
    role_arn = create_iam_role(role_name)

    print("=== Step 3: Create Redshift Cluster ===")
    create_redshift_cluster(
        cluster_id=cluster_id,
        db_name=db_name,
        user=user,
        password=password,
        role_arn=role_arn,
        region=region
    )

    print("=== Step 4: Upload CSV Files to S3 ===")
    upload_to_s3(directory, bucket, region)

    print("=== Step 5: Create Tables and COPY Data ===")
    for csv_file in Path(directory).glob("*.csv"):
        print(f"-> Processing file: {csv_file.name}")
        table_name, create_sql = infer_schema_and_generate_sql(csv_file)

        create_table_and_copy(
            table_name=table_name,
            create_sql=create_sql,
            bucket=bucket,
            filename=csv_file.name,
            cluster_id=cluster_id,
            db_name=db_name,
            user=user,
            password=password,
            region=region,
            role_arn=role_arn
        )

    print("✅ All CSVs processed and loaded into Redshift.")


if __name__ == '__main__':
    main()
