# Redshift Uploader CLI

This is a command-line tool that automates the ingestion of CSV files into **Amazon Redshift**. It performs end-to-end setup including:

- Creating an S3 bucket (if not exists)
- Creating an IAM role for Redshift to access S3
- Creating and configuring a Redshift cluster
- Uploading CSV files to S3
- Inferring schema from CSV and generating `CREATE TABLE` SQL
- Running `COPY` commands to load the data into Redshift

---

## 📦 Features

- ✅ Fully automated deployment pipeline
- ✅ Schema inference based on CSV contents
- ✅ CLI-driven with rich flags and user input
- ✅ Built-in test suite with coverage reporting
- ✅ Modular utilities for AWS Redshift, S3, IAM

---

## 🛠️ Installation

Clone the repository:

```bash
git clone https://github.com/yourname/redshift-uploader.git
cd redshift-uploader
```

Install dependencies:

```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

## 📖 User Manual

Run the CLI tool:

```bash
python uploader/cli.py \
  --directory ./data \
  --bucket my-s3-bucket-name \
  --cluster-id my-redshift-cluster \
  --db-name mydatabase \
  --user redshiftadmin \
  --password MySecurePassword123 \
  --region us-east-1
```

### CLI Options

| Flag           | Description                                                   |
| -------------- | ------------------------------------------------------------- |
| `--directory`  | Path to local directory containing CSV files                  |
| `--bucket`     | S3 bucket name (will be created if not exists)                |
| `--cluster-id` | Redshift cluster identifier                                   |
| `--db-name`    | Redshift database name                                        |
| `--user`       | Master Redshift username                                      |
| `--password`   | Master Redshift password                                      |
| `--role-name`  | IAM role name to be created (default: `RedshiftS3AccessRole`) |
| `--region`     | AWS region (default: `us-east-1`)                             |

## 📊 Test Coverage Report

To run unit tests, you can run `pytest` from the command line.

We pass all 12 tests with 85% coverage over the codebase.

You can also view the results in HTML by running the following:

```bash
pytest --cov=. --cov-report=term --cov-report=html
open htmlcov/index.html
```

## Demo Video

[You can find the demo video for the app here](https://youtu.be/bJj260Sl6BI)
