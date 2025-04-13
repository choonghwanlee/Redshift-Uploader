import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from click.testing import CliRunner
from uploader.cli import main
from unittest.mock import patch, MagicMock



def test_cli_help():
    """
    Test that the CLI shows the help message when called with --help.
    
    Expected behavior:
    - Exit code is 0
    - Output contains the word "Usage:"
    """
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])

    assert result.exit_code == 0
    assert "Usage:" in result.output


@patch("uploader.cli.create_table_and_copy")
@patch("uploader.cli.infer_schema_and_generate_sql")
@patch("uploader.cli.upload_to_s3")
@patch("uploader.cli.create_redshift_cluster")
@patch("uploader.cli.create_iam_role")
@patch("uploader.cli.create_s3_bucket")
def test_full_cli_flow(mock_bucket, mock_role, mock_cluster, mock_upload, mock_schema, mock_copy, tmp_path):
    """
    Simulates CLI run end-to-end with mocks and a real temp CSV directory.
    """
    # Setup mock return values
    mock_role.return_value = "arn:aws:iam::123456789012:role/MockRole"
    mock_schema.return_value = ("mock_table", "CREATE TABLE mock_table (id INT);")

    # Create fake CSV file
    (tmp_path / "sample.csv").write_text("id,name\n1,Alice")

    runner = CliRunner()
    result = runner.invoke(main, [
        "--directory", str(tmp_path),
        "--bucket", "test-bucket",
        "--cluster-id", "test-cluster",
        "--db-name", "testdb",
        "--user", "admin",
        "--password", "pw"
    ])

    assert result.exit_code == 0
    assert mock_bucket.called
    assert mock_role.called
    assert mock_cluster.called
    assert mock_upload.called
    assert mock_copy.called
