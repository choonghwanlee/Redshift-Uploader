import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from uploader.schema_generator import infer_schema_and_generate_sql
from pathlib import Path


def test_infer_schema_and_generate_sql(tmp_path):
    """
    Test that infer_schema_and_generate_sql correctly infers schema
    from a simple CSV file and generates a CREATE TABLE SQL string.
    
    Expected behavior:
    - Table name is derived from file name
    - SQL contains appropriate column types (INTEGER, FLOAT, VARCHAR)
    """
    sample_csv = tmp_path / "sample.csv"
    sample_csv.write_text("id,name,score\n1,Alice,88.5\n2,Bob,90.0")

    table_name, create_sql = infer_schema_and_generate_sql(sample_csv)

    assert "CREATE TABLE" in create_sql
    assert table_name == "sample"
    assert '"id" INTEGER' in create_sql
    assert '"score" FLOAT' in create_sql