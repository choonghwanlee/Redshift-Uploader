import pandas as pd
from pathlib import Path

def infer_schema_and_generate_sql(csv_path):
    df = pd.read_csv(csv_path, nrows=100)  # Sample for speed
    table_name = csv_path.stem
    cols = []
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "FLOAT"
        else:
            sql_type = "VARCHAR(256)"
        cols.append(f'"{col}" {sql_type}')
    create_sql = f'CREATE TABLE {table_name} (\n  ' + ",\n  ".join(cols) + '\n);'
    return table_name, create_sql


if __name__ == "__main__":
    csv_path = Path('./data/olist_order_reviews_dataset.csv')
    table_name, create_sql = infer_schema_and_generate_sql(csv_path)
    print(f"Table name: {table_name}")
    print(f"Create SQL:\n{create_sql}")