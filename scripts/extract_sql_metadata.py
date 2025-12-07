import os
import re
import csv

def find_sql_files(root_dir):
    sql_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for file in filenames:
            if file.endswith('.sql'):
                sql_files.append(os.path.join(dirpath, file))
    return sql_files

def extract_metadata(sql_text):
    results = []
    # Regex patterns for CREATE TABLE, optionally schema prefixed
    create_table_pattern = re.compile(
        r"CREATE\s+TABLE\s+(?:(\w+)\.)?(\w+)\s*\((.*?)\);",
        re.IGNORECASE | re.DOTALL
    )
    # Extract CREATE TABLE blocks
    for match in create_table_pattern.finditer(sql_text):
        schema, table, fields_block = match.groups()
        fields = []
        # Field lines: each line in field block before possible constraints
        for line in fields_block.split(','):
            field = line.strip().split()[0]
            # Ignore constraints (PRIMARY, CONSTRAINT, etc.)
            if field.upper() not in ('PRIMARY', 'CONSTRAINT', 'UNIQUE', 'KEY'):
                fields.append(field)
        for field_name in fields:
            results.append({
                'schema': schema or '',
                'table': table,
                'field': field_name
            })
    return results

def main():
    repo_root = os.path.abspath(os.path.dirname(__file__) + '/../')
    sql_files = find_sql_files(repo_root)
    all_results = []
    for sql_file in sql_files:
        with open(sql_file, 'r', encoding='utf-8', errors='ignore') as f:
            sql_text = f.read()
            metadata = extract_metadata(sql_text)
            for row in metadata:
                row['file'] = os.path.relpath(sql_file, repo_root)
                all_results.append(row)
    # Output CSV
    out_file = os.path.join(repo_root, 'sql_metadata.csv')
    with open(out_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['file','schema','table','field'])
        writer.writeheader()
        for row in all_results:
            writer.writerow(row)
    print(f"Extracted metadata from {len(sql_files)} files; output saved to {out_file}")

if __name__ == "__main__":
    main()
