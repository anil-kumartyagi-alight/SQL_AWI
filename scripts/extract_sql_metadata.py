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

def extract_metadata(sql_text, verbose=False):
    results = []
    # Regex for CREATE TABLE (schema optional), block should end with );
    create_table_pattern = re.compile(
        r"CREATE\s+TABLE\s+(?:(\w+)\.)?(\w+)\s*\((.*?)\);",
        re.IGNORECASE | re.DOTALL
    )
    matches = list(create_table_pattern.finditer(sql_text))
    if verbose:
        print(f"Matches found: {len(matches)}")
    for match in matches:
        schema, table, fields_block = match.groups()
        if verbose:
            print(f"Schema: {schema}, Table: {table}")
        fields = []
        # Split fields, ignore lines with constraints
        for line in fields_block.split(','):
            field = line.strip().split()[0]
            if field.upper() not in ('PRIMARY', 'CONSTRAINT', 'UNIQUE', 'KEY', ''):
                fields.append(field)
        for field_name in fields:
            results.append({
                'schema': schema or '',
                'table': table,
                'field': field_name
            })
    # Fallback: search for INSERT and SELECT statements
    fallback_pattern = re.compile(r"(?:INSERT INTO|SELECT.+FROM)\s+(?:(\w+)\.)?(\w+)", re.IGNORECASE)
    for m in fallback_pattern.finditer(sql_text):
        schema, table = m.groups()
        results.append({'schema': schema or '', 'table': table, 'field': ''})
        if verbose:
            print(f"Fallback match: Schema={schema}, Table={table}")
    return results

def main():
    repo_root = os.path.abspath(os.path.dirname(__file__) + '/../')
    sql_files = find_sql_files(repo_root)
    print(f"SQL files found: {len(sql_files)}")
    for f in sql_files:
        print(f"  {f}")
    all_results = []
    for sql_file in sql_files:
        try:
            with open(sql_file, 'r', encoding='utf-8', errors='ignore') as f:
                sql_text = f.read()
        except Exception as e:
            print(f"Could not read {sql_file}: {e}")
            continue
        metadata = extract_metadata(sql_text, verbose=True)
        print(f"{sql_file}: Found {len(metadata)} schema/table/fields")
        for row in metadata:
            row['file'] = os.path.relpath(sql_file, repo_root)
            all_results.append(row)
    out_file = os.path.join(repo_root, 'sql_metadata.csv')
    with open(out_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['file','schema','table','field'])
        writer.writeheader()
        for row in all_results:
            writer.writerow(row)
    print(f"Wrote {len(all_results)} rows to {out_file}")

if __name__ == "__main__":
    main()
