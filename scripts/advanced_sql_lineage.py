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

def extract_table_field_names(sql_text):
    results = []

    # Extract CTEs
    cte_pattern = re.compile(r"WITH\s+(.*?)\s+SELECT", re.IGNORECASE | re.DOTALL)
    ctes = []
    for match in re.finditer(r"WITH\s+(.*?)\)\s*,\s*", sql_text, re.IGNORECASE | re.DOTALL):
        ctes_raw = match.group(1)
        for cte_match in re.finditer(r"(\w+)\s+AS\s*\((.*?)\)", ctes_raw, re.IGNORECASE | re.DOTALL):
            cte_name, cte_sql = cte_match.groups()
            ctes.append((cte_name, cte_sql))
    # Add main query as another "cte"
    main_pattern = re.compile(r"SELECT\s+(.*?)\s+FROM\s+([^\s;]+)", re.IGNORECASE | re.DOTALL)
    main_match = main_pattern.search(sql_text)
    if main_match:
        select_fields, main_table = main_match.groups()
        fields = [f.strip().split()[-1] for f in select_fields.split(',')]
        schema, table = ("", main_table)
        if "." in main_table:
            schema, table = main_table.split(".", 1)
        for field in fields:
            results.append({"schema": schema, "table": table, "field": field})
    # Extract from each CTE
    for cte_name, cte_sql in ctes:
        select_match = main_pattern.search(cte_sql)
        if select_match:
            select_fields, from_table = select_match.groups()
            fields = [f.strip().split()[-1] for f in select_fields.split(',')]
            schema, table = ("", from_table)
            if "." in from_table:
                schema, table = from_table.split(".", 1)
            for field in fields:
                results.append({"schema": schema, "table": table, "field": field})
    # Table references from JOINs
    for join_match in re.finditer(r"(?:FROM|JOIN)\s+([^\s\(\)]+)", sql_text):
        target = join_match.group(1)
        schema, table = ("", target)
        if "." in target:
            schema, table = target.split(".", 1)
        results.append({"schema": schema, "table": table, "field": ""})
    return results

def main():
    repo_root = os.path.abspath(os.path.dirname(__file__) + '/../')
    sql_files = find_sql_files(repo_root)
    print(f"SQL files found: {sql_files}")
    all_results = []
    for sql_file in sql_files:
        with open(sql_file, 'r', encoding='utf-8', errors='ignore') as f:
            sql_text = f.read()
            metadata = extract_table_field_names(sql_text)
            print(f"{sql_file}: Found {len(metadata)} lineage rows.")
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
