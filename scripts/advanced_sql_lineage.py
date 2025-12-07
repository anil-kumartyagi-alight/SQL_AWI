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

def extract_select_fields(select_clause):
    fields = []
    # Split by comma, keep AS aliases, remove trailing/leading whitespace
    for col in re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", select_clause, flags=re.DOTALL):
        col = col.strip()
        # Get the field/alias name
        match = re.match(r".*?\s+AS\s+([^\s,]+)", col, re.IGNORECASE)
        if match:
            fields.append(match.group(1))
        else:
            # Try bare field name (may have function, table.field)
            simple = col.split(".")[-1].split()[-1].replace(",", "")
            if simple:
                fields.append(simple)
    return fields

def extract_tables(sql_block):
    # FROM/JOIN target extraction (handles schema)
    table_regex = re.compile(r"(?:FROM|JOIN)\s+([a-zA-Z0-9_\.]+)", re.IGNORECASE)
    tables = []
    for match in table_regex.finditer(sql_block):
        tbl = match.group(1)
        if "." in tbl:
            schema, table = tbl.split(".", 1)
        else:
            schema, table = "", tbl
        tables.append((schema, table))
    return tables

def extract_ctes(sql_text):
    # Extract CTE blocks from WITH ... AS ( ... )
    cte_blocks = []
    with_start = sql_text.find("WITH ")
    if with_start == -1:
        return []
    sql_after_with = sql_text[with_start+5:]
    # Match cte_name AS ( ... ) pairs
    pattern = re.compile(r"(\w+)\s+AS\s*\((.*?)\)(,|WITH|SELECT|INSERT|UPDATE|DELETE|$)", re.DOTALL | re.IGNORECASE)
    for m in pattern.finditer(sql_after_with):
        name, cte_content, _ = m.groups()
        cte_blocks.append((name.strip(), cte_content.strip()))
    return cte_blocks

def extract_main_select(sql_text):
    # Find outermost SELECT ... FROM ...
    select_match = re.search(r"SELECT\s+(.*?)\s+FROM\s+([a-zA-Z0-9_\.]+)", sql_text, re.DOTALL | re.IGNORECASE)
    if select_match:
        fields = extract_select_fields(select_match.group(1))
        tbl = select_match.group(2)
        schema, table = ("", tbl)
        if "." in tbl:
            schema, table = tbl.split(".", 1)
        return [{"schema": schema, "table": table, "field": field} for field in fields]
    return []

def process_sql_text(sql_text, filename):
    results = []

    # 1. CTEs
    cte_blocks = extract_ctes(sql_text)
    for cte_name, cte_sql in cte_blocks:
        select_rows = extract_main_select(cte_sql)
        for r in select_rows:
            r['file'] = filename
            results.append(r)
        # Also extract all table references from JOIN/FROM
        for schema, table in extract_tables(cte_sql):
            results.append({'file': filename, 'schema': schema, 'table': table, 'field': f'({cte_name})'})

    # 2. Outer SELECT
    outer_select_rows = extract_main_select(sql_text)
    for r in outer_select_rows:
        r['file'] = filename
        results.append(r)
    # All tables from FROM/JOIN at outer level
    for schema, table in extract_tables(sql_text):
        results.append({'file': filename, 'schema': schema, 'table': table, 'field': ''})

    # 3. Extra: try to find all SELECT ... FROM ... deep inside (for subqueries)
    for sub_select in re.finditer(r"SELECT\s+(.*?)\s+FROM\s+([a-zA-Z0-9_\.]+)", sql_text, re.DOTALL | re.IGNORECASE):
        select_clause = sub_select.group(1)
        table_full = sub_select.group(2)
        fields = extract_select_fields(select_clause)
        schema, table = ("", table_full)
        if "." in table_full:
            schema, table = table_full.split(".", 1)
        for f in fields:
            results.append({'file': filename, 'schema': schema, 'table': table, 'field': f})

    return results

def main():
    repo_root = os.path.abspath(os.path.dirname(__file__) + '/../')
    sql_files = find_sql_files(repo_root)
    print(f"SQL files found: {sql_files}")
    all_results = []
    for sql_file in sql_files:
        with open(sql_file, 'r', encoding='utf-8', errors='ignore') as f:
            sql_text = f.read()
            rows = process_sql_text(sql_text, os.path.relpath(sql_file, repo_root))
            print(f"{sql_file}: Found {len(rows)} lineage records.")
            all_results.extend(rows)
    # Deduplicate records
    seen = set()
    deduped = []
    for row in all_results:
        k = tuple(row.items())
        if k not in seen:
            seen.add(k)
            deduped.append(row)
    out_file = os.path.join(repo_root, 'sql_metadata.csv')
    with open(out_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['file','schema','table','field'])
        writer.writeheader()
        for row in deduped:
            writer.writerow(row)
    print(f"Wrote {len(deduped)} rows to {out_file}")

if __name__ == "__main__":
    main()
