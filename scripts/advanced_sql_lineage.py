import os
import re
import csv

def find_sql_files(directory):
    """
    Recursively find all .sql files in the given directory.
    Returns a list of file paths.
    """
    sql_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.sql'):
                sql_files.append(os.path.join(root, file))
    return sql_files

# Placeholder for missing SQL helpers referenced in main script
def extract_ctes(sql_text):
    """Extract CTE name and SQL as a list of (name, sql) tuples."""
    pattern = re.compile(r"WITH\s+([a-zA-Z0-9_]+)\s+AS\s*\((.*?)\)\s*(,|WITH|SELECT|$)", re.DOTALL|re.IGNORECASE)
    ctes = []
    for match in pattern.finditer(sql_text):
        ctes.append((match.group(1), match.group(2)))
    return ctes

def extract_main_select(sql_text):
    """Extract SELECT fields, as [{'schema':..., 'table':..., 'field':...}]"""
    # This is simplified and might need to be adapted for complex SQL.
    results = []
    select_match = re.search(r"SELECT\s+(.*?)\s+FROM\s+([a-zA-Z0-9_\.]+)", sql_text, re.DOTALL | re.IGNORECASE)
    if select_match:
        select_clause = select_match.group(1)
        table_full = select_match.group(2)
        fields = extract_select_fields(select_clause)
        schema, table = ("", table_full)
        if "." in table_full:
            schema, table = table_full.split(".", 1)
        for f in fields:
            results.append({'schema': schema, 'table': table, 'field': f})
    return results

def extract_select_fields(select_clause):
    """Splits the select clause into fields (simplified, does not handle all cases)."""
    fields = []
    for f in select_clause.split(","):
        f = f.strip()
        # Remove aliases if present
        field = f.split(" as ")[0].split(" AS ")[0].strip()
        fields.append(field)
    return fields

def extract_tables(sql_text):
    """
    Extract (schema, table) pairs from all FROM/JOIN clauses.
    Returns a set of (schema, table)
    """
    tables = set()
    for m in re.finditer(r'(FROM|JOIN)\s+([a-zA-Z0-9_\.]+)', sql_text, re.IGNORECASE):
        table_full = m.group(2)
        schema, table = ("", table_full)
        if "." in table_full:
            schema, table = table_full.split(".", 1)
        tables.add((schema, table))
    return tables

def extract_where_filters(sql_text):
    """
    Extract field and filter expression from WHERE clauses.
    Returns: list of dicts: {table, field, filter_condition, filter_type}
    """
    filters = []
    # Basic: Find WHERE ... (until GROUP|ORDER|HAVING|UNION|END)
    for where in re.finditer(r'WHERE\s+(.*?)(GROUP\s+BY|ORDER\s+BY|HAVING|UNION|$)', sql_text, re.IGNORECASE|re.DOTALL):
        cond_block = where.group(1)
        # Split by AND/OR, crude but common patterns
        conditions = re.split(r'\bAND\b|\bOR\b', cond_block, flags=re.IGNORECASE)
        for cond in conditions:
            cond = cond.strip()
            # Try to extract (table.)field
            match = re.match(r'([a-zA-Z0-9_\.]+)\s*(=|<>|!=|>|<|>=|<=|IN|LIKE|IS)\s*.+', cond)
            if match:
                fld = match.group(1)
                schema, table, field = '', '', fld
                if '.' in fld:
                    parts = fld.split('.')
                    if len(parts) == 2:
                        table, field = parts
                    elif len(parts) == 3:
                        schema, table, field = parts
                filters.append({
                    'filter_type': 'WHERE',
                    'schema': schema,
                    'table': table,
                    'field': field,
                    'filter_condition': cond,
                    'join_table': ''
                })
    return filters

def extract_join_filters(sql_text):
    """
    Extract field, filter expression, join table from JOIN ... ON ... clauses.
    Returns: list of dicts: {table, field, filter_condition, filter_type, join_table}
    """
    filters = []
    # Find all JOIN ... ON ... (stop at next JOIN/WHERE)
    join_pattern = re.compile(
        r'(JOIN)\s+([a-zA-Z0-9_\.]+).*?ON\s+([^\n;]*?)(?:\s+JOIN|\s+WHERE|GROUP\s+BY|ORDER\s+BY|$)',
        re.IGNORECASE|re.DOTALL
    )
    for m in join_pattern.finditer(sql_text):
        join_table_full = m.group(2)
        on_expr = m.group(3)
        join_schema, join_table = '', join_table_full
        if "." in join_table_full:
            parts = join_table_full.split('.')
            if len(parts) == 2:
                join_schema, join_table = parts
        # Each condition (split by AND/OR)
        for cond in re.split(r'\bAND\b|\bOR\b', on_expr, flags=re.IGNORECASE):
            cond = cond.strip()
            # Extract both sides of equality
            eq_match = re.match(r'([a-zA-Z0-9_\.]+)\s*=\s*([a-zA-Z0-9_\.]+)', cond)
            if eq_match:
                for fld in [eq_match.group(1), eq_match.group(2)]:
                    schema, table, field = '', '', fld
                    if '.' in fld:
                        parts = fld.split('.')
                        if len(parts) == 2:
                            table, field = parts
                        elif len(parts) == 3:
                            schema, table, field = parts
                    filters.append({
                        'filter_type': 'JOIN_ON',
                        'schema': schema,
                        'table': table,
                        'field': field,
                        'filter_condition': cond,
                        'join_table': join_table
                    })
            else:
                # Handle more complex ON conditions
                fld_match = re.match(r'([a-zA-Z0-9_\.]+)', cond)
                if fld_match:
                    fld = fld_match.group(1)
                    schema, table, field = '', '', fld
                    if '.' in fld:
                        parts = fld.split('.')
                        if len(parts) == 2:
                            table, field = parts
                        elif len(parts) == 3:
                            schema, table, field = parts
                    filters.append({
                        'filter_type': 'JOIN_ON',
                        'schema': schema,
                        'table': table,
                        'field': field,
                        'filter_condition': cond,
                        'join_table': join_table
                    })
    return filters

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

    # 3. Extra: find all SELECT ... FROM ... deep inside (for subqueries)
    for sub_select in re.finditer(r"SELECT\s+(.*?)\s+FROM\s+([a-zA-Z0-9_\.]+)", sql_text, re.DOTALL | re.IGNORECASE):
        select_clause = sub_select.group(1)
        table_full = sub_select.group(2)
        fields = extract_select_fields(select_clause)
        schema, table = ("", table_full)
        if "." in table_full:
            schema, table = table_full.split(".", 1)
        for f in fields:
            results.append({'file': filename, 'schema': schema, 'table': table, 'field': f})

    # 4. Extract predefined filters in WHERE/ON
    filter_results = []
    for f in extract_where_filters(sql_text):
        f['file'] = filename
        filter_results.append(f)
    for f in extract_join_filters(sql_text):
        f['file'] = filename
        filter_results.append(f)

    return results, filter_results

def main():
    repo_root = os.path.abspath(os.path.dirname(__file__) + '/../')
    sql_files = find_sql_files(repo_root)
    print(f"SQL files found: {sql_files}")
    all_results = []
    all_filters = []
    for sql_file in sql_files:
        with open(sql_file, 'r', encoding='utf-8', errors='ignore') as f:
            sql_text = f.read()
            rows, filters = process_sql_text(sql_text, os.path.relpath(sql_file, repo_root))
            print(f"{sql_file}: Found {len(rows)} lineage records, {len(filters)} filter records.")
            all_results.extend(rows)
            all_filters.extend(filters)
    # Deduplicate lineage records
    seen = set()
    deduped = []
    for row in all_results:
        k = tuple(row.items())
        if k not in seen:
            seen.add(k)
            deduped.append(row)
    # Deduplicate filter records
    seen_filters = set()
    deduped_filters = []
    for row in all_filters:
        k = tuple(sorted(row.items()))
        if k not in seen_filters:
            seen_filters.add(k)
            deduped_filters.append(row)
    out_file = os.path.join(repo_root, 'sql_metadata.csv')
    with open(out_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['file','schema','table','field'])
        writer.writeheader()
        for row in deduped:
            writer.writerow(row)
    filter_file = os.path.join(repo_root, 'sql_filters.csv')
    with open(filter_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['file','filter_type','schema','table','field','filter_condition','join_table'])
        writer.writeheader()
        for row in deduped_filters:
            writer.writerow(row)
    print(f"Wrote {len(deduped)} rows to {out_file}")
    print(f"Wrote {len(deduped_filters)} filter rows to {filter_file}")

if __name__ == "__main__":
    main()
