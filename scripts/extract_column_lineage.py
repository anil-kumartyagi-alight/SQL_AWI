import os
import glob
import csv
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

def extract_column_lineage(sql):
    """
    Returns a list of (column, source_table, source_schema) for the final SELECT statement in the SQL code.
    Only works for straightforward SQL; for highly complex queries, manual review may be needed.
    """
    lineage = []
    parsed = sqlparse.parse(sql)
    for stmt in parsed:
        # Look for the main SELECT statement
        if stmt.get_type() == 'SELECT':
            # Find the FROM source(s)
            from_seen = False
            source_table = None
            source_schema = None
            columns = []
            for token in stmt.tokens:
                if from_seen:
                    # Parse table name after FROM
                    if isinstance(token, Identifier):
                        full_name = token.get_real_name() or str(token)
                        source_table = token.get_real_name()
                        source_schema = token.get_parent_name()
                    elif isinstance(token, IdentifierList):
                        # Handle multiple tables
                        for t in token.get_identifiers():
                            source_table = t.get_real_name()
                            source_schema = t.get_parent_name()
                            # For each, add with column mapping below
                    break
                if token.ttype is Keyword and token.value.upper() == 'FROM':
                    from_seen = True

            # Parse the columns in the SELECT list
            # Looks for IdentifierList at SELECT
            for token in stmt.tokens:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        col = identifier.get_real_name() or str(identifier)
                        lineage.append((col, source_table, source_schema))
                    break
                elif isinstance(token, Identifier):
                    col = token.get_real_name() or str(token)
                    lineage.append((col, source_table, source_schema))
                    break

    return lineage

def process_sql_files(pattern="*.sql"):
    files = glob.glob(pattern)
    all_lineage = []
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            sql = f.read()
        file_lineage = extract_column_lineage(sql)
        for col, table, schema in file_lineage:
            all_lineage.append({
                "sql_file": file,
                "column": col,
                "source_table": table,
                "source_schema": schema
            })
    return all_lineage

def save_as_csv(lineage, out_csv="sql_column_lineage.csv"):
    with open(out_csv, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["sql_file", "column", "source_table", "source_schema"])
        writer.writeheader()
        for row in lineage:
            writer.writerow(row)
    print(f"Lineage written to {out_csv}")

if __name__ == "__main__":
    lineage = process_sql_files("*.sql")
    save_as_csv(lineage)
