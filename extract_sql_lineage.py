import os
import glob
import sys
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token
from sqlparse.tokens import Keyword, DML
from graphviz import Digraph

def extract_tables_and_joins(sql):
    parsed = sqlparse.parse(sql)
    tables, joins = set(), []
    for stmt in parsed:
        for token in stmt.tokens:
            if token.ttype is Keyword and token.value.upper() in ('FROM', 'JOIN'):
                idx = stmt.token_index(token)
                next_ = stmt.token_next(idx, skip_ws=True)
                if isinstance(next_, Identifier):
                    tb_name = str(next_)
                    tables.add(tb_name)
                    if token.value.upper() == 'JOIN':
                        joins.append(tb_name)
    return tables, joins

def build_lineage(files):
    all_tables, all_joins = set(), []
    details = []
    for file in files:
        with open(file, 'r') as f:
            sql = f.read()
        tables, joins = extract_tables_and_joins(sql)
        all_tables |= tables
        all_joins.extend(joins)
        details.append((file, tables, joins))
    return all_tables, all_joins, details

def generate_graph(tables, joins, details):
    dot = Digraph(comment='SQL Lineage')
    for tb in tables:
        dot.node(tb, tb)
    for j in joins:
        dot.edge('main', j)
    return dot

def generate_html(dot, details):
    graph_svg = dot.pipe(format='svg').decode('utf-8')
    html = f"""
    <html>
    <head><title>SQL Lineage Report</title></head>
    <body>
      <h1>SQL Lineage Summary</h1>
      {graph_svg}
      <h2>Details</h2>
      <ul>
    """
    for file, tables, joins in details:
        html += f"<li><b>{file}</b>: Tables - {', '.join(tables)}, Joins - {', '.join(joins)}</li>"
    html += "</ul></body></html>"
    return html

if __name__ == "__main__":
    pattern = sys.argv[1] if len(sys.argv) > 1 else 'sql/*.sql'
    files = glob.glob(pattern, recursive=True)
    if not files:
        print(f"No SQL files found for pattern: {pattern}")
        sys.exit(1)
    tables, joins, details = build_lineage(files)
    dot = generate_graph(tables, joins, details)
    html = generate_html(dot, details)
    with open("sql_lineage_report.html", "w") as f:
        f.write(html)
    print("SQL lineage report generated: sql_lineage_report.html")
