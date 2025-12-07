import os
import glob
import sys
import sqlparse
from sqlparse.sql import Identifier
from sqlparse.tokens import Keyword
import networkx as nx
import matplotlib.pyplot as plt

def extract_tables_and_joins(sql):
    parsed = sqlparse.parse(sql)
    tables, joins = set(), []
    last_from = None
    for stmt in parsed:
        tokens = list(stmt.flatten())
        for i, token in enumerate(tokens):
            if token.ttype is Keyword and token.value.upper() in ('FROM', 'JOIN'):
                # Look for next identifier as the table name
                j = i + 1
                while j < len(tokens) and not isinstance(tokens[j], Identifier):
                    j += 1
                if j < len(tokens):
                    tb_name = str(tokens[j])
                    tables.add(tb_name)
                    if token.value.upper() == 'JOIN' and last_from:
                        joins.append((last_from, tb_name))
                if token.value.upper() == 'FROM' and j < len(tokens):
                    last_from = str(tokens[j])
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

def generate_graph(tables, joins, output_filename='sql_lineage_graph.png'):
    G = nx.DiGraph()
    G.add_nodes_from(tables)
    G.add_edges_from(joins)
    plt.figure(figsize=(10, 7))
    pos = nx.spring_layout(G, k=0.6, seed=42)
    nx.draw(G, pos, with_labels=True, node_color='skyblue', 
            node_size=1700, edge_color='gray', font_size=10, arrows=True)
    plt.title("SQL Table Lineage")
    plt.axis('off')
    plt.savefig(output_filename, format='png')
    plt.close()
    return output_filename

def generate_html(image_path, details):
    html = f"""
    <html>
    <head><title>SQL Lineage Report</title></head>
    <body>
      <h1>SQL Lineage Summary</h1>
      <img src="{image_path}" alt="SQL Lineage Graph" width="800"/>
      <h2>Details</h2>
      <ul>
    """
    for file, tables, joins in details:
        html += f"<li><b>{file}</b>: Tables - {', '.join(tables)}, Joins - {', '.join(f'{a}→{b}' for a,b in joins)}</li>"
    html += "</ul></body></html>"
    return html

if __name__ == "__main__":
    pattern = sys.argv[1] if len(sys.argv) > 1 else 'sql/*.sql'
    files = glob.glob(pattern, recursive=True)
    if not files:
        print(f"No SQL files found for pattern: {pattern}")
        sys.exit(1)
    tables, joins, details = build_lineage(files)
    img_path = "sql_lineage_graph.png"
    generate_graph(tables, joins, img_path)
    html = generate_html(img_path, details)
    with open("sql_lineage_report.html", "w") as f:
        f.write(html)
    print("SQL lineage report generated: sql_lineage_report.html")
