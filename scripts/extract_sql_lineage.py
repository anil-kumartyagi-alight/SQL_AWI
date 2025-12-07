import os
import glob
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

def generate_graph(file_base, tables, joins):
    G = nx.DiGraph()
    G.add_nodes_from(tables)
    G.add_edges_from(joins)
    plt.figure(figsize=(8, 5))
    pos = nx.spring_layout(G, k=0.6, seed=42)
    nx.draw(G, pos, with_labels=True, node_color='skyblue',
            node_size=1700, edge_color='gray', font_size=10, arrows=True)
    plt.title(f"SQL Table Lineage for {file_base}")
    plt.axis('off')
    image_path = f"{file_base}_lineage.png"
    plt.savefig(image_path, format='png')
    plt.close()
    return image_path

def generate_html_report(file, tables, joins, image_path):
    html_path = f"{os.path.splitext(file)[0]}_lineage.html"
    html = f"""
    <html>
    <head><title>SQL Lineage Report - {file}</title></head>
    <body>
      <h1>SQL Lineage for {file}</h1>
      <img src="{os.path.basename(image_path)}" alt="SQL Lineage Graph" width="700"/><br/>
      <h2>Tables</h2>
      <ul>{''.join(f'<li>{tbl}</li>' for tbl in tables)}</ul>
      <h2>Joins</h2>
      <ul>{''.join(f'<li>{a} → {b}</li>' for a, b in joins)}</ul>
    </body>
    </html>
    """
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    return html_path

if __name__ == "__main__":
    files = glob.glob("*.sql")
    if not files:
        print("No .sql files found.")
        exit(1)
    all_reports = []
    for file in files:
        print(f"Processing {file} ...")
        with open(file, 'r', encoding='utf-8') as f:
            sql = f.read()
        tables, joins = extract_tables_and_joins(sql)
        base = os.path.splitext(file)[0]
        img = generate_graph(base, tables, joins)
        html = generate_html_report(file, tables, joins, img)
        all_reports.append(html)
    print(f"Generated {len(all_reports)} lineage reports.")
