import os
import glob
import re
import sqlparse
import networkx as nx
import matplotlib.pyplot as plt

def extract_tables_and_joins(sql):
    """
    Extracts table names (including schema.qualified) used in FROM and JOIN clauses in SQL,
    including CTEs and subqueries, using regex heuristics.
    Returns:
        tables: set of table names
        joins: list of (left_table, right_table)
    """
    # Pattern captures tables after FROM or JOIN (optionally schema.qualified), until a whitespace or bracket/comma/semicolon
    from_join_pattern = re.compile(r'\b(?:FROM|JOIN)\s+([a-zA-Z0-9_\.]+)', re.IGNORECASE)
    tables = set()
    joins = []
    last_from = None
    for match in from_join_pattern.finditer(sql):
        table = match.group(1)
        tables.add(table)
        # Determine if this is a FROM or JOIN by looking at the pattern
        keyword = sql[max(0, match.start()-5):match.start()].strip().upper()
        if keyword.endswith('FROM'):
            last_from = table
        elif keyword.endswith('JOIN') and last_from is not None:
            joins.append((last_from, table))
    return tables, joins

def generate_graph(file_base, tables, joins):
    G = nx.DiGraph()
    G.add_nodes_from(tables)
    G.add_edges_from(joins)
    plt.figure(figsize=(max(8, len(tables)), 5))
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
      <ul>{''.join(f'<li>{tbl}</li>' for tbl in sorted(tables))}</ul>
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
        print(f"Tables found in {file}: {tables}")
        print(f"Joins found in {file}: {joins}")
        base = os.path.splitext(file)[0]
        img = generate_graph(base, tables, joins)
        html = generate_html_report(file, tables, joins, img)
        all_reports.append(html)
    print(f"Generated {len(all_reports)} lineage reports.")
