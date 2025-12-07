import glob
from sqllineage.runner import LineageRunner
import csv

def lineage_csv(sql_files, out_csv="sql_table_lineage.csv"):
    with open(out_csv, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["sql_file", "source_tables", "target_tables"])
        for file in sql_files:
            with open(file, "r", encoding="utf-8") as f:
                sql = f.read()
            runner = LineageRunner(sql)
            sources = ",".join([str(s) for s in runner.source_tables])
            targets = ",".join([str(t) for t in runner.target_tables])
            writer.writerow([file, sources, targets])
    print(f"Table lineage written to {out_csv}")

if __name__ == "__main__":
    sql_files = glob.glob("*.sql")
    lineage_csv(sql_files)
