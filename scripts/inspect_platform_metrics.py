import os
import glob
import sqlite3
import pandas as pd
import pyarrow.parquet as pq

print("=== LAYER ROW COUNTS PER PAIR ===")
for layer in ['bronze', 'silver', 'gold']:
    path = f"data/{layer}"
    if os.path.exists(path):
        try:
            files = glob.glob(f"{path}/**/*.parquet", recursive=True)
            if files:
                dfs = []
                for f in files:
                    # extract pair from directory if partitioned
                    parts = f.replace('\\', '/').split('/')
                    pair = None
                    for part in parts:
                        if part.startswith("Pair="):
                            pair = part.split("=")[1]
                    table = pq.read_table(f, columns=['Date'] if 'Date' in pq.read_schema(f).names else None)
                    cnt = table.num_rows
                    dfs.append({'Pair': pair, 'count': cnt})
                df = pd.DataFrame(dfs).groupby('Pair')['count'].sum().reset_index()
                print(f"-- Layer: {layer} (total: {df['count'].sum():,}) --")
                for _, r in df.iterrows():
                    print(f"   {r['Pair']}: {r['count']:,}")
            else:
                print(f"-- Layer: {layer}: 0 files found --")
        except Exception as e:
            print(f"Error reading {layer}: {e}")

quarantine_path = "data/quarantine"
if os.path.exists(quarantine_path):
    files = glob.glob(f"{quarantine_path}/**/*.parquet", recursive=True)
    if files:
        dfs = []
        for f in files:
            t = pq.read_table(f)
            dfs.append(t.to_pandas())
        df_q = pd.concat(dfs, ignore_index=True)
        print(f"-- Layer: quarantine (total: {len(df_q):,}) --")
        grouped = df_q.groupby(['Pair', 'quarantine_reason']).size().reset_index(name='count')
        for _, r in grouped.iterrows():
            print(f"   {r['Pair']} [{r['quarantine_reason']}]: {r['count']:,}")
    else:
        print("-- Layer: quarantine: 0 rows (no quarantined files) --")
else:
    print("-- Layer: quarantine: directory not created / 0 rows quarantined --")

print("\n=== DATABASE TABLE ROW COUNTS ===")
if os.path.exists("data/forex_platform.db"):
    conn = sqlite3.connect("data/forex_platform.db")
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]
    print("Database tables in data/forex_platform.db:")
    for t in sorted(tables):
        cursor.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = cursor.fetchone()[0]
        print(f"   {t}: {cnt:,} rows")
    
    if "curated_rates" in tables:
        cursor.execute("SELECT pair, COUNT(*) FROM curated_rates GROUP BY pair ORDER BY pair")
        print("   curated_rates breakdown by pair:")
        for p, c in cursor.fetchall():
            print(f"      {p}: {c:,}")
            
    if "quarantine" in tables:
        cursor.execute("SELECT COUNT(*) FROM quarantine")
        cnt = cursor.fetchone()[0]
        print(f"   quarantine table rows: {cnt:,}")
    conn.close()

print("\n=== POSTGRES CONTAINER ROW COUNTS ===")
os.system('docker exec forex-airflow-postgres psql -U airflow -d forex -c "\\dt"')
os.system('docker exec forex-airflow-postgres psql -U airflow -d forex -c "SELECT \'curated_rates\' AS tbl, count(*) FROM curated_rates UNION ALL SELECT \'quality_results\', count(*) FROM quality_results UNION ALL SELECT \'quarantine\', count(*) FROM quarantine;"')
