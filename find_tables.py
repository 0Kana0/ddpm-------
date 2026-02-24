# find_tables.py
import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

conninfo = {
  "host": os.getenv("PGHOST"),
  "port": os.getenv("PGPORT"),
  "dbname": os.getenv("PGDATABASE"),
  "user": os.getenv("PGUSER"),
  "password": os.getenv("PGPASSWORD"),
  "sslmode": os.getenv("PGSSL", "require"),
}

with psycopg.connect(**conninfo) as conn:
  with conn.cursor() as cur:
    cur.execute("""
      SELECT n.nspname AS schema, c.relname AS table
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind = 'r'
        AND c.relname ILIKE '%shelter%'
      ORDER BY 1,2;
    """)
    rows = cur.fetchall()

print("FOUND:", len(rows))
for s, t in rows:
  print(f'{s}."{t}"')
