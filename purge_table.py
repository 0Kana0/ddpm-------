# purge_table.py
import os
import argparse
import psycopg
from psycopg import sql
from dotenv import load_dotenv

load_dotenv()

def get_conninfo():
    # อ่านค่าจาก env: PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD PGSSL(optional)
    host = os.getenv("PGHOST", "")
    port = os.getenv("PGPORT", "")
    dbname = os.getenv("PGDATABASE", "")
    user = os.getenv("PGUSER", "")
    password = os.getenv("PGPASSWORD", "")
    sslmode = os.getenv("PGSSL", "")  # ใส่ค่าเช่น require / disable / verify-full

    conninfo = {}
    if host: conninfo["host"] = host
    if port: conninfo["port"] = port
    if dbname: conninfo["dbname"] = dbname
    if user: conninfo["user"] = user
    if password: conninfo["password"] = password
    if sslmode: conninfo["sslmode"] = sslmode

    print(conninfo)
    return conninfo

def list_dependent_tables(cur, target_regclass_text: str):
    # หา “ตารางลูก” ที่อ้างอิง target (recursive)
    q = """
    WITH RECURSIVE fk_tree AS (
      SELECT
        conrelid::regclass AS child,
        confrelid::regclass AS parent
      FROM pg_constraint
      WHERE contype='f' AND confrelid = %s::regclass

      UNION ALL

      SELECT
        c.conrelid::regclass AS child,
        c.confrelid::regclass AS parent
      FROM pg_constraint c
      JOIN fk_tree t ON c.confrelid = t.child
      WHERE c.contype='f'
    )
    SELECT DISTINCT child::text AS table_name
    FROM fk_tree
    ORDER BY table_name;
    """
    cur.execute(q, (target_regclass_text,))
    return [r[0] for r in cur.fetchall()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True,
                    help='ชื่อตาราง เช่น public.Shelter หรือ "Shelter" (ถ้ามีตัวพิมพ์ใหญ่ต้องใส่เครื่องหมายคำพูด)')
    ap.add_argument("--dry-run", action="store_true", help="แสดงรายการตารางที่จะโดน CASCADE แต่ไม่ลบจริง")
    args = ap.parse_args()

    conninfo = get_conninfo()
    if not conninfo.get("host") or not conninfo.get("dbname") or not conninfo.get("user"):
        print("❌ กรุณาตั้งค่า env ให้ครบ: PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD (และ PGSSL ถ้ามี)")
        return

    with psycopg.connect(**conninfo) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # ตรวจว่าตารางมีอยู่จริง
            cur.execute("SELECT to_regclass(%s)::text", (args.table,))
            reg = cur.fetchone()[0]
            if not reg:
                print(f"❌ ไม่พบตาราง: {args.table}")
                print('   ตัวอย่าง: --table public.Shelter หรือ --table \'"Shelter"\'')
                return

            deps = list_dependent_tables(cur, reg)
            print(f"Target: {reg}")
            if deps:
                print("Dependent tables (FK -> target, recursive):")
                for t in deps:
                    print(" -", t)
            else:
                print("Dependent tables: (none)")

            if args.dry_run:
                print("\n✅ DRY RUN: ไม่ได้ลบข้อมูลจริง")
                return

            # ลบจริง: TRUNCATE + CASCADE + reset identity
            stmt = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(sql.SQL(reg))
            cur.execute(stmt)
            conn.commit()
            print("\n✅ ลบข้อมูลเรียบร้อย (TRUNCATE ... CASCADE)")

if __name__ == "__main__":
    main()
