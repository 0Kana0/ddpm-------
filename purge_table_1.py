# purge_table.py
import os
import argparse
import psycopg
from psycopg import sql
from dotenv import load_dotenv

load_dotenv()

def get_conninfo():
    host = os.getenv("PGHOST", "")
    port = os.getenv("PGPORT", "")
    dbname = os.getenv("PGDATABASE", "")
    user = os.getenv("PGUSER", "")
    password = os.getenv("PGPASSWORD", "")
    sslmode = os.getenv("PGSSL")

    conninfo = {}
    if host: conninfo["host"] = host
    if port: conninfo["port"] = port
    if dbname: conninfo["dbname"] = dbname
    if user: conninfo["user"] = user
    if password: conninfo["password"] = password
    if sslmode: conninfo["sslmode"] = sslmode

    print(conninfo)
    return conninfo

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--table",
        default='public."User"',
        help='ชื่อตาราง (default: public."User") เช่น public."User" หรือ "User"',
    )
    ap.add_argument(
        "--keep-roles",
        nargs="+",
        default=["SUPERADMIN", "CENTRAL"],
        help="รายการ role ที่ต้องเก็บไว้ (default: SUPERADMIN CENTRAL)",
    )
    ap.add_argument("--dry-run", action="store_true", help="แสดงจำนวนที่จะลบ แต่ไม่ลบจริง")
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
                print('   ตัวอย่าง: --table public."User" หรือ --table \'"User"\'')
                return

            # นับจำนวนที่จะลบ (role NOT IN keep_roles)
            placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(args.keep_roles))
            count_stmt = sql.SQL("SELECT COUNT(*) FROM {} WHERE role NOT IN ({})").format(
                sql.SQL(reg),
                placeholders,
            )
            cur.execute(count_stmt, tuple(args.keep_roles))
            to_delete = cur.fetchone()[0]

            # นับจำนวนที่คงไว้
            keep_stmt = sql.SQL("SELECT COUNT(*) FROM {} WHERE role IN ({})").format(
                sql.SQL(reg),
                placeholders,
            )
            cur.execute(keep_stmt, tuple(args.keep_roles))
            to_keep = cur.fetchone()[0]

            print(f"Target: {reg}")
            print(f"Keep roles: {args.keep_roles}")
            print(f"Will keep: {to_keep} rows")
            print(f"Will delete: {to_delete} rows")

            if args.dry_run:
                print("\n✅ DRY RUN: ไม่ได้ลบข้อมูลจริง")
                return

            # ลบจริง
            del_stmt = sql.SQL("DELETE FROM {} WHERE role NOT IN ({})").format(
                sql.SQL(reg),
                placeholders,
            )
            cur.execute(del_stmt, tuple(args.keep_roles))
            deleted = cur.rowcount

            conn.commit()
            print(f"\n✅ ลบข้อมูลเรียบร้อย: deleted={deleted}, kept_roles={args.keep_roles}")

if __name__ == "__main__":
    main()