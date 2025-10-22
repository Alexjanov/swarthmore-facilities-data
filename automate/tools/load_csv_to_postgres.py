'''
Steps for File Export
    1. In Asset Essentials, in the column chooser button, select all
    2. More > Export > make sure you select visible columns, not all columns


'''

#!/usr/bin/env python3
import argparse
import csv
import os
import sys
import tempfile
from typing import List, Dict

import psycopg2
import psycopg2.extras
from psycopg2 import sql
from dotenv import load_dotenv

# --- Load settings from .env ---
load_dotenv()

PG_CONN = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "user": os.getenv("PGUSER", "alex"),
    "password": os.getenv("PGPASSWORD", "secret123"),
    "dbname": os.getenv("PGDATABASE", "analytics"),
}

TARGET_SCHEMA = os.getenv("PGTARGET_SCHEMA", "raw")
TARGET_TABLE = os.getenv("PGTARGET_TABLE", "facilities")
UPSERT_KEY = os.getenv("PGUPSERT_KEY", "")

# Columns + types from .env
COLS_KEEP: List[str] = [
    c.strip() for c in os.getenv("COLS_KEEP", "").split(",") if c.strip()
]
COL_TYPES: List[str] = [
    t.strip() for t in os.getenv("COL_TYPES", "").split(",") if t.strip()
]
if len(COLS_KEEP) != len(COL_TYPES):
    print("ERROR: COLS_KEEP and COL_TYPES must have the same length.", file=sys.stderr)
    sys.exit(1)
COLUMN_TYPES: Dict[str, str] = dict(zip(COLS_KEEP, COL_TYPES))


def ensure_table(conn):
    """Create schema + table if not exists."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(TARGET_SCHEMA)
            )
        )
        cols_def = [
            sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(COLUMN_TYPES[c]))
            for c in COLS_KEEP
        ]
        create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({}{})").format(
            sql.Identifier(TARGET_SCHEMA),
            sql.Identifier(TARGET_TABLE),
            sql.SQL(", ").join(cols_def),
            sql.SQL(
                f", PRIMARY KEY ({UPSERT_KEY})"
                if UPSERT_KEY and UPSERT_KEY in COLS_KEEP
                else ""
            ),
        )
        cur.execute(create_stmt)
    conn.commit()


def filter_csv(in_path: str) -> str:
    """Write a temp CSV with only kept columns, in order."""
    with open(in_path, "r", encoding="utf-8", newline="") as fin:
        dr = csv.DictReader(fin)
        missing = [c for c in COLS_KEEP if c not in dr.fieldnames]
        if missing:
            print(f"ERROR: CSV is missing columns: {missing}", file=sys.stderr)
            sys.exit(2)
        tmp = tempfile.NamedTemporaryFile(
            "w", delete=False, encoding="utf-8", newline=""
        )
        with tmp as fout:
            dw = csv.DictWriter(fout, fieldnames=COLS_KEEP)
            dw.writeheader()
            for row in dr:
                dw.writerow({c: row.get(c, "") for c in COLS_KEEP})
        return tmp.name


def load_csv(conn, filtered_path):
    """Append or upsert data from filtered CSV into Postgres."""
    with conn.cursor() as cur, open(filtered_path, "r", encoding="utf-8") as f:
        if UPSERT_KEY and UPSERT_KEY in COLS_KEEP:
            # Load into temp staging
            stg = f"{TARGET_TABLE}__stg"
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier(TARGET_SCHEMA), sql.Identifier(stg)
                )
            )
            cur.execute(
                sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP").format(
                    sql.Identifier(stg),
                    sql.SQL(", ").join(
                        sql.SQL("{} {}").format(
                            sql.Identifier(c), sql.SQL(COLUMN_TYPES[c])
                        )
                        for c in COLS_KEEP
                    ),
                )
            )
            copy_sql = sql.SQL(
                "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
            ).format(
                sql.Identifier(stg),
                sql.SQL(", ").join(sql.Identifier(c) for c in COLS_KEEP),
            )
            cur.copy_expert(copy_sql.as_string(conn), f)
            # Merge into target
            set_clause = sql.SQL(", ").join(
                sql.SQL("{c}=EXCLUDED.{c}").format(c=sql.Identifier(c))
                for c in COLS_KEEP
                if c != UPSERT_KEY
            )
            insert_sql = sql.SQL(
                """
                INSERT INTO {}.{} ({cols})
                SELECT {cols} FROM {stg}
                ON CONFLICT ({pk}) DO UPDATE SET {set_clause}
            """
            ).format(
                sql.Identifier(TARGET_SCHEMA),
                sql.Identifier(TARGET_TABLE),
                cols=sql.SQL(", ").join(sql.Identifier(c) for c in COLS_KEEP),
                stg=sql.Identifier(stg),
                pk=sql.Identifier(UPSERT_KEY),
                set_clause=set_clause,
            )
            cur.execute(insert_sql)
        else:
            copy_sql = sql.SQL(
                "COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
            ).format(
                sql.Identifier(TARGET_SCHEMA),
                sql.Identifier(TARGET_TABLE),
                sql.SQL(", ").join(sql.Identifier(c) for c in COLS_KEEP),
            )
            cur.copy_expert(copy_sql.as_string(conn), f)
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to input CSV")
    args = ap.parse_args()

    filtered = filter_csv(args.csv)
    conn = psycopg2.connect(**PG_CONN)
    try:
        ensure_table(conn)
        load_csv(conn, filtered)
        print(f"Loaded into {PG_CONN['dbname']}.{TARGET_SCHEMA}.{TARGET_TABLE}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

# to run: python load_csv_to_postgres.py --csv /Users/you_path_name
