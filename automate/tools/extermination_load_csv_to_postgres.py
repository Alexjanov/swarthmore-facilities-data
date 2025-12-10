"""
Steps for File Export
    1. Export extermination data with all columns
    2. Ensure CSV includes: Date, Category, Address, Location, Room, Pest, Report, Reporter, Action A, Action B, Addtl Info

Expected Columns in CSV:
    - Date: Date of the extermination report
    - Category: Category of the service/issue
    - Address: Building/property address
    - Location: Specific location within building
    - Room: Room number or identifier
    - Pest: Type of pest reported
    - Report: Report description
    - Reporter: Person who reported the issue
    - Action A: First action taken
    - Action B: Second action taken
    - Addtl Info: Additional information/notes

Configuration via .env:
    Set COLS_KEEP to the columns you want (e.g., "Date,Category,Address,Location,Room,Pest,Report,Reporter,Action A,Action B,Addtl Info")
    Set COL_TYPES to matching PostgreSQL types (e.g., "DATE,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT")
    Or use simpler types if needed (e.g., all TEXT if Date is stored as text)
"""

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

# PostgreSQL connection settings
PG_CONN = {
    "host": os.getenv("PGHOST"),
    "port": int(os.getenv("PGPORT", "5432")),
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "dbname": os.getenv("PGDATABASE"),
}

# Validate required connection parameters
required_params = ["host", "user", "password", "dbname"]
missing = [k for k in required_params if not PG_CONN.get(k)]
if missing:
    print(
        f'ERROR: Missing required .env variables: {", ".join(f"PG{k.upper()}" for k in missing)}',
        file=sys.stderr,
    )
    sys.exit(1)

TARGET_SCHEMA = os.getenv("PGTARGET_SCHEMA_EXTERMINATION", "raw")
TARGET_TABLE = os.getenv("PGTARGET_TABLE_EXTERMINATION", "extermination")
UPSERT_KEY = os.getenv("PGUPSERT_KEY_EXTERMINATION", "")

# Columns + types from .env
COLS_KEEP_RAW: List[str] = [
    c.strip() for c in os.getenv("COLS_KEEP_EXTERMINATION", "").split(",") if c.strip()
]
COL_TYPES: List[str] = [
    t.strip() for t in os.getenv("COL_TYPES_EXTERMINATION", "").split(",") if t.strip()
]
if len(COLS_KEEP_RAW) != len(COL_TYPES):
    print(
        "ERROR: COLS_KEEP_EXTERMINATION and COL_TYPES_EXTERMINATION must have the same length.",
        file=sys.stderr,
    )
    sys.exit(1)

# Create lowercase versions for PostgreSQL
COLS_KEEP = [c.lower() for c in COLS_KEEP_RAW]
COLUMN_TYPES: Dict[str, str] = dict(zip(COLS_KEEP, COL_TYPES))
# Mapping from original CSV column names to lowercase versions
CSV_TO_PG_COLS: Dict[str, str] = dict(zip(COLS_KEEP_RAW, COLS_KEEP))


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
    """Write a temp CSV with only kept columns, in order, with lowercase headers."""
    with open(in_path, "r", encoding="utf-8", newline="") as fin:
        dr = csv.DictReader(fin)
        # Check for missing columns using original CSV column names
        missing = [c for c in COLS_KEEP_RAW if c not in dr.fieldnames]
        if missing:
            print(f"ERROR: CSV is missing columns: {missing}", file=sys.stderr)
            sys.exit(2)
        tmp = tempfile.NamedTemporaryFile(
            "w", delete=False, encoding="utf-8", newline=""
        )
        with tmp as fout:
            # Write with lowercase column names for PostgreSQL
            dw = csv.DictWriter(fout, fieldnames=COLS_KEEP)
            dw.writeheader()
            for row in dr:
                # Map from original CSV columns to lowercase
                dw.writerow(
                    {CSV_TO_PG_COLS[orig]: row.get(orig, "") for orig in COLS_KEEP_RAW}
                )
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

# Example .env configuration:
#
# PostgreSQL connection (required):
# PGHOST="localhost"
# PGPORT="5432"
# PGUSER="your_username"
# PGPASSWORD="your_password"
# PGDATABASE="analytics"
#
# Extermination-specific settings:
# COLS_KEEP_EXTERMINATION="Date,Category,Address,Location,Room,Pest,Report,Reporter,Action A,Action B,Addtl Info"
# COL_TYPES_EXTERMINATION="TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT"
# PGTARGET_SCHEMA_EXTERMINATION="raw"
# PGTARGET_TABLE_EXTERMINATION="extermination"
# PGUPSERT_KEY_EXTERMINATION=""
#
# To run: python extermination_load_csv_to_postgres.py --csv /path/to/your/extermination_data.csv
