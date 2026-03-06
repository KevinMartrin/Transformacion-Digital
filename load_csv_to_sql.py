"""
load_csv_to_sql.py
------------------
Loads 5 CSV files into their corresponding Azure SQL Server tables.

Requirements:
    pip install pandas pyodbc sqlalchemy

Usage:
    1. Fill in your connection credentials in the CONFIG section below.
    2. Place this script in the same folder as your CSV files, OR update
       the CSV_DIR path to point to wherever the files are.
    3. Run:  python load_csv_to_sql.py
"""

import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import os

# ──────────────────────────────────────────────
#  CONFIG — fill in your Azure SQL Server details
# ──────────────────────────────────────────────
SERVER   = "taluxserver-test1-ver1.database.windows.net"   # e.g. myserver.database.windows.net
DATABASE = "talux-db-central_test1_v1"
USERNAME = "admin_talux"
PASSWORD = "1234test@team"
DRIVER   = "ODBC Driver 18 for SQL Server"      # adjust if you use a different version

# Folder that contains the 5 CSV files
CSV_DIR  = "."   # "." means same directory as this script

# ──────────────────────────────────────────────
#  TABLE DEFINITIONS
#  Maps each CSV file → SQL table name + column renames
# ──────────────────────────────────────────────
TABLES = {
    "autos": {
        "file": "TablasFinales/autos.csv",
        "rename": {
            "Año Vehiculo": "Vehiculo_anio",
        },
    },
}

# ──────────────────────────────────────────────
#  HELPERS
# ──────────────────────────────────────────────

def clean_dataframe(df: pd.DataFrame, rename_map: dict) -> pd.DataFrame:
    """Rename columns and convert Python booleans / NaN for SQL compatibility."""

    # Rename columns to match SQL schema
    df = df.rename(columns=rename_map)

    for col in df.columns:
        # Convert True/False strings or booleans → 1/0 (SQL BIT)
        if df[col].dtype == object:
            bool_map = {"True": 1, "False": 0, "true": 1, "false": 0}
            if df[col].dropna().isin(bool_map.keys()).all():
                df[col] = df[col].map(bool_map)

        # Convert boolean dtype → int
        if df[col].dtype == bool:
            df[col] = df[col].astype(int)

    # Replace NaN with None so pyodbc sends NULL
    df = df.where(pd.notnull(df), None)

    return df


def build_engine():
    """Create a SQLAlchemy engine for Azure SQL Server."""
    params = quote_plus(
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
    # Add only if driver is still not found after installation
    os.environ["ODBCINI"] = "/opt/homebrew/etc/odbc.ini"
    os.environ["ODBCSYSINI"] = "/opt/homebrew/etc"
    return create_engine(connection_string, fast_executemany=True)


# ──────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────

print("Connecting to Azure SQL Server...")
engine = build_engine()

# Quick connectivity check
with engine.connect() as conn:
    conn.execute(text("SELECT 1"))
print("✔ Connection successful.\n")

for table_name, config in TABLES.items():
    file_path = os.path.join(CSV_DIR, config["file"])

    print(f"Loading  {config['file']}  →  [{table_name}]")

    # --- Read CSV ---
    df = pd.read_csv(file_path, encoding="utf-8-sig")  # utf-8-sig handles BOM

    # --- Clean & rename ---
    df = clean_dataframe(df, config["rename"])

    # --- Load into SQL (append; table already exists) ---
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",   # won't recreate the table, just inserts rows
        index=False,
        chunksize=500,        # batch size — adjust if needed
    )

    print(f"   ✔ {len(df):,} rows inserted.\n")

print("All tables loaded successfully!")

