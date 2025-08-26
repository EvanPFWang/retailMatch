from __future__ import annotations
import argparse, sys
from pathlib import Path
import duckdb

from etl.utils import connect_duckdb
from etl.abt_buy import load_abt_buy
from etl.cikm16 import load_cikm16
from etl.esci import load_esci
from etl.wdc_products import load_wdc

def ensure_schema(con, ddl_path: str):
    with open(ddl_path, "r") as f:
        sql = f.read()
    con.execute(sql)

def main():
    ap = argparse.ArgumentParser(description="Unify Abt-Buy, CIKM16, ESCI, and WDC Products into one DB")
    ap.add_argument("--db", required=True, help="DuckDB file path (e.g., db/retail_bench.duckdb)")
    ap.add_argument("--load", nargs="+", choices=["abt_buy","cikm16","esci","wdc"], required=True,
                    help="Datasets to load")
    ap.add_argument("--abt_dir", default="data/abt_buy", help="Path to Abt-Buy directory")
    ap.add_argument("--cikm_dir", default="data/cikm16", help="Path to CIKM16/DIGINETICA directory")
    ap.add_argument("--esci_dir", default="data/esci", help="Path to ESCI directory")
    ap.add_argument("--wdc_dir", default="data/wdc", help="Path to WDC Products directory (contains variants)")
    args = ap.parse_args()

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    con = connect_duckdb(args.db)
    ensure_schema(con, "etl/ddl_duckdb.sql")

    if "abt_buy" in args.load:
        print("Loading Abt-Buy...")
        load_abt_buy(con, args.abt_dir)
    if "cikm16" in args.load:
        print("Loading CIKM16/DIGINETICA... (this can take a while)")
        load_cikm16(con, args.cikm_dir)
    if "esci" in args.load:
        print("Loading ESCI...")
        load_esci(con, args.esci_dir)
    if "wdc" in args.load:
        print("Loading WDC Products...")
        load_wdc(con, args.wdc_dir)

    print("Done. DB at:", args.db)

if __name__ == "__main__":
    main()
