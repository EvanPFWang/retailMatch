from __future__ import annotations
import re, json, hashlib, unicodedata
from typing import Any, Dict, Iterable, Optional
import pandas as pd
import duckdb

def md5_id(*parts: str) -> str:
    h = hashlib.md5()
    for p in parts:
        if p is None:
            p = ""
        h.update(str(p).encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()

_ws_re = re.compile(r"\s+")
_tag_re = re.compile(r"<[^>]+>")

def normalize_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    # Unicode normalize + strip HTML tags + collapse spaces + lowercase (keep raw upstream if you need it)
    s = unicodedata.normalize("NFKC", str(s))
    s = _tag_re.sub(" ", s)
    s = _ws_re.sub(" ", s).strip()
    return s

_cur_map = {
    "$": "USD",
    "€": "EUR",
    "£": "GBP",
    "¥": "JPY",
}

_price_re = re.compile(r"([$\€£¥])?\s*([0-9]+(?:[.,][0-9]{1,2})?)")

def parse_price_currency(raw: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    if raw is None:
        return None, None
    m = _price_re.search(str(raw))
    if not m:
        # Sometimes price is numeric already
        try:
            return float(raw), None
        except Exception:
            return None, None
    symbol, num = m.groups()
    num = num.replace(",", "")
    try:
        val = float(num)
    except Exception:
        return None, _cur_map.get(symbol or "", None)
    return val, _cur_map.get(symbol or "", None)

def to_json_text(d: Dict[str, Any]) -> str:
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"))

def connect_duckdb(path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path)
    return con

def append_df(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame):
    if df is None or len(df) == 0:
        return
    # ensure table exists
    con.register("tmp_df", df)
    con.execute(f"INSERT INTO {table} SELECT * FROM tmp_df")
    con.unregister("tmp_df")
