from __future__ import annotations
import pandas as pd
from pathlib import Path
from .utils import md5_id, normalize_text, parse_price_currency, to_json_text, append_df

def load_abt_buy(con, data_dir: str):
    d = Path(data_dir)

    sep = "\t"  # or: r"\t+|\s{2,}" with engine='python' for a more forgiving parser
    abt = pd.read_csv(d / "TableA.csv",
                      sep=sep, header=None, dtype=str,  # engine='python' if you switch to regex
                      names=["id", "name", "description"])
    buy = pd.read_csv(d / "TableB.csv",
                      sep=sep, header=None, dtype=str,  # engine='python' if you switch to regex
                      names=["id", "name", "description", "manufacturer", "price"])
    mapping = pd.read_csv(d / "matches.csv",
                          sep=sep, header=None, dtype=str,  # engine='python' if you switch to regex
                          names=["tablea_id", "tableb_id"])

    #Normalize column names to lowercase so that downstream code still calls r.get("name"), etc.
    abt.columns = [c.lower() for c in abt.columns]
    buy.columns = [c.lower() for c in buy.columns]
    mapping.columns = [c.lower() for c in mapping.columns]
    # Items: Abt
    abt_rows = []
    for _, r in abt.iterrows():
        price, currency = parse_price_currency(r.get("price"))
        abt_rows.append(dict(
            item_id = md5_id("abt_buy","tablea", r.get("id")),
            dataset = "abt_buy",
            dataset_item_key = f"tablea:{r.get('id')}",
            merchant = "tablea",
            site = "tablea.com",
            locale = None,
            brand = None,
            title = normalize_text(r.get("name")),
            description = normalize_text(r.get("description")),
            bullet_points = None,
            color = None,
            price = price,
            currency = currency,
            category = None,
            image_url = None,
            attrs = to_json_text({}),
            split = None,
            variant = None,
            version = None,
        ))
    abt_df = pd.DataFrame(abt_rows)

    # Items: Buy
    buy_rows = []
    for _, r in buy.iterrows():
        price, currency = parse_price_currency(r.get("price"))
        buy_rows.append(dict(
            item_id = md5_id("abt_buy","tableb", r.get("id")),
            dataset = "abt_buy",
            dataset_item_key = f"tableb:{r.get('id')}",
            merchant = "tableb",
            site = "tableb.com",
            locale = None,
            brand = r.get("manufacturer"),
            title = normalize_text(r.get("name")),
            description = normalize_text(r.get("description")),
            bullet_points = None,
            color = None,
            price = price,
            currency = currency,
            category = None,
            image_url = None,
            attrs = to_json_text({k:v for k,v in r.items() if k not in ('id','name','description','price','manufacturer')}),
            split = None,
            variant = None,
            version = None,
        ))
    buy_df = pd.DataFrame(buy_rows)

    items_df = pd.concat([abt_df, buy_df], ignore_index=True)
    # Create tables if needed
    con.execute("CREATE TABLE IF NOT EXISTS items AS SELECT * FROM items WHERE 1=0")
    con.execute("CREATE TABLE IF NOT EXISTS item_item_pairs AS SELECT * FROM item_item_pairs WHERE 1=0")

    append_df(con, "items", items_df)

    # Pairs
    # mapping has columns like idabt, idbuy
    left_ids = mapping.columns[0]
    right_ids = mapping.columns[1]
    pair_rows = []
    for _, r in mapping.iterrows():
        left = md5_id("abt_buy","tablea", r.get(left_ids))
        right = md5_id("abt_buy","tableb", r.get(right_ids))
        pair_rows.append(dict(
            left_item_id = left,
            right_item_id = right,
            label = "match",
            pair_source = "gold",
            split = None,
            variant = None
        ))
    pair_df = pd.DataFrame(pair_rows)
    append_df(con, "item_item_pairs", pair_df)
