from __future__ import annotations
from pathlib import Path
import pandas as pd
from .utils import md5_id, normalize_text, to_json_text, append_df
#_examples: query-product paired judgements
"""example_id: Unique ID for labeled row (one query-product judgment). 


query: The raw customer query text string. Many rows share the same query text.

query_id: Unique identifier - all results/judgments for same query share ID
Used to group “up to ~40 products per query” in the original setup. 


product_id: points to specific Amazon catalog item (commonly treated as ASIN). 


product_locale: Which marketplace/language bucket query-product pair belongs to (US/ES/JP). 
part of join key to product metadata. 

esci_label: (human relevance judgment for query-product pair)
E (Exact): matches all query specs
S (Substitute): usable alternative but misses some spec
C (Complement): not requested item, but complements requested item
I (Irrelevant): doesn’t satisfy the query’s core intent 


small_version: flag (0/1) indicating the row belongs to the reduced/smaller dataset (used for Task 1 in the repo’s examples). 

large_version: Flag (0/1) indicating the row belongs to the larger dataset (superset; used for Tasks 2 and 3)

split: Split label (repo shows train and test) and stratified by query (all rows for a query stay together). 
"""


#_products: product information

def load_esci(con, data_dir: str):
    d = Path(data_dir)
    products_pq = d / "shopping_queries_dataset_products.parquet"
    examples_pq = d / "shopping_queries_dataset_examples.parquet"
    sources_csv = d / "shopping_queries_dataset_sources.csv"

    prods = pd.read_parquet(products_pq)
    ex = pd.read_parquet(examples_pq)
    src = pd.read_csv(sources_csv) if sources_csv.exists() else pd.DataFrame(columns=["query_id","source"])

    # Normalize column names
    prods.columns = [c.strip().lower() for c in prods.columns]
    ex.columns = [c.strip().lower() for c in ex.columns]
    src.columns = [c.strip().lower() for c in src.columns]

    # Items
    rows = []
    for _, r in prods.iterrows():
        pid = str(r["product_id"])
        loc = r["product_locale"]
        rows.append(dict(
            item_id = md5_id("esci", loc, pid),
            dataset = "esci",
            dataset_item_key = f"{loc}:{pid}",
            merchant = None,
            site = None,
            locale = loc,
            brand = r.get("product_brand"),
            title = normalize_text(r.get("product_title")),
            description = normalize_text(r.get("product_description")),
            bullet_points = normalize_text(r.get("product_bullet_point")),
            color = r.get("product_color"),
            price = None,
            currency = None,
            category = None,
            image_url = None,
            attrs = to_json_text({k:v for k,v in r.items() if k not in ("product_id","product_locale","product_brand","product_title","product_description","product_bullet_point","product_color")}),
            split = None,
            variant = None,
            version = None,
        ))
    items_df = pd.DataFrame(rows)
    append_df(con, "items", items_df)

    # Queries
    src_map = dict(zip(src["query_id"].astype(str), src["source"])) if len(src) else {}
    qrows = []
    for _, r in ex.iterrows():
        qid_native = str(r["query_id"])
        qrows.append(dict(
            query_id = md5_id("esci", qid_native),
            dataset = "esci",
            query_text = normalize_text(r["query"]),
            locale = r["product_locale"],
            query_type = "full",
            source = src_map.get(qid_native),
            session_id = None,
            event_date = None,
        ))
    q_df = pd.DataFrame(qrows).drop_duplicates(subset=["query_id"])
    append_df(con, "queries", q_df)

    # Labels
    lrows = []
    for _, r in ex.iterrows():
        qid = md5_id("esci", str(r["query_id"]))
        iid = md5_id("esci", r["product_locale"], str(r["product_id"]))
        lrows.append(dict(
            query_id = qid,
            item_id = iid,
            label_family = "ESCI",
            label = r["esci_label"],
            position = None,
            session_id = None,
            timeframe_ms = None,
            split = r.get("split")
        ))
    l_df = pd.DataFrame(lrows)
    append_df(con, "query_item_labels", l_df)
