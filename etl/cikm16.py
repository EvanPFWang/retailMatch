from __future__ import annotations
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from .utils import md5_id, normalize_text, parse_price_currency, to_json_text, append_df

def _load_products(con, d: Path):
    # products.csv and product-categories.csv are expected, but columns vary across mirrors.
    prod_path = d / "products.csv"
    cat_path = d / "product-categories.csv"
    if not prod_path.exists():
        raise FileNotFoundError(f"Missing {prod_path}")
    prods = pd.read_csv(prod_path)
    prods.columns = [c.strip() for c in prods.columns]

    # Best-effort column detection
    id_col = next((c for c in prods.columns if c.lower() in ("productid","product_id","itemid","item_id","id")), None)
    title_col = next((c for c in prods.columns if "title" in c.lower() or "name" in c.lower()), None)
    desc_col = next((c for c in prods.columns if "desc" in c.lower()), None)
    brand_col = next((c for c in prods.columns if "brand" in c.lower()), None)
    price_col = next((c for c in prods.columns if "price" in c.lower()), None)

    cats = None
    cat_map = {}
    if cat_path.exists():
        cats = pd.read_csv(cat_path)
        cats.columns = [c.strip() for c in cats.columns]
        pid_col = next((c for c in cats.columns if c.lower() in ("productid","product_id","itemid","item_id","id")), None)
        cat_col = next((c for c in cats.columns if "category" in c.lower()), None)
        if pid_col and cat_col:
            cat_map = dict(zip(cats[pid_col].astype(str), cats[cat_col].astype(str)))

    rows = []
    for _, r in prods.iterrows():
        pid = str(r.get(id_col))
        price, currency = parse_price_currency(r.get(price_col)) if price_col else (None,None)
        rows.append(dict(
            item_id = md5_id("cikm16", pid),
            dataset = "cikm16",
            dataset_item_key = pid,
            merchant = None,
            site = None,
            locale = None,
            brand = r.get(brand_col) if brand_col else None,
            title = normalize_text(r.get(title_col)) if title_col else None,
            description = normalize_text(r.get(desc_col)) if desc_col else None,
            bullet_points = None,
            color = None,
            price = price,
            currency = currency,
            category = cat_map.get(pid),
            image_url = None,
            attrs = to_json_text({k:v for k,v in r.items() if k not in (id_col, title_col, desc_col, brand_col, price_col)}),
            split = "train",
            variant = None,
            version = None,
        ))
    items_df = pd.DataFrame(rows)
    append_df(con, "items", items_df)

def _load_queries(con, d: Path):
    q_path = d / "train-queries.csv"
    if not q_path.exists():
        return
    q = pd.read_csv(q_path)
    q.columns = [c.strip() for c in q.columns]
    # Heuristic columns
    qid = next((c for c in q.columns if "queryid" in c.lower()), None)
    qtxt = next((c for c in q.columns if c.lower() in ("query","query_text","searchtokens","search_tokens")), None)
    qless = next((c for c in q.columns if "queryless" in c.lower()), None)
    locale = next((c for c in q.columns if "locale" in c.lower()), None)
    sess = next((c for c in q.columns if "session" in c.lower()), None)
    evdate = next((c for c in q.columns if "eventdate" in c.lower() or "event_date" in c.lower()), None)

    rows = []
    for _, r in q.iterrows():
        native_qid = str(r.get(qid)) if qid else normalize_text(r.get(qtxt)) or None
        qid_g = md5_id("cikm16", native_qid)
        rows.append(dict(
            query_id = qid_g,
            dataset = "cikm16",
            query_text = normalize_text(r.get(qtxt)) if qtxt else None,
            locale = r.get(locale) if locale else None,
            query_type = ("queryless" if (qless and r.get(qless)) else "full"),
            source = "train-queries",
            session_id = str(r.get(sess)) if sess else None,
            event_date = str(r.get(evdate)) if evdate else None,
        ))
    q_df = pd.DataFrame(rows)
    append_df(con, "queries", q_df)

def _load_interactions(con, d: Path, fname: str, label_family: str):
    p = d / fname
    if not p.exists():
        return
    chunks = pd.read_csv(p, chunksize=1_000_000)
    for ch in tqdm(chunks, desc=f"cikm16:{label_family}"):
        ch.columns = [c.strip() for c in ch.columns]
        qid = next((c for c in ch.columns if "queryid" in c.lower()), None)
        sess = next((c for c in ch.columns if "session" in c.lower()), None)
        item = next((c for c in ch.columns if c.lower() in ("itemid","productid","item_id","product_id")), None)
        timeframe = next((c for c in ch.columns if "timeframe" in c.lower()), None)
        pos = next((c for c in ch.columns if c.lower() in ("position","rank")), None)

        rows = []
        for _, r in ch.iterrows():
            native_qid = str(r.get(qid)) if qid else str(r.get(sess)) if sess else None
            gid = md5_id("cikm16", native_qid) if native_qid else None
            iid_native = str(r.get(item)) if item else None
            iid = md5_id("cikm16", iid_native) if iid_native else None
            rows.append(dict(
                query_id = gid,
                item_id = iid,
                label_family = label_family,
                label = 1 if label_family in ("click","purchase","view") else None,
                position = int(r.get(pos)) if pos is not None and pd.notna(r.get(pos)) else None,
                session_id = str(r.get(sess)) if sess else None,
                timeframe_ms = int(r.get(timeframe)) if timeframe and pd.notna(r.get(timeframe)) else None,
                split = "train"
            ))
        df = pd.DataFrame(rows)
        append_df(con, "query_item_labels", df)

def load_cikm16(con, data_dir: str):
    d = Path(data_dir)
    _load_products(con, d)
    _load_queries(con, d)
    _load_interactions(con, d, "train-item-views.csv", "view")       # browse/click/view log
    _load_interactions(con, d, "train-clicks.csv", "click")
    _load_interactions(con, d, "train-purchases.csv", "purchase")
