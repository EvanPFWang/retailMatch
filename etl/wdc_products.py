from __future__ import annotations
from pathlib import Path
import pandas as pd
from .utils import md5_id, normalize_text, parse_price_currency, to_json_text, append_df

def _detect_offers_file(dirpath: Path) -> Path|None:
    for p in dirpath.glob("*.csv"):
        try:
            head = pd.read_csv(p, nrows=5)
        except Exception:
            continue
        cols = [c.lower() for c in head.columns]
        if all(k in cols for k in ["id","title","description","price","pricecurrency","brand"]):
            return p
    for p in dirpath.glob("*.tsv"):
        try:
            head = pd.read_csv(p, nrows=5, sep="\t")
        except Exception:
            continue
        cols = [c.lower() for c in head.columns]
        if all(k in cols for k in ["id","title","description","price","pricecurrency","brand"]):
            return p
    return None

def _detect_pairs_file(dirpath: Path) -> Path|None:
    for p in list(dirpath.glob("pairs.*")) + list(dirpath.glob("*pairs*.csv")) + list(dirpath.glob("*pairs*.tsv")):
        try:
            head = pd.read_csv(p, nrows=5) if p.suffix != ".tsv" else pd.read_csv(p, nrows=5, sep="\t")
        except Exception:
            continue
        cols = [c.lower() for c in head.columns]
        if all(k in cols for k in ["left_id","right_id","label"]):
            return p
    # heuristic fallback
    for p in dirpath.glob("*.csv"):
        head = pd.read_csv(p, nrows=5)
        cols = [c.lower() for c in head.columns]
        if all(k in cols for k in ["left_id","right_id","label"]):
            return p
    return None

def _detect_multiclass_file(dirpath: Path) -> Path|None:
    for p in list(dirpath.glob("*offer_to_entity*.csv")) + list(dirpath.glob("*offer*entity*.csv")) + list(dirpath.glob("*multi*.csv")):
        try:
            head = pd.read_csv(p, nrows=5)
        except Exception:
            continue
        cols = [c.lower() for c in head.columns]
        if any(k in cols for k in ["offer_id","id","item_id"]) and any("entity" in c.lower() for c in cols):
            return p
    return None

def load_wdc_variant(con, variant_dir: str, variant_name: str|None=None, split: str|None=None):
    d = Path(variant_dir)
    offers_file = _detect_offers_file(d)
    if not offers_file:
        raise FileNotFoundError(f"No offers file with expected columns found in {variant_dir}")
    is_tsv = offers_file.suffix == ".tsv"
    offers = pd.read_csv(offers_file, sep="\t" if is_tsv else ",", low_memory=False)
    offers.columns = [c.strip().lower() for c in offers.columns]

    rows = []
    for _, r in offers.iterrows():
        oid = str(r["id"])
        price, currency = parse_price_currency(r.get("price"))
        rows.append(dict(
            item_id = md5_id("wdc", oid),
            dataset = "wdc",
            dataset_item_key = oid,
            merchant = None,
            site = None,
            locale = None,
            brand = r.get("brand"),
            title = normalize_text(r.get("title")),
            description = normalize_text(r.get("description")),
            bullet_points = None,
            color = None,
            price = price,
            currency = r.get("pricecurrency") or currency,
            category = None,
            image_url = None,
            attrs = to_json_text({k:v for k,v in r.items() if k not in ("id","title","description","price","pricecurrency","brand")}),
            split = split,
            variant = variant_name,
            version = "2024"
        ))
    items_df = pd.DataFrame(rows)
    append_df(con, "items", items_df)

    # Pairs (if present)
    pf = _detect_pairs_file(d)
    if pf:
        head = pd.read_csv(pf, nrows=5) if pf.suffix != ".tsv" else pd.read_csv(pf, nrows=5, sep="\t")
        cols = [c.lower() for c in head.columns]
        sep = "\t" if pf.suffix == ".tsv" else ","
        pairs = pd.read_csv(pf, sep=sep)
        pairs.columns = [c.strip().lower() for c in pairs.columns]
        lcol = "left_id"
        rcol = "right_id"
        lab = "label"
        prow = []
        for _, r in pairs.iterrows():
            prow.append(dict(
                left_item_id = md5_id("wdc", str(r[lcol])),
                right_item_id = md5_id("wdc", str(r[rcol])),
                label = str(r[lab]).lower() if pd.notna(r[lab]) else None,
                pair_source = "benchmark",
                split = split,
                variant = variant_name
            ))
        append_df(con, "item_item_pairs", pd.DataFrame(prow))

    # Multi-class (if present)
    mf = _detect_multiclass_file(d)
    if mf:
        mc = pd.read_csv(mf)
        mc.columns = [c.strip().lower() for c in mc.columns]
        # Try common column names
        offer_col = next((c for c in mc.columns if c in ("offer_id","id","item_id")), None)
        ent_col = next((c for c in mc.columns if "entity" in c), None)
        if offer_col and ent_col:
            # entities
            ent_ids = mc[ent_col].astype(str).unique().tolist()
            ent_df = pd.DataFrame([dict(entity_id=f"wdc:{e}", dataset="wdc", notes=variant_name) for e in ent_ids])
            append_df(con, "entities", ent_df)
            # links
            link_rows = []
            for _, r in mc.iterrows():
                link_rows.append(dict(
                    item_id = md5_id("wdc", str(r[offer_col])),
                    entity_id = f"wdc:{str(r[ent_col])}"
                ))
            append_df(con, "item_entity", pd.DataFrame(link_rows))

def load_wdc(con, base_dir: str):
    base = Path(base_dir)
    # Load subfolders as separate variants
    subs = [p for p in base.iterdir() if p.is_dir()]
    if not subs:
        # allow direct folder with files
        load_wdc_variant(con, str(base), variant_name=base.name, split=None)
    else:
        for s in subs:
            # try to infer split from folder name
            split = None
            name = s.name
            for tok in ("train","val","valid","validation","test","dev"):
                if tok in name.lower():
                    split = tok
                    break
            load_wdc_variant(con, str(s), variant_name=name, split=split)
