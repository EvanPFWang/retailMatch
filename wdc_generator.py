from __future__ import annotations

import argparse
import json
import gzip
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from tqdm import tqdm


def _parse_variant_and_split(filename: str) -> Tuple[str, str | None]:
    """Infer the variant name and split from a file name.

    WDC Products file names follow the pattern::

        wdcproducts80cc20rnd000un_train_large.json.gz
        wdcproductsmulti50cc50rnd050un_valid_medium.json.gz

    The first token up to the first underscore is the variant name.
    The second token, if present, contains the split (train/valid/test).


    filename : str
        Name of the JSON file.

    Returns
    -------
    variant : str
        The variant identifier (e.g. ``wdcproducts80cc20rnd000un``).
    split : str | None
        The split string if ``train``, ``valid``/``validation`` or ``test`` appears in the name,
        otherwise ``None``.
    """
    base = filename
    # remove .json or .json.gz suffixes
    base = re.sub(r"\.(json|json\.gz)$", "", base)
    parts = base.split("_")
    variant = parts[0]
    split = None
    if len(parts) > 1:
        # check if any token resembles a split name
        for tok in parts[1:]:
            low = tok.lower()
            if low in {"train", "valid", "validation", "test", "dev"}:
                split = low
                break
    return variant, split


def _ensure_csv_append(path: Path, df: pd.DataFrame) -> None:
    """Append a DataFrame to a CSV, writing the header only on first write."""
    header = not path.exists()
    # create parent directories if needed
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, mode="a", header=header)


def _process_pair_file(json_path: Path, variant_dir: Path) -> None:
    """Convert a pair‑wise JSON‑lines file into offers and pairs CSV files.

    Parameters
    ----------
    json_path : Path
        Path to the gzip compressed JSON lines file.
    variant_dir : Path
        Directory where ``offers.csv`` and ``pairs.csv`` will be created/updated.
    """
    offers: Dict[str, Dict[str, str | None]] = {}
    pairs: List[Dict[str, int | str]] = []
    with gzip.open(json_path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            obj = json.loads(line)
            # Collect left offer
            left_id = str(obj["id_left"])
            offers[left_id] = {
                "id": left_id,
                "title": obj.get("title_left"),
                "description": obj.get("description_left"),
                "price": obj.get("price_left"),
                "pricecurrency": obj.get("priceCurrency_left"),
                "brand": obj.get("brand_left"),
            }
            # Collect right offer
            right_id = str(obj["id_right"])
            offers[right_id] = {
                "id": right_id,
                "title": obj.get("title_right"),
                "description": obj.get("description_right"),
                "price": obj.get("price_right"),
                "pricecurrency": obj.get("priceCurrency_right"),
                "brand": obj.get("brand_right"),
            }
            # Append pair
            label = obj.get("label")
            # In the WDC dataset labels are 1 for matches and 0 otherwise
            pairs.append({
                "left_id": left_id,
                "right_id": right_id,
                "label": int(label) if label is not None else None,
            })
    # Write offers and pairs
    offers_df = pd.DataFrame(list(offers.values()))
    pairs_df = pd.DataFrame(pairs)
    _ensure_csv_append(variant_dir / "offers.csv", offers_df)
    _ensure_csv_append(variant_dir / "pairs.csv", pairs_df)


def _process_multi_file(json_path: Path, variant_dir: Path) -> None:
    """Convert a multi‑class JSON‑lines file into offers and entity mapping CSV files."""
    # read JSON lines with pandas; compression='infer' detects .gz
    df = pd.read_json(json_path, lines=True)
    # Normalise column names: lower‑case priceCurrency -> pricecurrency
    if "priceCurrency" in df.columns:
        df.rename(columns={"priceCurrency": "pricecurrency"}, inplace=True)
    # Determine entity column
    entity_col = None
    if "label" in df.columns:
        # In WDC multi‑class files the label column holds the cluster id
        entity_col = "label"
    elif "cluster_id" in df.columns:
        entity_col = "cluster_id"
    # Build offers DataFrame
    offers_cols = [c for c in ["id", "title", "description", "price", "pricecurrency", "brand"] if c in df.columns]
    offers_df = df[offers_cols].copy()
    offers_df["id"] = offers_df["id"].astype(str)
    _ensure_csv_append(variant_dir / "offers.csv", offers_df)
    # Build mapping
    if entity_col is not None:
        mapping_df = df[["id", entity_col]].copy()
        mapping_df["id"] = mapping_df["id"].astype(str)
        mapping_df.rename(columns={"id": "offer_id", entity_col: "entity_id"}, inplace=True)
        mapping_df["entity_id"] = mapping_df["entity_id"].astype(str)
        _ensure_csv_append(variant_dir / "offer_to_entity.csv", mapping_df)


def _iterate_json_files(root: Path) -> List[Path]:
    """Return a list of JSON or JSON.GZ files in a directory tree."""
    files: List[Path] = []
    for p in root.rglob("*.json"):
        files.append(p)
    for p in root.rglob("*.json.gz"):
        files.append(p)
    return files


def _unpack_archive(archive_path: Path, dest: Path) -> None:
    """Unpack a zip/tar archive into the destination directory.

    The WDC benchmark archives are distributed as plain zip files.  This helper
    extracts them using the standard library.  Existing contents at the
    destination will be removed.
    """
    # clean destination
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)
    # Attempt zip extraction first
    try:
        import zipfile
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(path=dest)
        return
    except Exception:
        pass
    # Fallback to tar
    import tarfile
    with tarfile.open(archive_path, "r:*") as tf:
        tf.extractall(path=dest)


def convert_wdc(source: Path, output_base: Path) -> None:
    """Convert WDC Products JSON files into ETL friendly CSVs.

    Parameters
    ----------
    source : Path
        Either a directory containing JSON/JSON.GZ files or a zip/tar archive.
    output_base : Path
        Base directory where variant subdirectories will be created.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        # If source is an archive, unpack it into temporary directory
        data_root: Path
        if source.is_file():
            _unpack_archive(source, tmp)
            data_root = tmp
        else:
            data_root = source
        # Iterate through JSON files
        files = _iterate_json_files(data_root)
        if not files:
            raise RuntimeError(f"No JSON files found in {data_root}")
        for json_file in tqdm(files, desc="Processing WDC files"):
            variant, split = _parse_variant_and_split(json_file.name)
            variant_dir = output_base / variant
            # Ensure variant directory exists
            variant_dir.mkdir(parents=True, exist_ok=True)
            # Determine if this is a pair or multi variant
            if variant.lower().startswith("wdcproducts"):
                # pair‑wise dataset
                _process_pair_file(json_file, variant_dir)
            elif variant.lower().startswith("wdcproductsmulti"):
                # multi‑class dataset
                _process_multi_file(json_file, variant_dir)
            else:
                # Unknown variant naming; attempt to detect by columns
                # Read a single line to guess
                try:
                    with gzip.open(json_file, "rt", encoding="utf-8") as fh:
                        first = fh.readline()
                    rec = json.loads(first)
                except Exception:
                    rec = pd.read_json(json_file, nrows=1).iloc[0].to_dict()
                if any(k.endswith("_left") for k in rec.keys()):
                    _process_pair_file(json_file, variant_dir)
                else:
                    _process_multi_file(json_file, variant_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert WDC Products JSON archives into CSVs.")
    parser.add_argument("--input", required=True, type=str,
                        help="Path to a WDC archive (.zip/.tar) or directory containing JSON/JSON.GZ files.")
    parser.add_argument("--output", required=True, type=str,
                        help="Directory where the converted CSVs will be stored.  Variant subdirectories will be created here.")
    args = parser.parse_args()
    source = Path(args.input)
    output_base = Path(args.output)
    convert_wdc(source, output_base)
    print(f"Converted WDC data written to {output_base}")


if __name__ == "__main__":
    main()