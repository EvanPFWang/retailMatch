PRAGMA threads=4;

CREATE TABLE IF NOT EXISTS items (
  item_id TEXT PRIMARY KEY,
  dataset TEXT,
  dataset_item_key TEXT,
  merchant TEXT,
  site TEXT,
  locale TEXT,
  brand TEXT,
  title TEXT,
  description TEXT,
  bullet_points TEXT,
  color TEXT,
  price DOUBLE,
  currency TEXT,
  category TEXT,
  image_url TEXT,
  attrs TEXT,            -- JSON-serialized string
  split TEXT,
  variant TEXT,
  version TEXT
);

CREATE TABLE IF NOT EXISTS queries (
  query_id TEXT PRIMARY KEY,
  dataset TEXT,
  query_text TEXT,
  locale TEXT,
  query_type TEXT,
  source TEXT,
  session_id TEXT,
  event_date TEXT
);

CREATE TABLE IF NOT EXISTS query_item_labels (
  query_id TEXT,
  item_id TEXT,
  label_family TEXT,
  label TEXT,
  position INTEGER,
  session_id TEXT,
  timeframe_ms BIGINT,
  split TEXT
);

CREATE TABLE IF NOT EXISTS item_item_pairs (
  left_item_id TEXT,
  right_item_id TEXT,
  label TEXT,            -- match / non_match
  pair_source TEXT,      -- gold / generated / benchmark
  split TEXT,
  variant TEXT
);

CREATE TABLE IF NOT EXISTS entities (
  entity_id TEXT PRIMARY KEY,
  dataset TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS item_entity (
  item_id TEXT,
  entity_id TEXT
);

CREATE TABLE IF NOT EXISTS lineage (
  origin_ref TEXT,
  download_url TEXT,
  hash TEXT,
  dataset_version TEXT,
  notes TEXT
);
