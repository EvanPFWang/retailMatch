# retailMatch





Create a venv, install deps, put raw files under ./data/…, then call main.py with the DB path and which datasets to load, e.g.:



python -m venv .venv \&\& source .venv/bin/activate

pip install -r requirements.txt

python main.py --db db/retail\_bench.duckdb --load abt\_buy esci cikm16 wdc





That entrypoint connects to DuckDB, creates the tables, and dispatches to each loader.


Sanity Checks Post-Load:
sql
```
-- items per dataset

select dataset, count(\*) from items group by 1;



-- Abt-Buy gold pairs

select count(\*) from item\_item\_pairs where pair\_source='gold';



-- ESCI label distribution

select label, count(\*) from query\_item\_labels where label\_family='ESCI' group by 1;



-- WDC variants/splits landed

select variant, split, count(\*) from items where dataset='wdc' group by 1,2 order by 1,2;

```

