# Usage

Set environment variables:

```
DUNE_PASSWORD=
DUNE_USER=
DUNE_QUERY_ID=
```

(dune_query_id is the id of a new query you have to create in dune to act as a placeholder to run this query)

# Install

```bash
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

# Run

```bash
. venv/bin/activate
python -m src.main "2022-09-01 00:00:00" "2022-09-12 00:00:00"
```
