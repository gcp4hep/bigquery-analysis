# bigquery-analysis

```
python table.py create-table test_replace_dot_remove_long.parquet physlitetestdata
python table.py delete-table physlitetestdata
```

```
python data.py prep-jsonl test_replace_dot_remove_long.parquet data.jsonl
python data.py load-data data.jsonl physlitetestdata
```
