import sqlite3
import pandas as pd

con = sqlite3.connect("wiki.db")

# Sanity check: how much data did we get?
check = pd.read_sql("""
    SELECT is_hoax, COUNT(DISTINCT article) AS articles, COUNT(*) AS revisions
    FROM revisions GROUP BY is_hoax
""", con)
print(check)

# Edit size and revert flag per revision
edits = pd.read_sql("""
    SELECT *,
           size - COALESCE(LAG(size) OVER (PARTITION BY article ORDER BY timestamp), 0)
               AS bytes_changed,
           CASE WHEN tags LIKE '%mw-reverted%' THEN 1 ELSE 0 END AS was_reverted
    FROM revisions
    ORDER BY article, timestamp
""", con)

print(edits.head(100))
edits.to_csv("edits_with_features.csv", index=False)