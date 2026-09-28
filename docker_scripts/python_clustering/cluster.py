import argparse
import os
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--n-clusters", type=int, required=True)
parser.add_argument("--input-table", required=True)
parser.add_argument("--output-table", required=True)
args = parser.parse_args()

# Connection details come from the environment, not hardcoded -- see Step 3.
db_url = (
    f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
    f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
)
engine = create_engine(db_url)
df = pd.read_sql_table(args.input_table, engine)

labels = KMeans(n_clusters=args.n_clusters, n_init="auto").fit_predict(
    df[["feature_a", "feature_b"]]
)
df["cluster"] = labels
df.to_sql(args.output_table, engine, if_exists="replace", index=False)
print(f"wrote {len(df)} rows with {args.n_clusters} clusters to {args.output_table}")
