# ─── reports helpers ───────────────────────────────────────────────
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import os

def paper_distribution(df):
    dist = (
        df["category_name"]
          .value_counts()
          .rename_axis("category_name")
          .reset_index(name="paper_cnt")
    )
    dist["pct"] = (dist["paper_cnt"] / dist["paper_cnt"].sum() * 100).round(2)
    return dist

def wordcount_stats(df):
    wc = (
        df.groupby("category_name")["word_count"]
          .agg(avg_word_count="mean", min_wc="min", max_wc="max")
          .reset_index()
          .sort_values("avg_word_count", ascending=False)
          .astype({"avg_word_count": "int"})
    )
    return wc

def export_reports(df, out_dir="reports", save_png=True):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 1. distribution
    dist = paper_distribution(df)
    dist.to_csv(out / "paper_distribution.csv", index=False)
    if save_png:
        plt.figure()
        dist.plot(kind="barh", x="category_name", y="paper_cnt", legend=False,
                  title="Papers per field", ylabel="")
        plt.tight_layout()
        plt.savefig(out / "paper_distribution.png", dpi=120)
        plt.close()

    # 2. word-count stats
    wc = wordcount_stats(df)
    wc.to_csv(out / "wordcount_by_category.csv", index=False)
    if save_png:
        plt.figure()
        wc.plot(kind="barh", x="category_name", y="avg_word_count", legend=False,
                title="Average word count", ylabel="")
        plt.tight_layout()
        plt.savefig(out / "wordcount_by_category.png", dpi=120)
        plt.close()

    # logging.info(f"📊 Reports saved to {out.resolve()}")
# ───────────────────────────────────────────────────────────────────

# … (твій код вище без змін) …

def build_reports():
    """Бере дані з Postgres та робить CSV+PNG."""
    import psycopg2
    PG_DSN = os.getenv(
        "PG_DSN",
        "dbname=datastore user=user password=password host=datastore port=5432",
    )
    with psycopg2.connect(PG_DSN) as conn:
        df = pd.read_sql("SELECT category_name, word_count FROM papers", conn)
    export_reports(df, out_dir="/opt/airflow/reports", save_png=True)
    print(os.listdir("/opt/airflow/reports"))
