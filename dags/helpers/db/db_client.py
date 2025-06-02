import os
from io import StringIO
import pandas as pd
import psycopg2

PG_DSN = os.getenv(
    "PG_DSN",
    "dbname=datastore user=user password=password host=datastore port=5432",
)

DDL = """
CREATE TABLE IF NOT EXISTS papers (
    category_id     text,
    category_name   text,
    title           text,
    link            text,
    pdf_url         text,
    summary         text,
    published       timestamptz,     -- або text, якщо не парсите дату
    authors         text,            -- перелік авторів через “;”
    paper_text      text,
    paper_text_uk   text,
    word_count      integer
);
"""

def save_to_postgres(records: list[dict]) -> None:
    """Bulk-insert DataFrame → Postgres через COPY."""
    if not records:
        return

    df = pd.DataFrame(records)

    # ── підчищаємо та приводимо типи ───────────────────────────────
    df["word_count"] = df["word_count"].fillna(0).astype(int)

    # published → datetime (якщо хочете timestamptz у PG)
    if df["published"].dtype == object:
        df["published"] = pd.to_datetime(df["published"], errors="coerce", utc=True)

    # NULL-байти ламають COPY
    obj_cols = df.select_dtypes(include="object").columns
    df[obj_cols] = df[obj_cols].apply(
        lambda s: s.str.replace("\x00", "", regex=False)
    )

    # автори як один текстовий стовпець
    # if isinstance(df.iloc[0]["authors"], (list, tuple)):
    #     df["authors"] = df["authors"].apply(lambda lst: "; ".join(lst))

    # порядок колонок EXACTLY як у COPY
    cols = [
        "category_id",
        "category_name",
        "title",
        "link",
        "pdf_url",
        "summary",
        "published",
        "authors",
        "paper_text",
        "paper_text_uk",
        "word_count",
    ]

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(DDL)

        buf = StringIO()
        df[cols].to_csv(buf, index=False, header=False, quoting=1)  # quoting=csv.QUOTE_MINIMAL
        buf.seek(0)

        cur.copy_expert(
            f"COPY papers ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv)",
            buf,
        )
        conn.commit()
