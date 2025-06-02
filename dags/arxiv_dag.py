import os
import pendulum
import pandas as pd
import re
import logging

from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone

from helpers.db.db_client import save_to_postgres
from helpers.analytics.data_reports import build_reports
from helpers.scrapy.arxiv_cats_scraper import run_scraper as cats_scraper
from helpers.scrapy.arxiv_papers_spider import run_scraper as papers_scraper
from helpers.pdf.tools import download_pdf_text#, deepl_translator

DAG_ID = os.path.splitext(os.path.basename(__file__))[0]
SCHEDULE_INTERVAL = None #"0 3 * * *"
START_DATE = pendulum.datetime(2025, 5, 2, tz="Europe/Kyiv")
DEFAULT_ARGS = {
    "owner": "airflow", 
    "retries": 3, 
    "retry_delay": timedelta(seconds=30)
}

def scrape_wrapper(**context):
    categories = context["ti"].xcom_pull(task_ids="collect_categories")
    return papers_scraper(categories)

def process_article(article):
    try:
        text = download_pdf_text(article['pdf_url'])
        if text:
            article["paper_text"] = text
            # Translating text with one of the best AI tool
            # translated = deepl_translator(os.getenv("DEEPL_KEY"), text)
            # article["paper_text_uk"] = translated
            article["paper_text_uk"] = "mocked"
            article["word_count"] = len(re.findall(r'\b\w+\b', text))
    except Exception as e:
        logging.error(f"❌ Failed processing {article['pdf_url']} — {e}")
    return article


def process_docs(**context):
    ti = context["ti"]
    articles = ti.xcom_pull(task_ids="collect_articles")[:1000]
    total = len(articles)

    logging.info(f"⚙️ Start processing {total} articles...")

    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(process_article, article) for article in articles]
        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logging.warning(f"⚠️ Error in article #{i}: {e}")
            if i % 100 == 0:
                logging.info(f"✅ Processed {i}/{total}")

    return results

 
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Fetch and process data from arxiv.",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
) as dag:
    
    fetch_categories = PythonOperator(
        task_id="collect_categories",
        python_callable=cats_scraper
    )

    fetch_articles = PythonOperator(
        task_id="collect_articles",
        python_callable=scrape_wrapper
    )

    process_articles = PythonOperator(
        task_id="process_articles",
        python_callable=process_docs
    )

    save_db = PythonOperator(
        task_id="save_to_db",
        python_callable=lambda **ctx: save_to_postgres(
            ctx["ti"].xcom_pull(task_ids="process_articles")
        ),
    )

    calc_reports = PythonOperator(
        task_id="calc_reports",
        python_callable=build_reports,
    )
    fetch_categories >> fetch_articles >> process_articles >> [calc_reports, save_db]