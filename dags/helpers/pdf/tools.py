# import deepl
import fitz
import requests
import tempfile
import logging
import time

from requests.exceptions import RequestException
from http.client import IncompleteRead


def download_pdf_text(pdf_url: str, retries: int = 3, delay: int = 5) -> str | None:
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(pdf_url, timeout=20)
            response.raise_for_status()

            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                tmp_file.write(response.content)
                tmp_path = tmp_file.name

            doc = fitz.open(tmp_path)
            text = ""
            for page in doc:
                text += page.get_text()
            doc.close()

            return text

        except (RequestException, IncompleteRead, ConnectionError) as e:
            logging.error(f"❌ Failed processing {pdf_url} — {e} (attempt {attempt}/{retries})")
            time.sleep(delay)

    return None


# def deepl_translator(api_key, prompt, language="EN-US"):
#     translator = deepl.Translator(api_key)
#     result = translator.translate_text(prompt, target_lang=language)  # UK = Ukrainian
#     return result.text