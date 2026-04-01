import logging
import datetime
import json
import hashlib
import time
import os
import requests
import feedparser
import trafilatura

from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosResourceNotFoundError


# --- CONFIGURATION ---
BASE = "https://www.supplychainbrain.com"
SOURCE_DOMAIN = urlparse(BASE).netloc.replace("www.", "")

BLOB_CONN_STR = os.environ.get("AzureWebJobsStorage")
COSMOS_URL = os.environ.get("COSMOS_URL")
COSMOS_KEY = os.environ.get("COSMOS_KEY")

CONTAINER_TEXT = "raw-text"
CONTAINER_STATE = "scraper-state2"
DB_NAME = "ScraperDB"
DB_CONTAINER = "Articles"

CRAWL_DELAY_SECONDS = int(os.environ.get("CRAWL_DELAY_SECONDS", "3"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "25"))
MAX_ARTICLES_PER_RUN = int(os.environ.get("MAX_ARTICLES_PER_RUN", "50"))
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "1"))

FEEDS = [
    f"{BASE}/rss/topic/1163-business-strategy-alignment",
    f"{BASE}/rss/topic/1376-customer-relationship-management",
    f"{BASE}/rss/topic/1164-education-professional-development",
    f"{BASE}/rss/topic/1165-global-supply-chain-management",
    f"{BASE}/rss/topic/1377-green-energy",
    f"{BASE}/rss/topic/1167-hr-labor-management",
    f"{BASE}/rss/topic/1169-quality-metrics",
    f"{BASE}/rss/topic/1170-regulation-compliance",
    f"{BASE}/rss/topic/1160-sourcing-procurement-srm",
    f"{BASE}/rss/topic/1171-sc-security-risk-mgmt",
    f"{BASE}/rss/topic/1347",
    f"{BASE}/rss/topic/1173-sustainability-corporate-social-responsibility",
    f"{BASE}/rss/topic/1182-aerospace-defense",
    f"{BASE}/rss/topic/1183-apparel",
    f"{BASE}/rss/topic/1184-automotive",
    f"{BASE}/rss/topic/1185-chemicals-energy",
    f"{BASE}/rss/topic/1186-consumer-packaged-goods",
    f"{BASE}/rss/topic/1187-e-commerce-omni-channel",
    f"{BASE}/rss/topic/1188-food-beverage",
    f"{BASE}/rss/topic/1189-healthcare",
    f"{BASE}/rss/topic/1190-high-tech-electronics",
    f"{BASE}/rss/topic/1191-industrial-manufacturing",
    f"{BASE}/rss/topic/1192-pharmaceutical-biotech",
    f"{BASE}/rss/topic/1193-retail",
]

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Compatible; AzureBot/1.0)",
    "Accept": "application/rss+xml, text/xml, */*",
}

NON_NEWS_KEYWORDS = {
    "opinion", "opinions", "view", "views", "commentary", "editorial",
    "perspective", "thoughts", "think tank", "blog", "blogs",
    "podcast", "podcasts", "webinar", "webinars", "white paper", "whitepaper",
    "case study", "case studies", "sponsored", "advertorial", "forecast",
    "predictions", "trend", "trends", "how to", "q&a", "watch:", "listen:",
    "video", "videos", "magazine", "digital edition",
    "feature story", "feature stories", "scb feature"
}

NON_NEWS_URL_PARTS = (
    "/blogs",
    "/podcasts",
    "/webinars",
    "/whitepapers",
    "/videos",
    "/publications",
    "/authors",
    "/newsletters",
)

BLOCKED_AUTHOR_TERMS = {
    "contributor",
    "guest author",
    "columnist",
    "op-ed",
    "op ed",
    "think tank",
    "blogger",
}

TRUSTED_AUTHOR_TERMS = {
    "supplychainbrain",
    "editor",
    "managing editor",
    "senior editor",
    "news desk",
}

# Words that often indicate "analysis" rather than direct news reporting.
# Do not hard-reject solely on these in page body; use them to mark borderline items.
BORDERLINE_KEYWORDS = {
    "analysis",
    "insight",
    "insights",
    "explainer",
    "deep dive",
    "outlook",
    "strategy",
}


app = func.FunctionApp()


# --- HELPERS ---

def validate_config():
    missing = []
    if not BLOB_CONN_STR:
        missing.append("AzureWebJobsStorage")
    if not COSMOS_URL:
        missing.append("COSMOS_URL")
    if not COSMOS_KEY:
        missing.append("COSMOS_KEY")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def get_blob_client(container, blob_name):
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    return blob_service.get_blob_client(container=container, blob=blob_name)


def load_state_from_blob(filename, default_value):
    try:
        client = get_blob_client(CONTAINER_STATE, filename)
        if not client.exists():
            return default_value
        data = client.download_blob().readall()
        return json.loads(data)
    except Exception as e:
        logging.warning(f"Could not load state {filename}: {e}")
        return default_value


def save_state_to_blob(filename, data):
    try:
        client = get_blob_client(CONTAINER_STATE, filename)
        client.upload_blob(json.dumps(data), overwrite=True)
    except Exception as e:
        logging.error(f"Failed to save state {filename}: {e}")


def get_cosmos_container():
    client = CosmosClient(COSMOS_URL, COSMOS_KEY)
    db = client.create_database_if_not_exists(id=DB_NAME)
    return db.create_container_if_not_exists(
        id=DB_CONTAINER,
        partition_key=PartitionKey(path="/source_domain")
    )


def ensure_utc(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def parse_date(val):
    try:
        return ensure_utc(datetime.datetime.fromisoformat(val.replace("Z", "+00:00")))
    except Exception:
        try:
            return ensure_utc(parsedate_to_datetime(val))
        except Exception:
            return None


def normalized_text(*parts):
    return " ".join(str(p or "") for p in parts).strip().lower()


def contains_non_news_keywords(text):
    text = (text or "").lower()
    return any(term in text for term in NON_NEWS_KEYWORDS)


def contains_borderline_keywords(text):
    text = (text or "").lower()
    return any(term in text for term in BORDERLINE_KEYWORDS)


def is_allowed_article_path(link):
    try:
        parsed = urlparse(link)
        path = (parsed.path or "").lower()

        if any(part in path for part in NON_NEWS_URL_PARTS):
            return False

        return path.startswith("/articles/")
    except Exception:
        return False


def entry_tags_text(entry):
    terms = []
    for tag in entry.get("tags", []) or []:
        term = tag.get("term")
        if term:
            terms.append(term)
    return " ".join(terms)


def extract_entry_author(entry):
    author = entry.get("author", "") or ""
    if not author and entry.get("authors"):
        author = " ".join(
            a.get("name", "")
            for a in entry.get("authors", [])
            if a.get("name")
        )
    return author.strip()


def classify_author(author_text):
    author_l = (author_text or "").strip().lower()

    if not author_l:
        return "none"

    if any(term in author_l for term in BLOCKED_AUTHOR_TERMS):
        return "blocked"

    if any(term in author_l for term in TRUSTED_AUTHOR_TERMS):
        return "trusted"

    return "unknown"


def is_probably_factual_news_entry(entry):
    """
    RSS-level filter.
    Returns:
      (True/False, reason, review_flag, author_classification)
    """
    link = entry.get("link", "") or ""
    if not link:
        return False, "missing_link", False, "none"

    if not is_allowed_article_path(link):
        return False, "non_article_path", False, "none"

    title = entry.get("title", "") or ""
    summary = entry.get("summary", "") or entry.get("description", "") or ""
    tags_text = entry_tags_text(entry)
    author = extract_entry_author(entry)
    author_class = classify_author(author)

    combined = normalized_text(title, summary, tags_text)

    if contains_non_news_keywords(combined):
        return False, "non_news_keyword", False, author_class

    if author_class == "blocked":
        return False, "blocked_author", False, author_class

    review_flag = False
    if author_class == "unknown":
        review_flag = True

    if contains_borderline_keywords(combined):
        review_flag = True

    return True, "ok", review_flag, author_class


def fetch_feed(session, url, validators):
    headers = dict(REQUEST_HEADERS)
    meta = validators.get(url, {})

    if meta.get("etag"):
        headers["If-None-Match"] = meta["etag"]
    if meta.get("last_modified"):
        headers["If-Modified-Since"] = meta["last_modified"]

    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        logging.error(f"Network error fetching {url}: {e}")
        return 0, None, meta

    if resp.status_code == 304:
        return 304, None, meta

    new_meta = meta.copy()
    if resp.headers.get("ETag"):
        new_meta["etag"] = resp.headers["ETag"]
    if resp.headers.get("Last-Modified"):
        new_meta["last_modified"] = resp.headers["Last-Modified"]

    return resp.status_code, resp.content, new_meta


def fetch_article_html(session, url):
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logging.error(f"Failed to fetch article HTML {url}: {e}")
        return None


def page_has_blocked_author_or_byline(soup, html_text):
    selectors = [
        '[class*="contributor"]',
        '[id*="contributor"]',
        '[class*="columnist"]',
        '[id*="columnist"]',
    ]

    for selector in selectors:
        if soup.select_one(selector):
            return True

    text = (html_text or "").lower()

    blocked_patterns = [
        "contributor",
        "guest author",
        "columnist",
        "op-ed",
        "op ed",
        "think tank",
    ]

    return any(pattern in text for pattern in blocked_patterns)


def extract_page_author(soup):
    meta_author = soup.select_one('meta[name="author"]')
    if meta_author and meta_author.get("content"):
        return meta_author["content"].strip()

    meta_article_author = soup.select_one('meta[property="article:author"]')
    if meta_article_author and meta_article_author.get("content"):
        return meta_article_author["content"].strip()

    byline_selectors = [
        '[class*="author"]',
        '[id*="author"]',
        '[class*="byline"]',
        '[id*="byline"]',
    ]

    for selector in byline_selectors:
        node = soup.select_one(selector)
        if node:
            text = node.get_text(" ", strip=True)
            if text:
                return text

    return ""


def is_probably_factual_news_page(url, html):
    """
    Page-level filter.
    Returns:
      (True/False, reason, review_flag, author_classification, extracted_author)
    """
    if not html:
        return False, "no_html", False, "none", ""

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return False, "html_parse_error", False, "none", ""

    title = soup.title.get_text(" ", strip=True) if soup.title else ""

    meta_desc = ""
    desc_tag = soup.find("meta", attrs={"name": "description"})
    if desc_tag and desc_tag.get("content"):
        meta_desc = desc_tag["content"]

    page_text = soup.get_text(" ", strip=True)
    first_slice = page_text[:4000].lower()
    combined = normalized_text(title, meta_desc, first_slice, url)

    if any(part in url.lower() for part in NON_NEWS_URL_PARTS):
        return False, "page_non_article_path", False, "none", ""

    if not is_allowed_article_path(url):
        return False, "page_not_articles_path", False, "none", ""

    if contains_non_news_keywords(combined):
        return False, "page_non_news_keyword", False, "none", ""

    if page_has_blocked_author_or_byline(soup, first_slice):
        return False, "page_blocked_author", False, "blocked", ""

    extracted_author = extract_page_author(soup)
    author_class = classify_author(extracted_author)

    if author_class == "blocked":
        return False, "page_blocked_author", False, author_class, extracted_author

    review_flag = False
    if author_class == "unknown":
        review_flag = True

    if contains_borderline_keywords(combined):
        review_flag = True

    return True, "ok", review_flag, author_class, extracted_author


def extract_content_from_html(html):
    try:
        return trafilatura.extract(html, include_comments=False)
    except Exception as e:
        logging.error(f"Trafilatura extraction error: {e}")
        return None


def classify_final_decision(entry_review, page_review, entry_author_class, page_author_class):
    """
    Simple decision layer before future LLM integration.
    """
    if entry_author_class == "blocked" or page_author_class == "blocked":
        return "reject"

    if entry_review or page_review:
        return "review"

    return "accept"


# --- MAIN FUNCTION ---

@app.schedule(schedule="0 0 6 * * *", arg_name="myTimer", run_on_startup=False)
def DailyScraper(myTimer: func.TimerRequest) -> None:
    validate_config()
    logging.info("Starting DailyScraper")

    validators = load_state_from_blob("validators.json", {})
    cosmos_container = get_cosmos_container()

    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)

    text_container_client = blob_service.get_container_client(CONTAINER_TEXT)
    if not text_container_client.exists():
        text_container_client.create_container()

    state_container_client = blob_service.get_container_client(CONTAINER_STATE)
    if not state_container_client.exists():
        state_container_client.create_container()

    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)

    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=LOOKBACK_DAYS)

    processed_count = 0
    skipped_non_news = 0
    review_count = 0

    try:
        for feed_url in FEEDS:
            if processed_count >= MAX_ARTICLES_PER_RUN:
                logging.info("Reached max article limit for this run")
                break

            logging.info(f"Checking feed: {feed_url}")
            status, content, new_meta = fetch_feed(session, feed_url, validators)
            validators[feed_url] = new_meta

            if status == 304:
                logging.info(f"No changes: {feed_url}")
                continue

            if status != 200 or not content:
                logging.warning(f"Feed failed with status {status}: {feed_url}")
                continue

            feed = feedparser.parse(content)

            for entry in feed.entries:
                if processed_count >= MAX_ARTICLES_PER_RUN:
                    break

                link = entry.get("link")
                if not link:
                    continue

                pub_date_str = entry.get("published") or entry.get("updated")
                published_iso = None
                if pub_date_str:
                    dt = parse_date(pub_date_str)
                    if dt:
                        published_iso = dt.isoformat()
                        if dt < cutoff:
                            continue

                ok_entry, entry_reason, entry_review, entry_author_class = is_probably_factual_news_entry(entry)
                if not ok_entry:
                    skipped_non_news += 1
                    logging.info(f"Skipping RSS entry [{entry_reason}]: {link}")
                    continue

                doc_id = hashlib.md5(link.encode("utf-8")).hexdigest()

                try:
                    cosmos_container.read_item(doc_id, partition_key=SOURCE_DOMAIN)
                    logging.info(f"Skipping existing: {doc_id}")
                    continue
                except CosmosResourceNotFoundError:
                    pass

                time.sleep(CRAWL_DELAY_SECONDS)

                html = fetch_article_html(session, link)
                if not html:
                    logging.info(f"Skipping unreadable page: {link}")
                    continue

                ok_page, page_reason, page_review, page_author_class, page_author = is_probably_factual_news_page(link, html)
                if not ok_page:
                    skipped_non_news += 1
                    logging.info(f"Skipping page [{page_reason}]: {link}")
                    continue

                body_text = extract_content_from_html(html)
                if not body_text or len(body_text.strip()) < 200:
                    logging.info(f"Skipping article with insufficient body text: {link}")
                    continue

                final_decision = classify_final_decision(
                    entry_review=entry_review,
                    page_review=page_review,
                    entry_author_class=entry_author_class,
                    page_author_class=page_author_class
                )

                if final_decision == "reject":
                    skipped_non_news += 1
                    logging.info(f"Rejected after final rules: {link}")
                    continue

                blob_name = f"{doc_id}.txt"
                blob_client = text_container_client.get_blob_client(blob_name)
                blob_client.upload_blob(body_text, overwrite=True)
                blob_url = blob_client.url

                status_value = "new"
                content_type = "news_article"
                review_required = False

                if final_decision == "review":
                    status_value = "review"
                    content_type = "news_article_review"
                    review_required = True
                    review_count += 1

                item = {
                    "id": doc_id,
                    "source_domain": SOURCE_DOMAIN,
                    "url": link,
                    "title": entry.get("title"),
                    "published_at": published_iso or pub_date_str,
                    "blob_text_url": blob_url,
                    "scraped_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "status": status_value,
                    "content_type": content_type,
                    "filter_version": "rules_first_llm_ready_v1",
                    "review_required": review_required,
                    "entry_author_class": entry_author_class,
                    "page_author_class": page_author_class,
                    "page_author_value": page_author,
                    "entry_review_flag": entry_review,
                    "page_review_flag": page_review
                }

                cosmos_container.upsert_item(item)
                processed_count += 1
                logging.info(f"Saved article [{status_value}]: {item['title']}")

    finally:
        session.close()
        save_state_to_blob("validators.json", validators)

    logging.info(
        f"Run complete. Processed={processed_count}, SkippedNonNews={skipped_non_news}, Review={review_count}"
    )
