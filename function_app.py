import logging
import datetime
import json
import hashlib
import re
import time
import os
import calendar
import requests
import feedparser
import trafilatura
from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse, urlunparse, urlencode, parse_qsl
 
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient, PartitionKey
from azure.core.exceptions import ResourceNotFoundError
 
# --- CONFIGURATION (Load from Environment Variables) ---
BASE = "https://www.supplychainbrain.com"
BLOB_CONN_STR = os.environ.get("AzureWebJobsStorage") # Uses the default storage account
COSMOS_URL = os.environ.get("COSMOS_URL")
COSMOS_KEY = os.environ.get("COSMOS_KEY")
 
# Container Names
CONTAINER_TEXT = "raw-text"
CONTAINER_STATE = "scraper-state" # Will hold validators.json and seen.json
DB_NAME = "ScraperDB"
DB_CONTAINER = "Articles"
 
# Settings
CRAWL_DELAY_SECONDS = 3 # Reduced for cloud execution
REQUEST_TIMEOUT = 25
MAX_ARTICLES_PER_RUN = 50 # Reduced to prevent Function Timeout (5-10 min limit)
 
FEEDS = [
    # f"{BASE}/rss/articles",
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
 
app = func.FunctionApp()
 
# --- AZURE HELPERS ---
 
def get_blob_client(container, blob_name):
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    return blob_service.get_blob_client(container=container, blob=blob_name)
 
def load_state_from_blob(filename, default_value):
    """Downloads seen.json or validators.json from Blob Storage"""
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
    """Uploads state back to Blob Storage"""
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
 
# --- YOUR EXISTING LOGIC (Refactored for Cloud) ---
 
def ensure_utc(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)
 
def parse_date(val):
    try:
        # Simple ISO parser
        return ensure_utc(datetime.datetime.fromisoformat(val.replace("Z", "+00:00")))
    except:
        try:
            # RSS parser
            return ensure_utc(parsedate_to_datetime(val))
        except:
            return None
 
def fetch_feed(session, url, validators):
    """Fetches feed handling ETags"""
    headers = {**REQUEST_HEADERS}
    meta = validators.get(url, {})
   
    if meta.get("etag"): headers["If-None-Match"] = meta["etag"]
    if meta.get("last_modified"): headers["If-Modified-Since"] = meta["last_modified"]
 
    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        logging.error(f"Network error fetching {url}: {e}")
        return 0, None, {}, meta
 
    if resp.status_code == 304:
        return 304, None, {}, meta
   
    # Update state
    new_meta = meta.copy()
    if resp.headers.get("ETag"): new_meta["etag"] = resp.headers.get("ETag")
    if resp.headers.get("Last-Modified"): new_meta["last_modified"] = resp.headers.get("Last-Modified")
   
    return resp.status_code, resp.content, resp.headers, new_meta
 
def extract_content(url):
    """Uses Trafilatura to get text"""
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(downloaded, include_comments=False)
            return text
    except Exception as e:
        logging.error(f"Trafilatura error on {url}: {e}")
    return None
 
# --- MAIN AZURE FUNCTION ---
 
@app.schedule(schedule="0 0 6 * * *", arg_name="myTimer", run_on_startup=False)
def DailyScraper(myTimer: func.TimerRequest) -> None:
    logging.info("Starting Daily Scraper...")
 
    # 1. Setup State
    validators = load_state_from_blob("validators.json", {})
    # Note: For 'seen', querying Cosmos DB is better, but to keep your logic
    # we can load the seen list. WARNING: This list will grow large over time.
    # It is better to rely on Cosmos DB upserts to handle deduplication.
   
    cosmos_container = get_cosmos_container()
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    text_container_client = blob_service.get_container_client(CONTAINER_TEXT)
    if not text_container_client.exists(): text_container_client.create_container()
   
    # Create State container if missing
    state_container_client = blob_service.get_container_client(CONTAINER_STATE)
    if not state_container_client.exists(): state_container_client.create_container()
 
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)
   
    # Define cutoff (e.g., last 24 hours)
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=20000)
 
    processed_count = 0
 
    for feed_url in FEEDS:
        if processed_count >= MAX_ARTICLES_PER_RUN:
            logging.info("Hit max limit for this run.")
            break
 
        logging.info(f"Checking feed: {feed_url}")
        status, content, headers, new_meta = fetch_feed(session, feed_url, validators)
        validators[feed_url] = new_meta # Update validator state
 
        if status == 304:
            logging.info("No changes (304).")
            continue
       
        if status != 200:
            logging.warning(f"Feed failed with {status}")
            continue
 
        # Parse RSS
        feed = feedparser.parse(content)
       
        for entry in feed.entries:
            if processed_count >= MAX_ARTICLES_PER_RUN: break
 
            link = entry.get("link")
            if not link: continue
 
            # Generate ID
            doc_id = hashlib.md5(link.encode("utf-8")).hexdigest()
           
            # Check date (RSS level filter)
            pub_date_str = entry.get("published") or entry.get("updated")
            if pub_date_str:
                dt = parse_date(pub_date_str)
                if dt and dt < cutoff:
                    continue # Too old
 
            # Check if exists in Cosmos (Deduplication)
            # This replaces "seen.json" efficiently
            try:
                cosmos_container.read_item(doc_id, partition_key="supplychainbrain.com")
                logging.info(f"Skipping existing: {doc_id}")
                continue
            except:
                pass # Item doesn't exist, proceed
 
            # Fetch Body Text
            time.sleep(CRAWL_DELAY_SECONDS)
            body_text = extract_content(link)
           
            blob_url = ""
            if body_text:
                # Upload Text to Blob
                blob_name = f"{doc_id}.txt"
                blob_client = text_container_client.get_blob_client(blob_name)
                blob_client.upload_blob(body_text, overwrite=True)
                blob_url = blob_client.url
 
            # Prepare Metadata
            item = {
                "id": doc_id,
                "source_domain": "supplychainbrain.com", # Partition Key
                "url": link,
                "title": entry.get("title"),
                "published_at": pub_date_str,
                "blob_text_url": blob_url,
                "scraped_at": datetime.datetime.utcnow().isoformat(),
                "status": "new"
            }
 
            # Save to Cosmos
            cosmos_container.upsert_item(item)
            processed_count += 1
            logging.info(f"Saved: {item['title']}")
 
    # Save validators state back to blob for next run
    save_state_to_blob("validators.json", validators)
    logging.info(f"Run complete. Processed {processed_count} items.")
