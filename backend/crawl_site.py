#!/usr/bin/env python3
"""
Manage sites, crawl them, and generate sitemaps with product/collection/page URLs.

Supports Shopify, WooCommerce, Magento, BigCommerce, PrestaShop, OpenCart,
Squarespace, Wix, and other stores via sitemap + common URL patterns.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
import threading
import time
import warnings
import xml.etree.ElementTree as ET
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

try:
    from .storage import load_file, load_json, persist_file, save_json, sync_directory
except ImportError:
    from storage import load_file, load_json, persist_file, save_json, sync_directory

try:
    from urllib3.exceptions import NotOpenSSLWarning

    warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
except ImportError:
    pass

SITEMAP_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"
SITEMAP_NS_MAP = {"sm": SITEMAP_NS}

PRODUCT_PATH_RES = (
    re.compile(r"^/products/[^/?#]+/?$", re.I),           # Shopify
    re.compile(r"^/product[s]?/[^/?#]+", re.I),            # WooCommerce, BigCommerce
    re.compile(r"^/shop/p/[^/?#]+", re.I),                 # Squarespace
    re.compile(r"^/store/p/[^/?#]+", re.I),                # Squarespace alt
    re.compile(r"^/catalog/product/", re.I),               # Magento
    re.compile(r"^/store/products?/", re.I),
    re.compile(r"^/p/[^/?#]+", re.I),
    re.compile(r"^/item[s]?/[^/?#]+", re.I),
    re.compile(r"^/buy/[^/?#]+", re.I),
    re.compile(r"^/dp/[^/?#]+", re.I),                     # Amazon-style
)

COLLECTION_PATH_RES = (
    re.compile(r"^/collections/[^/?#]+/?$", re.I),         # Shopify
    re.compile(r"^/product-category/", re.I),              # WooCommerce
    re.compile(r"^/catalog/category/", re.I),              # Magento
    re.compile(r"^/categories?/[^/?#]+", re.I),            # BigCommerce, generic
    re.compile(r"^/collection[s]?/[^/?#]+", re.I),
    re.compile(r"^/shop/category/", re.I),
    re.compile(r"^/store/category/", re.I),
    re.compile(r"^/c/[^/?#]+", re.I),
)

PAGE_PATH_RES = (
    re.compile(r"^/pages/[^/?#]+/?$", re.I),               # Shopify
    re.compile(r"^/cms/page/", re.I),                      # Magento CMS
    re.compile(r"^/page/[^/?#]+", re.I),
    re.compile(r"^/content/[^/?#]+", re.I),
)

PRODUCT_QUERY_RES = (
    re.compile(r"(?:^|&)route=product/product\b", re.I),    # OpenCart
    re.compile(r"(?:^|&)route=checkout/product\b", re.I),
)

COLLECTION_QUERY_RES = (
    re.compile(r"(?:^|&)route=product/category\b", re.I),   # OpenCart
)

PRODUCT_SITEMAP_RE = re.compile(
    r"sitemap_products|products[-_]sitemap|product[-_]sitemap(?!.*categor)"
    r"|product[-_]urls|catalog[-_]product|item[-_]sitemap|store[-_]products"
    r"|com_product|sm[-_]products?",
    re.I,
)
COLLECTION_SITEMAP_RE = re.compile(
    r"sitemap_collections|collections[-_]sitemap|collection[-_]sitemap"
    r"|product_cat[-_]sitemap|product[-_]category|catalog[-_]category"
    r"|categories[-_]sitemap|category[-_]sitemap(?!.*post)"
    r"|store[-_]categor",
    re.I,
)
PAGE_SITEMAP_RE = re.compile(
    r"sitemap_pages|pages[-_]sitemap|page[-_]sitemap|cms[-_]page"
    r"|cms[-_]sitemap|static[-_]pages?",
    re.I,
)

CRAWL_PREFIXES = (
    "/collections/", "/pages/", "/products/", "/blogs/",
    "/product/", "/product-category/", "/shop/", "/store/",
    "/catalog/", "/category/", "/categories/", "/collection/",
    "/cms/", "/p/", "/item/", "/buy/",
)

SITEMAP_SEED_PATHS = (
    "sitemap.xml",
    "sitemap_index.xml",
    "sitemap-index.xml",
    "wp-sitemap.xml",
    "sitemap/sitemap.xml",
    "sitemap/sitemap-index.xml",
    "sitemaps/sitemap.xml",
)

CONTENT_SELECTORS = (
    "main",
    '[role="main"]',
    "#MainContent",
    ".shopify-section .rte",
    ".rte",
    ".page-content",
    ".entry-content",
    "article.page",
    "article",
    "#content",
    ".content-area",
    ".page-width",
    ".product-info-main",
    ".productView",
    "#product-description",
    ".woocommerce-product-details",
    ".product-single__description",
    ".collection-description",
    ".category-description",
    "#maincontent",
    ".main-content",
)

STRIP_TAGS = frozenset({
    "script", "style", "noscript", "iframe", "svg", "nav", "header", "footer",
    "form", "button", "input", "select", "textarea", "meta", "link", "aside",
})

KEEP_TAGS = frozenset({
    "h1", "h2", "h3", "h4", "h5", "h6", "p", "ul", "ol", "li", "a", "img",
    "table", "thead", "tbody", "tr", "th", "td", "blockquote", "strong", "b",
    "em", "i", "br", "div", "span", "figure", "figcaption", "pre", "code",
    "hr", "dl", "dt", "dd", "section", "article",
})

ALLOWED_ATTRS: dict[str, frozenset[str]] = {
    "a": frozenset({"href", "title"}),
    "img": frozenset({"src", "alt", "title"}),
}

def _default_data_dir() -> Path:
    if os.environ.get("VERCEL") or os.environ.get("DATA_DIR"):
        base = Path(os.environ.get("DATA_DIR", "/tmp/sitecrawler/data"))
        base.mkdir(parents=True, exist_ok=True)
        return base
    backend_dir = Path(__file__).resolve().parent
    return backend_dir.parent / "data"


DATA_DIR = _default_data_dir()
SITES_FILE = DATA_DIR / "sites.json"
OUTPUT_DIR = DATA_DIR / "output"
STATUS_DIR = DATA_DIR / "status"
DEFAULT_FETCH_WORKERS = 20
_status_write_lock = threading.Lock()
_status_memory: dict[str, dict] = {}


@dataclass
class CrawlResult:
    products: set[str] = field(default_factory=set)
    collections: set[str] = field(default_factory=set)
    pages: set[str] = field(default_factory=set)

    def merge(self, other: CrawlResult) -> None:
        self.products.update(other.products)
        self.collections.update(other.collections)
        self.pages.update(other.pages)

    def to_dict(self) -> dict[str, list[str]]:
        return {
            "products": sorted(self.products),
            "collections": sorted(self.collections),
            "pages": sorted(self.pages),
        }

    @property
    def total(self) -> int:
        return len(self.products) + len(self.collections) + len(self.pages)


def normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    path = parsed.path.rstrip("/") or "/"
    query = parsed.query
    # OpenCart / legacy PHP stores use query strings for product & category routes
    if query and (
        path.endswith(".php")
        or "route=" in query.lower()
        or "product_id=" in query.lower()
        or "category_id=" in query.lower()
    ):
        return urlunparse((parsed.scheme, parsed.netloc.lower(), path, "", query, ""))
    return urlunparse((parsed.scheme, parsed.netloc.lower(), path, "", "", ""))


def canonical_netloc(netloc: str) -> str:
    netloc = netloc.lower()
    if netloc.startswith("www."):
        return netloc[4:]
    return netloc


def resolve_canonical_url(url: str, *, timeout: int = 15) -> str:
    value = url.strip()
    if not value.startswith(("http://", "https://")):
        value = "https://" + value
    headers = {"User-Agent": "SiteCrawler/1.0"}
    for method in ("head", "get"):
        try:
            request = requests.head if method == "head" else requests.get
            response = request(
                value,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
            )
            if response.url:
                return normalize_url(response.url)
        except requests.RequestException:
            continue
    return normalize_url(value)


def site_key(url: str) -> str:
    value = url.strip()
    if not value.startswith(("http://", "https://")):
        value = "https://" + value
    return canonical_netloc(urlparse(normalize_url(value)).netloc)


def same_site(url: str, base_netloc: str) -> bool:
    return canonical_netloc(urlparse(url).netloc) == canonical_netloc(base_netloc)


def _matches_any(patterns: tuple[re.Pattern[str], ...], value: str) -> bool:
    return any(pattern.search(value) for pattern in patterns)


def classify_url(url: str) -> str | None:
    parsed = urlparse(url)
    path = parsed.path or "/"
    if path in ("/", ""):
        return None

    query = parsed.query

    if _matches_any(COLLECTION_PATH_RES, path) or _matches_any(COLLECTION_QUERY_RES, query):
        return "collections"

    if _matches_any(PAGE_PATH_RES, path):
        return "pages"

    if _matches_any(PRODUCT_PATH_RES, path) or _matches_any(PRODUCT_QUERY_RES, query):
        return "products"

    # Magento / legacy stores: category-slug/product-slug.html
    if path.endswith(".html") and not re.search(r"/(category|collection|catalog)/", path, re.I):
        return "products"

    return None


def classify_sitemap_kind(sitemap_url: str) -> str | None:
    name = urlparse(sitemap_url).path.rsplit("/", 1)[-1]
    if PRODUCT_SITEMAP_RE.search(name):
        return "products"
    if COLLECTION_SITEMAP_RE.search(name):
        return "collections"
    if PAGE_SITEMAP_RE.search(name):
        return "pages"
    return None


def should_fetch_sitemap(sitemap_url: str) -> bool:
    name = urlparse(sitemap_url).path.rsplit("/", 1)[-1].lower()
    skip_parts = (
        "blog", "agentic", "metaobject", "article", "author", "attachment",
        "post_tag", "post-", "tag-", "brand", "vendor", "news", "portfolio",
        "testimonial", "recipe",
    )
    if any(part in name for part in skip_parts):
        return False
    # WordPress blog category sitemap (not product categories)
    if name == "category-sitemap.xml":
        return False
    return True


def resolve_crawl_mode(args: argparse.Namespace) -> tuple[bool, bool]:
    if getattr(args, "crawl_only", False):
        return False, True
    if getattr(args, "crawl_links", False):
        return True, True
    return True, False


def add_classified(result: CrawlResult, url: str, *, kind: str | None = None) -> None:
    resolved = kind or classify_url(url)
    if resolved == "products":
        result.products.add(url)
    elif resolved == "collections":
        result.collections.add(url)
    elif resolved == "pages":
        result.pages.add(url)


def sitemap_root_tag(content: bytes) -> str | None:
    for _event, elem in ET.iterparse(io.BytesIO(content), events=("start",)):
        tag = elem.tag.rsplit("}", 1)[-1]
        elem.clear()
        return tag
    return None


def iter_sitemap_locs(content: bytes) -> Iterable[str]:
    for _event, elem in ET.iterparse(io.BytesIO(content), events=("end",)):
        if elem.tag.rsplit("}", 1)[-1] == "loc" and elem.text:
            yield elem.text.strip()
        elem.clear()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


PHASE_LABELS = {
    "resolve": "Connecting to store",
    "discover": "Discovering URLs from sitemap",
    "save": "Saving sitemap files",
    "metadata": "Fetching titles & metadata",
    "finalize": "Finishing up",
    "done": "Complete",
}


def _parse_iso(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def compute_crawl_progress(status: dict) -> dict:
    """Derive progress %, phase label, and ETA from structured crawl status fields."""
    out = dict(status)
    fetch_pages = bool(out.get("fetch_pages"))
    phase = out.get("phase") or "discover"
    explicit = int(out.get("progress") or 0)
    computed = explicit

    if phase == "resolve":
        computed = max(computed, min(8, explicit or 3))

    elif phase == "discover":
        span = 22 if fetch_pages else 77
        base = 8
        s_done = int(out.get("sitemaps_done") or 0)
        s_total = max(int(out.get("sitemaps_total") or 0), 1)
        by_sitemap = base + int(span * 0.45 * s_done / s_total)
        entries = int(out.get("discover_entries") or 0)
        entry_cap = max(entries, 1)
        by_entries = base + int(span * 0.55 * min(1.0, entries / (entry_cap + 1500)))
        computed = max(computed, by_sitemap, by_entries, base)

    elif phase == "save":
        computed = max(computed, 30 if fetch_pages else 92)

    elif phase == "metadata":
        done = int(out.get("progress_done") or 0)
        total = max(int(out.get("progress_total") or 0), 1)
        computed = max(computed, 35 + int(63 * done / total))

    elif phase in ("finalize", "done"):
        computed = 100

    computed = min(99 if out.get("state") == "running" else 100, max(0, computed))
    out["progress"] = computed
    out["display_progress"] = computed
    out["phase_label"] = PHASE_LABELS.get(phase, phase.replace("_", " ").title())

    counts = out.get("counts")
    if isinstance(counts, dict):
        out["counts"] = {
            "products": int(counts.get("products") or 0),
            "collections": int(counts.get("collections") or 0),
            "pages": int(counts.get("pages") or 0),
            "total": int(counts.get("total") or 0),
        }
    else:
        out["counts"] = {"products": 0, "collections": 0, "pages": 0, "total": 0}

    if not out["counts"]["total"]:
        out["counts"]["total"] = (
            out["counts"]["products"]
            + out["counts"]["collections"]
            + out["counts"]["pages"]
        )

    started = _parse_iso(out.get("started_at", ""))
    done = int(out.get("progress_done") or 0)
    total = int(out.get("progress_total") or 0)
    if (
        phase == "metadata"
        and started
        and done >= 5
        and total > done
    ):
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        if elapsed > 0:
            rate = done / elapsed
            out["eta_seconds"] = max(1, int((total - done) / rate))
            out["pages_per_second"] = round(rate, 2)

    return out


def write_crawl_status(key: str, **fields) -> None:
    with _status_write_lock:
        STATUS_DIR.mkdir(parents=True, exist_ok=True)
        path = STATUS_DIR / f"{key}.json"
        current: dict = dict(_status_memory.get(key) or {})
        if not current and path.exists():
            with path.open(encoding="utf-8") as fh:
                current = json.load(fh)
        current.update(fields)
        if current.get("state") == "running" and not current.get("started_at"):
            current["started_at"] = utc_now()
        current["updated_at"] = utc_now()
        current["revision"] = int(current.get("revision") or 0) + 1
        current = compute_crawl_progress(current)
        _status_memory[key] = current
        tmp = path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(current, fh, indent=2)
            fh.write("\n")
            fh.flush()
        tmp.replace(path)
        if os.environ.get("VERCEL"):
            rel = str(path.relative_to(DATA_DIR))
            persist_file(DATA_DIR, rel, path.read_bytes())


def read_crawl_status(key: str, *, enrich: bool = False) -> dict | None:
    with _status_write_lock:
        if key in _status_memory:
            data = dict(_status_memory[key])
        else:
            path = STATUS_DIR / f"{key}.json"
            if not path.exists():
                return None
            with path.open(encoding="utf-8") as fh:
                data = json.load(fh)
            _status_memory[key] = dict(data)
    return compute_crawl_progress(data) if enrich else data


def _meta_content(soup: BeautifulSoup, *, name: str | None = None, prop: str | None = None) -> str:
    attrs: dict[str, str] = {}
    if name:
        attrs["name"] = name
    if prop:
        attrs["property"] = prop
    tag = soup.find("meta", attrs=attrs)
    if tag and tag.get("content"):
        return tag["content"].strip()
    return ""


def _first_match(soup: BeautifulSoup, selector: str):
    try:
        return soup.select_one(selector)
    except Exception:
        return None


def find_main_content(soup: BeautifulSoup):
    for selector in CONTENT_SELECTORS:
        node = _first_match(soup, selector)
        if node and node.get_text(strip=True):
            return node
    body = soup.body
    if body:
        for tag in body.find_all(STRIP_TAGS):
            tag.decompose()
        return body
    return soup


def clean_content_html(root) -> str:
    for tag in root.find_all(list(STRIP_TAGS)):
        tag.decompose()

    for tag in root.find_all(True):
        if tag.name in STRIP_TAGS:
            continue
        if tag.name not in KEEP_TAGS:
            tag.unwrap()
            continue
        allowed = ALLOWED_ATTRS.get(tag.name, frozenset())
        tag.attrs = {key: value for key, value in tag.attrs.items() if key in allowed}

    html = root.decode_contents().strip()
    html = re.sub(r"\n{3,}", "\n\n", html)
    html = re.sub(r"[ \t]+\n", "\n", html)
    return html


def extract_meta_details(html: str, url: str, *, include_content: bool = False) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")

    title = ""
    for selector in (
        "h1.product__title",
        "h1.product-title",
        ".product-single__title",
        "h1.page-title",
        ".productView-title",
        ".woocommerce-product-details__title",
        "h1.collection-hero__title",
        ".collection__title",
        ".page-header h1",
        "h1",
    ):
        node = _first_match(soup, selector)
        if node and node.get_text(strip=True):
            title = node.get_text(strip=True)
            break

    title_tag = soup.find("title")
    if not title and title_tag:
        title = title_tag.get_text(strip=True)

    meta_title = (
        _meta_content(soup, prop="og:title")
        or _meta_content(soup, name="title")
        or _meta_content(soup, name="twitter:title")
        or title
    )
    meta_description = (
        _meta_content(soup, name="description")
        or _meta_content(soup, prop="og:description")
        or _meta_content(soup, name="twitter:description")
    )

    entry: dict[str, str] = {
        "url": url,
        "title": title,
        "meta_title": meta_title,
        "meta_description": meta_description,
    }

    if include_content:
        content_root = find_main_content(soup)
        content_html = clean_content_html(content_root)
        entry["content_html"] = content_html

    return entry


def enrich_url_details(
    crawler: SiteCrawler,
    urls: Iterable[str],
    site_dir: Path,
    output_filename: str,
    json_key: str,
    on_progress: Callable[..., None] | None = None,
    *,
    include_content: bool = False,
    content_subdir: str | None = None,
    workers: int = DEFAULT_FETCH_WORKERS,
    on_item_done: Callable[[int, int], None] | None = None,
) -> str:
    url_list = sorted(urls)
    total = len(url_list)
    output_path = site_dir / output_filename

    if total == 0:
        with output_path.open("w", encoding="utf-8") as fh:
            json.dump({json_key: []}, fh, indent=2)
            fh.write("\n")
        return str(output_path)

    content_dir: Path | None = None
    if include_content and content_subdir:
        content_dir = site_dir / content_subdir
        content_dir.mkdir(parents=True, exist_ok=True)

    def fetch_one(page_url: str) -> dict[str, str]:
        entry: dict[str, str] = {
            "url": page_url,
            "title": "",
            "meta_title": "",
            "meta_description": "",
        }
        if include_content:
            entry["content_html"] = ""

        try:
            response = requests.get(
                page_url,
                headers={"User-Agent": crawler.user_agent},
                timeout=crawler.timeout,
                allow_redirects=True,
            )
            if not response.ok:
                entry["error"] = f"HTTP {response.status_code}"
                return entry
        except requests.RequestException as exc:
            entry["error"] = str(exc)
            return entry

        content_type = response.headers.get("Content-Type", "")
        if "text/html" not in content_type:
            entry["error"] = "Not an HTML page"
            return entry

        try:
            entry = extract_meta_details(
                response.text, page_url, include_content=include_content
            )
        except Exception as exc:
            entry["error"] = str(exc)
            return entry

        if include_content and content_dir is not None:
            slug = urlparse(page_url).path.strip("/").replace("/", "_") or "home"
            slug = re.sub(r"[^\w\-]", "_", slug)[:80]
            html_path = content_dir / f"{slug}.html"
            html_path.write_text(entry.get("content_html", ""), encoding="utf-8")
            entry["content_file"] = str(html_path.relative_to(site_dir))

        return entry

    details: list[dict[str, str]] = []
    completed = 0
    with ThreadPoolExecutor(max_workers=min(workers, total)) as pool:
        futures = {pool.submit(fetch_one, page_url): page_url for page_url in url_list}
        for future in as_completed(futures):
            completed += 1
            if on_item_done:
                on_item_done(completed, total)
            if on_progress:
                page_url = futures[future]
                path = urlparse(page_url).path or "/"
                on_progress(f"{json_key}: {completed}/{total} {path}")
            details.append(future.result())

    details.sort(key=lambda item: item["url"])
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump({json_key: details}, fh, indent=2)
        fh.write("\n")
    return str(output_path)


def enrich_page_details(
    crawler: SiteCrawler,
    page_urls: Iterable[str],
    site_dir: Path,
    on_progress: Callable[..., None] | None = None,
    *,
    workers: int = DEFAULT_FETCH_WORKERS,
    on_item_done: Callable[[int, int], None] | None = None,
) -> str:
    return enrich_url_details(
        crawler,
        page_urls,
        site_dir,
        "pages_detail.json",
        "pages",
        on_progress,
        include_content=True,
        content_subdir="pages_content",
        workers=workers,
        on_item_done=on_item_done,
    )


def load_sites() -> dict[str, dict]:
    return load_json(DATA_DIR, "sites.json", {})


def save_sites(sites: dict[str, dict]) -> None:
    save_json(DATA_DIR, "sites.json", sites)


class SiteCrawler:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: int = 15,
        max_pages: int = 500,
        respect_robots: bool = True,
        user_agent: str = "SiteCrawler/1.0",
        skip_resolve: bool = False,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            base_url = "https://" + base_url

        if skip_resolve:
            base_url = normalize_url(base_url)
        else:
            base_url = resolve_canonical_url(base_url, timeout=timeout)
        parsed = urlparse(base_url)
        self.base_url = urlunparse((parsed.scheme, parsed.netloc.lower(), "", "", "", ""))
        self.base_netloc = parsed.netloc.lower()
        self.timeout = timeout
        self.max_pages = max_pages
        self.respect_robots = respect_robots
        self.user_agent = user_agent
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        self._robots: RobotFileParser | None = None
        self.on_progress: Callable[..., None] | None = None

    def _progress(self, message: str, **extra) -> None:
        if self.on_progress:
            self.on_progress(message, **extra)

    def _load_robots(self) -> RobotFileParser:
        if self._robots is not None:
            return self._robots

        rp = RobotFileParser()
        robots_url = urljoin(self.base_url + "/", "robots.txt")
        try:
            response = self.session.get(robots_url, timeout=self.timeout)
            if response.ok:
                rp.parse(response.text.splitlines())
            else:
                rp.parse([])
        except requests.RequestException:
            rp.parse([])

        self._robots = rp
        return rp

    def can_fetch(self, url: str) -> bool:
        if not self.respect_robots:
            return True
        return self._load_robots().can_fetch(self.user_agent, url)

    def fetch(self, url: str) -> requests.Response | None:
        if not self.can_fetch(url):
            return None
        try:
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            if response.ok:
                return response
        except requests.RequestException:
            pass
        return None

    def discover_sitemap_seeds(self) -> list[str]:
        seeds = [urljoin(self.base_url + "/", path) for path in SITEMAP_SEED_PATHS]
        robots_url = urljoin(self.base_url + "/", "robots.txt")
        try:
            response = self.session.get(robots_url, timeout=self.timeout)
            if response.ok:
                for line in response.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        seeds.append(line.split(":", 1)[1].strip())
        except requests.RequestException:
            pass
        return list(dict.fromkeys(seeds))

    def discover_from_sitemap(self) -> CrawlResult:
        result = CrawlResult()
        visited_sitemaps: set[str] = set()
        queue: deque[str] = deque(self.discover_sitemap_seeds())
        discover_entries = 0
        last_status_at = time.monotonic()

        def discover_status(message: str, **extra) -> None:
            nonlocal last_status_at
            last_status_at = time.monotonic()
            extra.setdefault("phase", "discover")
            extra["counts"] = {
                "products": len(result.products),
                "collections": len(result.collections),
                "pages": len(result.pages),
                "total": result.total,
            }
            extra["sitemaps_done"] = len(visited_sitemaps)
            extra["sitemaps_total"] = len(visited_sitemaps) + len(queue)
            extra["discover_entries"] = discover_entries
            self._progress(message, **extra)

        def maybe_status(message: str, *, force: bool = False) -> None:
            now = time.monotonic()
            if force or now - last_status_at >= 0.4:
                discover_status(message)

        discover_status("Reading sitemap.xml…", progress=8)

        while queue:
            current = queue.popleft()
            if current in visited_sitemaps:
                continue
            visited_sitemaps.add(current)

            name = urlparse(current).path.rsplit("/", 1)[-1] or "sitemap"
            discover_status(f"Reading {name}…")

            response = self.fetch(current)
            if response is None:
                continue

            content = response.content
            try:
                tag = sitemap_root_tag(content)
            except ET.ParseError:
                continue
            if not tag:
                continue

            if tag == "sitemapindex":
                added = 0
                for loc_text in iter_sitemap_locs(content):
                    if should_fetch_sitemap(loc_text):
                        queue.append(loc_text)
                        added += 1
                discover_status(
                    f"Indexed {name} — {added:,} sitemaps queued "
                    f"({len(result.products):,} products, "
                    f"{len(result.collections):,} collections, "
                    f"{len(result.pages):,} pages so far)",
                )
                continue

            if tag == "urlset":
                sitemap_kind = classify_sitemap_kind(current)
                found = 0
                processed = 0
                for loc_text in iter_sitemap_locs(content):
                    processed += 1
                    discover_entries += 1
                    url = normalize_url(loc_text)
                    if same_site(url, self.base_netloc):
                        kind = sitemap_kind or classify_url(url)
                        if kind is not None:
                            before = result.total
                            add_classified(result, url, kind=kind)
                            if result.total > before:
                                found += 1
                    if processed % 25 == 0:
                        maybe_status(
                            f"{name}: {processed:,} entries — "
                            f"{len(result.products):,} products, "
                            f"{len(result.collections):,} collections, "
                            f"{len(result.pages):,} pages",
                        )
                discover_status(
                    f"{name}: +{found:,} URLs — "
                    f"{len(result.products):,} products, "
                    f"{len(result.collections):,} collections, "
                    f"{len(result.pages):,} pages",
                )

        return result

    def discover_from_crawl(self) -> CrawlResult:
        result = CrawlResult()
        visited: set[str] = set()
        queue: deque[str] = deque([self.base_url + "/"])
        self._progress(f"Crawling HTML links (up to {self.max_pages} pages, slower)...")

        while queue and len(visited) < self.max_pages:
            current = normalize_url(queue.popleft())
            if current in visited:
                continue
            visited.add(current)

            if len(visited) == 1 or len(visited) % 25 == 0:
                self._progress(f"  pages visited: {len(visited)}/{self.max_pages}")

            if not same_site(current, self.base_netloc):
                continue

            add_classified(result, current)

            response = self.fetch(current)
            if response is None or "text/html" not in response.headers.get("Content-Type", ""):
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"].strip()
                if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
                    continue

                absolute = normalize_url(urljoin(current + "/", href))
                if not same_site(absolute, self.base_netloc):
                    continue

                add_classified(result, absolute)

                path = urlparse(absolute).path
                query = urlparse(absolute).query
                if path in ("/", "") or any(path.startswith(prefix) for prefix in CRAWL_PREFIXES):
                    queue.append(absolute)
                elif query and ("route=" in query.lower() or path.endswith(".php")):
                    queue.append(absolute)

        self._progress(f"  finished: {len(visited)} pages visited")
        return result

    def run(self, *, use_sitemap: bool = True, use_crawl: bool = False) -> CrawlResult:
        result = CrawlResult()

        if use_sitemap:
            result.merge(self.discover_from_sitemap())

        if use_crawl:
            result.merge(self.discover_from_crawl())

        return result


def write_urlset(path: Path, urls: Iterable[str], lastmod: str) -> None:
    urlset = ET.Element("urlset", xmlns=SITEMAP_NS)
    for url in sorted(urls):
        entry = ET.SubElement(urlset, "url")
        ET.SubElement(entry, "loc").text = url
        ET.SubElement(entry, "lastmod").text = lastmod

    tree = ET.ElementTree(urlset)
    ET.indent(tree, space="  ")
    tree.write(path, encoding="utf-8", xml_declaration=True)


def write_sitemap_index(path: Path, entries: list[tuple[str, str]], lastmod: str) -> None:
    index = ET.Element("sitemapindex", xmlns=SITEMAP_NS)
    for loc, _label in entries:
        sitemap = ET.SubElement(index, "sitemap")
        ET.SubElement(sitemap, "loc").text = loc
        ET.SubElement(sitemap, "lastmod").text = lastmod

    tree = ET.ElementTree(index)
    ET.indent(tree, space="  ")
    tree.write(path, encoding="utf-8", xml_declaration=True)


def generate_sitemaps(
    result: CrawlResult,
    site_dir: Path,
    *,
    crawler: SiteCrawler | None = None,
    on_progress: Callable[..., None] | None = None,
    fetch_pages: bool = False,
    fetch_workers: int = DEFAULT_FETCH_WORKERS,
) -> dict[str, str]:
    site_dir.mkdir(parents=True, exist_ok=True)
    lastmod = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    files: dict[str, str] = {}
    sections = [
        ("products", result.products, "sitemap_products_1.xml"),
        ("collections", result.collections, "sitemap_collections_1.xml"),
        ("pages", result.pages, "sitemap_pages_1.xml"),
    ]

    index_entries: list[tuple[str, str]] = []
    for label, urls, filename in sections:
        if not urls:
            continue
        filepath = site_dir / filename
        write_urlset(filepath, urls, lastmod)
        files[label] = str(filepath)
        index_entries.append((filename, label))

    if index_entries:
        index_path = site_dir / "sitemap.xml"
        write_sitemap_index(index_path, index_entries, lastmod)
        files["index"] = str(index_path)

    urls_json = site_dir / "urls.json"
    with urls_json.open("w", encoding="utf-8") as fh:
        json.dump(result.to_dict(), fh, indent=2)
        fh.write("\n")
    files["urls_json"] = str(urls_json)

    if fetch_pages and crawler is not None:
        meta_total = len(result.products) + len(result.collections) + len(result.pages)
        meta_state = {"done": 0}

        if on_progress and meta_total:
            on_progress(
                f"Fetching titles & metadata (0/{meta_total:,})…",
                phase="metadata",
                progress=35,
                progress_done=0,
                progress_total=meta_total,
                counts={
                    "products": len(result.products),
                    "collections": len(result.collections),
                    "pages": len(result.pages),
                    "total": meta_total,
                },
            )

        def meta_item_done(_completed: int, _batch_total: int) -> None:
            meta_state["done"] += 1
            if not meta_total or not on_progress:
                return
            done = meta_state["done"]
            if done % 3 != 0 and done != meta_total:
                return
            on_progress(
                f"Fetching titles & metadata ({done:,}/{meta_total:,})…",
                phase="metadata",
                progress_done=done,
                progress_total=meta_total,
                counts={
                    "products": len(result.products),
                    "collections": len(result.collections),
                    "pages": len(result.pages),
                    "total": meta_total,
                },
            )

        if result.products:
            if on_progress:
                on_progress(f"Fetching metadata for {len(result.products)} products...", progress=28)
            files["products_detail"] = enrich_url_details(
                crawler,
                result.products,
                site_dir,
                "products_detail.json",
                "products",
                on_progress,
                workers=fetch_workers,
                on_item_done=meta_item_done,
            )
        if result.collections:
            if on_progress:
                on_progress(
                    f"Fetching metadata for {len(result.collections)} collections...",
                    progress=28 + int(67 * meta_state["done"] / meta_total) if meta_total else 28,
                )
            files["collections_detail"] = enrich_url_details(
                crawler,
                result.collections,
                site_dir,
                "collections_detail.json",
                "collections",
                on_progress,
                workers=fetch_workers,
                on_item_done=meta_item_done,
            )
        if result.pages:
            if on_progress:
                on_progress(
                    f"Fetching metadata for {len(result.pages)} pages...",
                    progress=28 + int(67 * meta_state["done"] / meta_total) if meta_total else 28,
                )
            files["pages_detail"] = enrich_page_details(
                crawler,
                result.pages,
                site_dir,
                on_progress=on_progress,
                workers=fetch_workers,
                on_item_done=meta_item_done,
            )

    return files


def register_site(url: str, *, force: bool = False) -> str:
    url = resolve_canonical_url(url)
    key = site_key(url)
    sites = load_sites()
    if key in sites and not force:
        return key
    sites[key] = {
        "url": url,
        "added_at": sites.get(key, {}).get("added_at", utc_now()),
        "last_crawled_at": sites.get(key, {}).get("last_crawled_at"),
        "last_counts": sites.get(key, {}).get("last_counts"),
    }
    save_sites(sites)
    return key


def crawl_and_save(
    key: str,
    *,
    crawl_links: bool = False,
    fetch_pages: bool = False,
    max_pages: int = 500,
    timeout: int = 15,
    respect_robots: bool = True,
    fetch_workers: int = DEFAULT_FETCH_WORKERS,
    on_progress: Callable[..., None] | None = None,
    status_key: str | None = None,
) -> tuple[CrawlResult, dict[str, str], dict[str, int]]:
    sites = load_sites()
    if key not in sites:
        raise KeyError(key)

    status_id = status_key or key

    def report(message: str, **extra) -> None:
        if status_key is not None:
            write_crawl_status(status_id, message=message, state="running", **extra)
        if on_progress:
            on_progress(message, **extra)

    report("Discovering URLs from sitemap…", phase="discover", progress=5)

    url = sites[key]["url"]
    report("Resolving store URL…", phase="resolve", progress=3)
    resolved_url = resolve_canonical_url(url, timeout=timeout)
    report("Store URL ready.", phase="resolve", progress=8)

    crawler = SiteCrawler(
        resolved_url,
        timeout=timeout,
        max_pages=max_pages,
        respect_robots=respect_robots,
        skip_resolve=True,
    )
    report("Reading robots.txt and sitemap index…", phase="discover", progress=8)
    crawler.on_progress = report

    result = crawler.run(use_sitemap=True, use_crawl=crawl_links)

    counts = {
        "products": len(result.products),
        "collections": len(result.collections),
        "pages": len(result.pages),
        "total": result.total,
    }
    report(
        f"Found {counts['total']:,} URLs — "
        f"{counts['products']:,} products, {counts['collections']:,} collections, "
        f"{counts['pages']:,} pages. Saving sitemaps…",
        phase="save",
        counts=counts,
        progress=30 if fetch_pages else 92,
    )

    site_dir = OUTPUT_DIR / key
    files = generate_sitemaps(
        result,
        site_dir,
        crawler=crawler,
        on_progress=report,
        fetch_pages=fetch_pages,
        fetch_workers=fetch_workers,
    )
    if not fetch_pages:
        report(
            f"Sitemaps saved — {counts['products']:,} products, "
            f"{counts['collections']:,} collections, {counts['pages']:,} pages",
            phase="finalize",
            counts=counts,
            progress=100,
        )
    info = sites[key]
    info["last_crawled_at"] = utc_now()
    info["last_counts"] = counts
    info["output_dir"] = str(site_dir)
    sites[key] = info
    save_sites(sites)
    if os.environ.get("VERCEL"):
        sync_directory(DATA_DIR, f"output/{key}")
    return result, files, counts


def cmd_add(args: argparse.Namespace) -> int:
    url = resolve_canonical_url(args.url)
    key = site_key(url)
    sites = load_sites()

    if key in sites and not args.force:
        if args.crawl:
            print(f"Site already registered: {key} — re-crawling...", file=sys.stderr)
            args.site = key
            return cmd_crawl(args)
        print(f"Site already registered: {key}", file=sys.stderr)
        print(f"Re-crawl with: ./crawl crawl {key}", file=sys.stderr)
        print(f"Or add again with: ./crawl add {url} --crawl", file=sys.stderr)
        return 1

    sites[key] = {
        "url": url,
        "added_at": sites.get(key, {}).get("added_at", utc_now()),
        "last_crawled_at": sites.get(key, {}).get("last_crawled_at"),
        "last_counts": sites.get(key, {}).get("last_counts"),
    }
    save_sites(sites)

    print(f"Added site: {url}")
    print(f"Run crawl with: ./crawl crawl {key}")
    if args.crawl:
        args.site = key
        return cmd_crawl(args)
    return 0


def cmd_list(_args: argparse.Namespace) -> int:
    sites = load_sites()
    if not sites:
        print("No sites registered. Add one with: python crawl_site.py add <url>")
        return 0

    for key, info in sorted(sites.items()):
        counts = info.get("last_counts") or {}
        summary = (
            f"{counts.get('products', 0)} products, "
            f"{counts.get('collections', 0)} collections, "
            f"{counts.get('pages', 0)} pages"
        )
        last = info.get("last_crawled_at") or "never"
        print(f"{key}")
        print(f"  url:          {info['url']}")
        print(f"  last crawled: {last}")
        print(f"  urls found:   {summary}")
    return 0


def cmd_remove(args: argparse.Namespace) -> int:
    key = site_key(args.site)
    sites = load_sites()
    if key not in sites:
        print(f"Site not found: {key}", file=sys.stderr)
        return 1

    del sites[key]
    save_sites(sites)
    print(f"Removed site: {key}")
    return 0


def cmd_crawl(args: argparse.Namespace) -> int:
    sites = load_sites()
    if args.all:
        targets = sorted(sites.keys())
        if not targets:
            print("No sites to crawl. Add one with: python crawl_site.py add <url>", file=sys.stderr)
            return 1
    else:
        key = site_key(args.site)
        if key not in sites:
            url = resolve_canonical_url(args.site) if "://" in args.site else resolve_canonical_url(f"https://{key}/")
            sites[key] = {
                "url": url,
                "added_at": utc_now(),
                "last_crawled_at": None,
                "last_counts": None,
            }
            save_sites(sites)
            print(f"Auto-registered: {url}", file=sys.stderr)
        targets = [key]

    use_sitemap, use_crawl = resolve_crawl_mode(args)
    exit_code = 0

    for key in targets:
        info = sites[key]
        url = info["url"]
        mode = "sitemap + link crawl" if use_sitemap and use_crawl else (
            "link crawl only" if use_crawl else "sitemap only (fast)"
        )
        print(f"\nCrawling {url} [{mode}]...", file=sys.stderr)

        crawler = SiteCrawler(
            url,
            timeout=args.timeout,
            max_pages=args.max_pages,
            respect_robots=not args.no_robots,
        )
        crawler.on_progress = lambda message: print(message, file=sys.stderr)

        try:
            result = crawler.run(use_sitemap=use_sitemap, use_crawl=use_crawl)
        except KeyboardInterrupt:
            print("\nInterrupted.", file=sys.stderr)
            return 130

        site_dir = OUTPUT_DIR / key
        progress = lambda message: print(message, file=sys.stderr)
        files = generate_sitemaps(
            result,
            site_dir,
            crawler=crawler,
            on_progress=progress,
            fetch_pages=getattr(args, "fetch_pages", False),
        )

        counts = {
            "products": len(result.products),
            "collections": len(result.collections),
            "pages": len(result.pages),
            "total": result.total,
        }
        info["last_crawled_at"] = utc_now()
        info["last_counts"] = counts
        info["output_dir"] = str(site_dir)
        sites[key] = info
        save_sites(sites)

        print(
            f"Done: {counts['total']} URLs "
            f"({counts['products']} products, "
            f"{counts['collections']} collections, "
            f"{counts['pages']} pages)",
            file=sys.stderr,
        )
        print(f"Output: {site_dir}", file=sys.stderr)
        for label, path in files.items():
            print(f"  {label}: {path}", file=sys.stderr)

        if args.format == "json":
            print(json.dumps({"site": key, "url": url, "counts": counts, "files": files}, indent=2))
        elif len(targets) == 1:
            for section, urls in result.to_dict().items():
                print(f"\n=== {section.upper()} ({len(urls)}) ===")
                for item in urls:
                    print(item)

        if result.total == 0:
            exit_code = 2

    return exit_code


def add_crawl_flags(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("crawl mode (default: fast sitemap-only)")
    group.add_argument(
        "--crawl-links",
        action="store_true",
        help="Also crawl HTML page links (slower, use if sitemap misses URLs)",
    )
    group.add_argument(
        "--sitemap-only",
        action="store_true",
        help="Only read sitemap.xml (default behaviour)",
    )
    group.add_argument(
        "--crawl-only",
        action="store_true",
        help="Only crawl HTML links, skip sitemap",
    )
    group.add_argument(
        "--fetch-pages",
        action="store_true",
        help="Fetch title, meta tags for products/collections; include page HTML content",
    )
    parser.add_argument("--max-pages", type=int, default=500)
    parser.add_argument("--no-robots", action="store_true")
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--format", choices=("text", "json"), default="text")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Add sites, crawl them, and generate sitemaps."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser("add", help="Register a site for crawling")
    add_parser.add_argument("url", help="Site URL (e.g. https://example.com)")
    add_parser.add_argument("--crawl", action="store_true", help="Crawl immediately after adding")
    add_parser.add_argument("--force", action="store_true", help="Update URL if site already exists")
    add_crawl_flags(add_parser)
    add_parser.set_defaults(func=cmd_add, all=False)

    list_parser = subparsers.add_parser("list", help="List registered sites")
    list_parser.set_defaults(func=cmd_list)

    remove_parser = subparsers.add_parser("remove", help="Remove a registered site")
    remove_parser.add_argument("site", help="Site domain or URL")
    remove_parser.set_defaults(func=cmd_remove)

    crawl_parser = subparsers.add_parser("crawl", help="Crawl a site and generate sitemaps")
    crawl_parser.add_argument("site", nargs="?", help="Site domain or URL")
    crawl_parser.add_argument("--all", action="store_true", help="Crawl every registered site")
    add_crawl_flags(crawl_parser)
    crawl_parser.set_defaults(func=cmd_crawl)

    ui_parser = subparsers.add_parser("ui", help="Start the web UI")
    ui_parser.add_argument("--host", default="127.0.0.1")
    ui_parser.add_argument("--port", type=int, default=8080, help="Port (default: 8080; avoid 5000 on macOS)")
    ui_parser.set_defaults(func=cmd_ui)

    return parser


def find_available_port(host: str, start: int, limit: int = 20) -> int | None:
    import socket

    for port in range(start, start + limit):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind((host, port))
                return port
            except OSError:
                continue
    return None


def cmd_ui(args: argparse.Namespace) -> int:
    try:
        from .app import run_server
    except ImportError:
        from app import run_server

    port = args.port
    if find_available_port(args.host, port, limit=1) is None:
        next_port = find_available_port(args.host, port + 1)
        if next_port is None:
            print(
                f"Ports {args.port}-{args.port + 20} are in use. "
                f"Stop the other process or run: ./crawl ui --port <port>",
                file=sys.stderr,
            )
            return 1
        print(
            f"Port {args.port} is in use; using {next_port} instead.",
            file=sys.stderr,
        )
        port = next_port

    print(f"Open http://{args.host}:{port} in your browser", file=sys.stderr)
    run_server(host=args.host, port=port)
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "crawl" and not args.all and not args.site:
        parser.error("crawl requires a site or --all")

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
