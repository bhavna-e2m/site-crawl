#!/usr/bin/env python3
"""Web UI for the site crawler."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from urllib.parse import unquote

from flask import Flask, abort, flash, jsonify, redirect, render_template, request, send_file, url_for

from crawl_site import (
    OUTPUT_DIR,
    crawl_and_save,
    load_sites,
    read_crawl_status,
    register_site,
    save_sites,
    write_crawl_status,
)

ROOT = Path(__file__).resolve().parent
IS_VERCEL = bool(os.environ.get("VERCEL"))

app = Flask(__name__, template_folder=str(ROOT / "templates"))
app.secret_key = os.environ.get("SECRET_KEY", "sitecrawler-dev-key-change-in-production")

DOWNLOAD_FILES = {
    "sitemap.xml",
    "sitemap_products_1.xml",
    "sitemap_collections_1.xml",
    "sitemap_pages_1.xml",
    "urls.json",
    "products_detail.json",
    "collections_detail.json",
    "pages_detail.json",
}

_active_crawls: set[str] = set()
_crawl_lock = threading.Lock()


def load_urls_data(key: str) -> dict:
    urls_data = {"products": [], "collections": [], "pages": []}
    urls_json = OUTPUT_DIR / key / "urls.json"
    if urls_json.exists():
        with urls_json.open(encoding="utf-8") as fh:
            urls_data = json.load(fh)
    return urls_data


def load_detail(key: str, kind: str) -> list[dict]:
    path = OUTPUT_DIR / key / f"{kind}_detail.json"
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    return data.get(kind, [])


def list_downloads(key: str) -> list[str]:
    output_dir = OUTPUT_DIR / key
    downloads = []
    for name in DOWNLOAD_FILES:
        if (output_dir / name).exists():
            downloads.append(name)
    return downloads


def start_background_crawl(
    key: str,
    *,
    crawl_links: bool,
    fetch_pages: bool,
) -> bool:
    with _crawl_lock:
        if key in _active_crawls:
            return False
        _active_crawls.add(key)

    write_crawl_status(
        key,
        state="running",
        message="Starting crawl...",
        crawl_links=crawl_links,
        fetch_pages=fetch_pages,
    )

    def run() -> None:
        try:
            _result, _files, counts = crawl_and_save(
                key,
                crawl_links=crawl_links,
                fetch_pages=fetch_pages,
                status_key=key,
            )
            write_crawl_status(
                key,
                state="done",
                message=f"Done — {counts['total']} URLs found.",
                counts=counts,
            )
        except Exception as exc:
            write_crawl_status(key, state="error", message=str(exc))
        finally:
            with _crawl_lock:
                _active_crawls.discard(key)

    threading.Thread(target=run, daemon=True).start()
    return True


def run_crawl_sync(
    key: str,
    *,
    crawl_links: bool,
    fetch_pages: bool,
) -> tuple[str, dict | None]:
    """Run crawl in-process (required on Vercel serverless)."""
    write_crawl_status(
        key,
        state="running",
        message="Starting crawl...",
        crawl_links=crawl_links,
        fetch_pages=fetch_pages,
    )
    try:
        _result, _files, counts = crawl_and_save(
            key,
            crawl_links=crawl_links,
            fetch_pages=fetch_pages,
            status_key=key,
        )
        write_crawl_status(
            key,
            state="done",
            message=f"Done — {counts['total']} URLs found.",
            counts=counts,
        )
        return "done", counts
    except Exception as exc:
        write_crawl_status(key, state="error", message=str(exc))
        return "error", None


def trigger_crawl(
    key: str,
    *,
    crawl_links: bool,
    fetch_pages: bool,
) -> str:
    """Start a crawl. On Vercel runs synchronously; locally uses a background thread."""
    if IS_VERCEL:
        state, _counts = run_crawl_sync(
            key,
            crawl_links=crawl_links,
            fetch_pages=fetch_pages,
        )
        return state
    if not start_background_crawl(key, crawl_links=crawl_links, fetch_pages=fetch_pages):
        return "running"
    return "started"


@app.route("/")
def index():
    sites = load_sites()
    ordered = sorted(sites.items(), key=lambda item: item[1].get("last_crawled_at") or "", reverse=True)
    return render_template("index.html", sites=ordered)


@app.route("/add", methods=["POST"])
def add_and_crawl():
    url = (request.form.get("url") or "").strip()
    if not url:
        flash("Please enter a site URL.", "error")
        return redirect(url_for("index"))

    crawl_links = request.form.get("crawl_links") == "on"
    fetch_pages = request.form.get("fetch_pages") == "on"
    key = register_site(url, force=True)

    result = trigger_crawl(key, crawl_links=crawl_links, fetch_pages=fetch_pages)
    if result == "running":
        flash(f"Crawl already running for {key}.", "warning")
        return redirect(url_for("crawl_status_page", key=key))
    if result == "error":
        flash(f"Crawl failed for {key}. See status for details.", "error")
        return redirect(url_for("crawl_status_page", key=key))
    if IS_VERCEL:
        flash(f"Crawl complete for {key}.", "success")
        return redirect(url_for("site_results", key=key))

    return redirect(url_for("crawl_status_page", key=key))


@app.route("/crawl-status/<key>")
def crawl_status_page(key: str):
    sites = load_sites()
    if key not in sites:
        flash(f"Site not found: {key}", "error")
        return redirect(url_for("index"))
    status = read_crawl_status(key) or {"state": "unknown", "message": "No status yet."}
    return render_template("crawl_status.html", key=key, info=sites[key], status=status)


@app.route("/api/crawl-status/<key>")
def crawl_status_api(key: str):
    status = read_crawl_status(key)
    if status is None:
        return jsonify({"state": "unknown", "message": "No crawl in progress."})
    return jsonify(status)


@app.route("/site/<key>")
def site_results(key: str):
    sites = load_sites()
    if key not in sites:
        flash(f"Site not found: {key}", "error")
        return redirect(url_for("index"))

    info = sites[key]
    tab = request.args.get("tab", "products")
    if tab not in ("products", "collections", "pages"):
        tab = "products"

    urls_data = load_urls_data(key)
    detail = load_detail(key, tab)
    downloads = list_downloads(key)
    status = read_crawl_status(key)

    counts = info.get("last_counts") or {
        "products": len(urls_data["products"]),
        "collections": len(urls_data["collections"]),
        "pages": len(urls_data["pages"]),
        "total": sum(len(urls_data[k]) for k in urls_data),
    }

    return render_template(
        "site.html",
        key=key,
        info=info,
        counts=counts,
        urls=urls_data,
        detail=detail,
        tab=tab,
        downloads=downloads,
        crawl_status=status,
    )


@app.route("/site/<key>/page")
def page_detail(key: str):
    sites = load_sites()
    if key not in sites:
        flash(f"Site not found: {key}", "error")
        return redirect(url_for("index"))

    page_url = unquote(request.args.get("url", ""))
    if not page_url:
        flash("Page URL is required.", "error")
        return redirect(url_for("site_results", key=key, tab="pages"))

    pages = load_detail(key, "pages")
    page = next((item for item in pages if item.get("url") == page_url), None)
    if page is None:
        flash("Page details not found. Try re-crawling with metadata fetch enabled.", "warning")
        return redirect(url_for("site_results", key=key, tab="pages"))

    return render_template(
        "page_detail.html",
        key=key,
        info=sites[key],
        page=page,
    )


@app.route("/crawl/<key>", methods=["POST"])
def recrawl(key: str):
    sites = load_sites()
    if key not in sites:
        flash(f"Site not found: {key}", "error")
        return redirect(url_for("index"))

    crawl_links = request.form.get("crawl_links") == "on"
    fetch_pages = request.form.get("fetch_pages") == "on"

    result = trigger_crawl(key, crawl_links=crawl_links, fetch_pages=fetch_pages)
    if result == "running":
        flash(f"Crawl already running for {key}.", "warning")
        return redirect(url_for("crawl_status_page", key=key))
    if result == "error":
        flash(f"Crawl failed for {key}. See status for details.", "error")
        return redirect(url_for("crawl_status_page", key=key))
    if IS_VERCEL:
        flash(f"Crawl complete for {key}.", "success")
        return redirect(url_for("site_results", key=key))

    return redirect(url_for("crawl_status_page", key=key))


@app.route("/remove/<key>", methods=["POST"])
def remove_site(key: str):
    sites = load_sites()
    if key in sites:
        del sites[key]
        save_sites(sites)
        flash(f"Removed {key}.", "success")
    return redirect(url_for("index"))


@app.route("/download/<key>/<filename>")
def download(key: str, filename: str):
    if filename not in DOWNLOAD_FILES:
        abort(404)

    path = (OUTPUT_DIR / key / filename).resolve()
    if not path.is_file() or OUTPUT_DIR.resolve() not in path.parents:
        abort(404)
    return send_file(path, as_attachment=True, download_name=filename)


@app.route("/download/<key>/pages_content/<path:filename>")
def download_page_content(key: str, filename: str):
    path = (OUTPUT_DIR / key / "pages_content" / filename).resolve()
    content_dir = (OUTPUT_DIR / key / "pages_content").resolve()
    if not path.is_file() or content_dir not in path.parents:
        abort(404)
    return send_file(path, as_attachment=True, download_name=filename)


def run_server(host: str = "127.0.0.1", port: int = 8080) -> None:
    app.run(host=host, port=port, debug=False, threaded=True)


if __name__ == "__main__":
    run_server()
