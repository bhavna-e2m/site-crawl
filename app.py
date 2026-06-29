#!/usr/bin/env python3
"""Web UI for the site crawler."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from urllib.parse import unquote

from flask import (
    Flask,
    Response,
    abort,
    flash,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_file,
    stream_with_context,
    url_for,
)

from crawl_site import (
    OUTPUT_DIR,
    compute_crawl_progress,
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


def is_crawl_active(key: str) -> bool:
    with _crawl_lock:
        return key in _active_crawls


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
        message="Starting crawl…",
        phase="resolve",
        crawl_links=crawl_links,
        fetch_pages=fetch_pages,
        progress=1,
        counts={"products": 0, "collections": 0, "pages": 0, "total": 0},
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
                message=(
                    f"Done — {counts['products']:,} products, "
                    f"{counts['collections']:,} collections, {counts['pages']:,} pages"
                ),
                phase="done",
                counts=counts,
                progress=100,
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
        message="Starting crawl…",
        phase="resolve",
        crawl_links=crawl_links,
        fetch_pages=fetch_pages,
        progress=1,
        counts={"products": 0, "collections": 0, "pages": 0, "total": 0},
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
            message=(
                f"Done — {counts['products']:,} products, "
                f"{counts['collections']:,} collections, {counts['pages']:,} pages"
            ),
            phase="done",
            counts=counts,
            progress=100,
        )
        return "done", counts
    except Exception as exc:
        write_crawl_status(key, state="error", message=str(exc))
        return "error", None


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

    return redirect(
        url_for(
            "crawl_status_page",
            key=key,
            crawl_links="1" if crawl_links else "0",
            fetch_pages="1" if fetch_pages else "0",
            start="1",
        )
    )


@app.route("/crawl-status/<key>")
def crawl_status_page(key: str):
    sites = load_sites()
    if key not in sites:
        flash(f"Site not found: {key}", "error")
        return redirect(url_for("index"))

    crawl_links = request.args.get("crawl_links") == "1"
    fetch_pages = request.args.get("fetch_pages") == "1"
    crawl_links_q = "1" if crawl_links else "0"
    fetch_pages_q = "1" if fetch_pages else "0"

    if request.args.get("start") == "1":
        status = read_crawl_status(key)
        stale_running = (
            status
            and status.get("state") == "running"
            and not is_crawl_active(key)
        )
        should_start = (
            not IS_VERCEL
            and not is_crawl_active(key)
            and (stale_running or not status or status.get("state") in ("pending", "done", "error"))
        )
        if should_start:
            start_background_crawl(key, crawl_links=crawl_links, fetch_pages=fetch_pages)
        return redirect(
            url_for(
                "crawl_status_page",
                key=key,
                crawl_links=crawl_links_q,
                fetch_pages=fetch_pages_q,
            )
        )

    status = read_crawl_status(key)
    if status is None:
        status = {
            "state": "pending",
            "message": "Waiting to start…",
            "progress": 0,
            "counts": {"products": 0, "collections": 0, "pages": 0, "total": 0},
            "phase_label": "Starting crawl",
        }
    else:
        status = compute_crawl_progress(status)

    return render_template(
        "crawl_status.html",
        key=key,
        info=sites[key],
        status=status,
        crawl_links=crawl_links,
        fetch_pages=fetch_pages,
        is_vercel=IS_VERCEL,
        status_stream_url=url_for("crawl_status_events", key=key),
    )


@app.route("/api/crawl/<key>", methods=["POST"])
def api_start_crawl(key: str):
    sites = load_sites()
    if key not in sites:
        return jsonify({"state": "error", "message": "Site not found"}), 404

    existing = read_crawl_status(key)
    if existing and existing.get("state") == "running" and is_crawl_active(key):
        return jsonify(existing)

    data = request.get_json(silent=True) or {}
    crawl_links = bool(data.get("crawl_links"))
    fetch_pages = bool(data.get("fetch_pages"))

    if IS_VERCEL:
        run_crawl_sync(key, crawl_links=crawl_links, fetch_pages=fetch_pages)
        return jsonify(read_crawl_status(key) or {"state": "error", "message": "Crawl finished without status."})

    if not start_background_crawl(key, crawl_links=crawl_links, fetch_pages=fetch_pages):
        return jsonify(read_crawl_status(key) or {"state": "running", "message": "Crawl already running."})
    return jsonify(read_crawl_status(key) or {"state": "running", "message": "Starting crawl..."})


@app.route("/api/crawl-status/<key>")
def crawl_status_api(key: str):
    status = read_crawl_status(key, enrich=True)
    if status is None:
        payload = {
            "state": "unknown",
            "message": "No crawl in progress.",
            "progress": 0,
            "counts": {"products": 0, "collections": 0, "pages": 0, "total": 0},
        }
    else:
        payload = status
    response = make_response(jsonify(payload))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/api/crawl-status/<key>/events")
def crawl_status_events(key: str):
    """Server-Sent Events stream — pushes status changes to the browser live."""

    def generate():
        yield ": connected\n\n"
        last_revision = -1
        idle = 0
        while idle < 120:
            status = read_crawl_status(key, enrich=True)
            if status is None:
                payload = {
                    "state": "unknown",
                    "message": "No crawl in progress.",
                    "progress": 0,
                    "counts": {"products": 0, "collections": 0, "pages": 0, "total": 0},
                    "revision": 0,
                }
            else:
                payload = status
            revision = int(payload.get("revision") or 0)
            if revision != last_revision:
                yield f"data: {json.dumps(payload)}\n\n"
                last_revision = revision
                idle = 0
                if payload.get("state") in ("done", "error"):
                    return
            else:
                idle += 1
                if idle % 5 == 0:
                    yield ": keepalive\n\n"
            time.sleep(0.2)

    response = Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
    )
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"
    response.headers["X-Accel-Buffering"] = "no"
    return response


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
    details = {
        "products": load_detail(key, "products"),
        "collections": load_detail(key, "collections"),
        "pages": load_detail(key, "pages"),
    }
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
        details=details,
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

    return redirect(
        url_for(
            "crawl_status_page",
            key=key,
            crawl_links="1" if crawl_links else "0",
            fetch_pages="1" if fetch_pages else "0",
            start="1",
        )
    )


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
