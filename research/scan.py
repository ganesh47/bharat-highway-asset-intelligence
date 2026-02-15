from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse
import urllib.robotparser

import requests

from .loader import load_inventory, write_machine_inventory


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; BHAI-research-scan/0.2; +https://example.local/official-first-scan)"
    )
}


def _safe_url(item: Dict[str, Any]) -> str | None:
    url = item.get("resource_page_url") or item.get("url")
    if not url:
        return None
    if "{resource_id}" not in url:
        return url

    if str(item.get("resource_id", "")).startswith("PLACEHOLDER_"):
        return None

    resource_id = item.get("resource_id")
    if not resource_id and item.get("resource_id_env"):
        resource_id = os.getenv(item.get("resource_id_env", "").strip())

    if not resource_id:
        return None
    return url.format(resource_id=resource_id)


def _safe_url_list(item: Dict[str, Any]) -> list[str]:
    raw = item.get("resource_file_urls")
    if not raw:
        return []
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, (tuple, list, set)):
        return [str(value) for value in raw if value]
    return []


def _robots_allowed(url: str) -> Dict[str, Any]:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return {"allowed": False, "reason": "invalid_url", "crawl_delay": None}
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    parser = urllib.robotparser.RobotFileParser()
    parser.set_url(robots_url)
    try:
        resp = requests.get(robots_url, headers=DEFAULT_HEADERS, timeout=15)
        if resp.status_code == 404:
            return {"allowed": True, "crawl_delay": None, "reason": None}
        if resp.status_code >= 400:
            return {
                "allowed": False,
                "reason": f"robots_fetch_http_{resp.status_code}",
                "crawl_delay": None,
            }
        parser.parse(resp.text.splitlines())
    except Exception as exc:  # pragma: no cover - network dependent
        return {
            "allowed": False,
            "reason": f"robots_fetch_failed:{exc.__class__.__name__}",
            "crawl_delay": None,
        }
    delay = parser.crawl_delay("")
    allowed = parser.can_fetch("*", url)
    return {
        "allowed": bool(allowed),
        "crawl_delay": delay,
        "reason": None if allowed else "disallowed_by_robots",
    }


def _http_probe(url: str, timeout: int = 20) -> Dict[str, Any]:
    status: Dict[str, Any] = {
        "status_ok": False,
        "http_status": None,
        "content_type": None,
        "etag": None,
        "last_modified": None,
        "error": None,
    }

    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
        status["http_status"] = resp.status_code
        status["content_type"] = resp.headers.get("Content-Type")
        status["etag"] = resp.headers.get("ETag")
        status["last_modified"] = resp.headers.get("Last-Modified")
        status["status_ok"] = 200 <= resp.status_code < 400
    except requests.RequestException as exc:
        status["error"] = str(exc)
    return status


def _scan_item(item: Dict[str, Any]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    source_id = item.get("source_id")
    result = dict(item)
    result.update(
        {
            "last_checked_at": now,
            "status_ok": False,
            "http_status": None,
            "content_type": None,
            "etag": None,
            "last_modified": None,
            "last-modified": None,
            "scan_error": None,
        }
    )

    url = _safe_url(item)
    if not url:
        result["status_ok"] = False
        result["scan_error"] = "missing_or_unresolved_url"
        return result

    if item.get("auth") in {"captcha", "restricted"}:
        result["scan_error"] = f"auto_fetch_skipped_auth={item.get('auth')}"
        return result

    if not item.get("allow_auto_fetch"):
        result["scan_error"] = "auto_fetch_disabled_in_inventory"
        return result

    candidates = _safe_url_list(item)
    if url and url not in candidates:
        candidates.append(url)

    if not candidates:
        result["scan_error"] = "missing_or_unresolved_url"
        return result

    for candidate in candidates:
        robots = _robots_allowed(candidate)
        if not robots.get("allowed"):
            reason = robots.get("reason")
            if reason and reason.startswith("robots_fetch"):
                # best-effort probe for transient robots failures; keep strict on explicit disallow.
                result.update(_http_probe(candidate))
                result["crawl_delay_seconds"] = robots.get("crawl_delay")
                result["scan_error"] = reason
                if result.get("last_modified"):
                    result["last-modified"] = result["last_modified"]
                if result.get("status_ok"):
                    result["scanned_url"] = candidate
                    return result
                continue

            continue

        probe = _http_probe(candidate)
        result.update(probe)
        result["crawl_delay_seconds"] = robots.get("crawl_delay")
        result["scanned_url"] = candidate
        if result.get("last_modified"):
            result["last-modified"] = result["last_modified"]

        if probe.get("error"):
            result["scan_error"] = probe["error"]
            continue

        return result

    result["scan_error"] = result.get("scan_error") or "candidate_probe_failed"
    return result


def run_scan(inventory_path: str = "research/source_inventory.yaml", out_path: str = "research/source_inventory.json", min_delay: float = 1.0) -> List[Dict[str, Any]]:
    inventory = load_inventory(inventory_path)
    results: List[Dict[str, Any]] = []

    for item in inventory.sources:
        scanned = _scan_item(item)
        results.append(scanned)

        delay = scanned.get("crawl_delay_seconds") or min_delay
        if delay and delay > 0:
            time.sleep(min(2.0, float(delay)))

    write_machine_inventory(results, out_path, version=inventory.version)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run official source inventory scan")
    parser.add_argument("--inventory", default="research/source_inventory.yaml")
    parser.add_argument("--out", default="research/source_inventory.json")
    parser.add_argument("--min-delay", type=float, default=1.0)
    args = parser.parse_args()
    results = run_scan(args.inventory, args.out, args.min_delay)

    ok = sum(1 for item in results if item.get("status_ok"))
    total = len(results)
    print(json.dumps({"status": "done", "checked": total, "ok": ok, "out": str(Path(args.out))}, indent=2))


if __name__ == "__main__":
    main()
