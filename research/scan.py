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
from pipelines.url_safety import collect_allowed_hosts_from_source, sanitize_public_http_url
from pipelines.common import read_json, write_json


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


def _robots_allowed(url: str, allowed_hosts: set[str]) -> Dict[str, Any]:
    safe_url = sanitize_public_http_url(url, allowed_hosts=allowed_hosts)
    if not safe_url:
        return {"allowed": False, "reason": "unsafe_url", "crawl_delay": None}
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
    allowed = parser.can_fetch("*", safe_url)
    return {
        "allowed": bool(allowed),
        "crawl_delay": delay,
        "reason": None if allowed else "disallowed_by_robots",
    }


def _http_probe(url: str, allowed_hosts: set[str], timeout: int = 20) -> Dict[str, Any]:
    status: Dict[str, Any] = {
        "status_ok": False,
        "http_status": None,
        "content_type": None,
        "etag": None,
        "last_modified": None,
        "error": None,
    }
    safe_url = sanitize_public_http_url(url, allowed_hosts=allowed_hosts)
    if not safe_url:
        status["error"] = "unsafe_url"
        return status

    resp = None
    try:
        resp = requests.get(safe_url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True, stream=True)
        if not sanitize_public_http_url(resp.url or safe_url, allowed_hosts=allowed_hosts):
            status["error"] = "unsafe_redirect_url"
            return status
        status["http_status"] = resp.status_code
        status["content_type"] = resp.headers.get("Content-Type")
        status["etag"] = resp.headers.get("ETag")
        status["last_modified"] = resp.headers.get("Last-Modified")
        status["status_ok"] = 200 <= resp.status_code < 400
    except requests.RequestException as exc:
        status["error"] = str(exc)
    finally:
        if resp is not None:
            resp.close()
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
            "scan_status": "unavailable",
            "endpoint_checks": [],
        }
    )

    allowed_hosts = collect_allowed_hosts_from_source(item)
    if item.get("retrieval_method") == "model_generation":
        result["scan_status"] = "model_generated"
        result["scan_error"] = "local_model_no_remote_endpoint"
        return result
    if item.get("auth") in {"captcha", "restricted"}:
        result["scan_status"] = "restricted"
        result["scan_error"] = f"auto_fetch_skipped_auth={item.get('auth')}"
        return result
    if not item.get("allow_auto_fetch"):
        result["scan_status"] = "manual_evidence_required"
        result["scan_error"] = "auto_fetch_disabled_in_inventory"
        return result
    url = _safe_url(item)
    if not url:
        result["status_ok"] = False
        result["scan_error"] = "missing_or_unresolved_url"
        return result
    if not sanitize_public_http_url(url, allowed_hosts=allowed_hosts):
        result["scan_error"] = "invalid_or_unsafe_url"
        result["endpoint_checks"].append({"url": url, "status_ok": False, "http_status": None,
                                          "error": "invalid_or_unsafe_url", "request_attempted": False})
        return result

    candidates = _safe_url_list(item)
    if url and url not in candidates:
        candidates.append(url)

    if not candidates:
        result["scan_error"] = "missing_or_unresolved_url"
        return result

    successful_probe = None
    last_probe = None
    for candidate in dict.fromkeys(candidates):
        safe_candidate = sanitize_public_http_url(candidate, allowed_hosts=allowed_hosts)
        if not safe_candidate:
            result["scan_error"] = "invalid_or_unsafe_url"
            result["endpoint_checks"].append({"url": candidate, "status_ok": False, "http_status": None,
                                              "error": "invalid_or_unsafe_url", "request_attempted": False})
            continue
        robots = _robots_allowed(safe_candidate, allowed_hosts)
        if not robots.get("allowed"):
            reason = robots.get("reason")
            if reason and reason.startswith("robots_fetch"):
                # best-effort probe for transient robots failures; keep strict on explicit disallow.
                probe = _http_probe(safe_candidate, allowed_hosts)
                result["endpoint_checks"].append({"url": safe_candidate, **probe, "robots_warning": reason})
                last_probe = probe
                result["crawl_delay_seconds"] = robots.get("crawl_delay")
                result["scan_error"] = reason
                if result.get("last_modified"):
                    result["last-modified"] = result["last_modified"]
                if probe.get("status_ok") and successful_probe is None:
                    successful_probe = probe | {"scanned_url": safe_candidate}
                continue
            result["scan_error"] = reason
            result["endpoint_checks"].append({"url": safe_candidate, "status_ok": False, "http_status": None, "error": reason, "request_attempted": False})
            continue

        probe = _http_probe(safe_candidate, allowed_hosts)
        result["endpoint_checks"].append({"url": safe_candidate, **probe})
        last_probe = probe
        result["crawl_delay_seconds"] = robots.get("crawl_delay")
        result["scanned_url"] = safe_candidate
        if result.get("last_modified"):
            result["last-modified"] = result["last_modified"]

        if probe.get("error"):
            result["scan_error"] = probe["error"]
            continue

        if probe.get("status_ok") and successful_probe is None:
            successful_probe = probe | {"scanned_url": safe_candidate}

    if successful_probe:
        result.update(successful_probe)
        result["scan_status"] = "available"
        result["scan_error"] = None
        result["last-modified"] = result.get("last_modified")
        return result
    if last_probe:
        result.update(last_probe)
    if result.get("scan_error") == "disallowed_by_robots":
        result["scan_status"] = "restricted_by_robots"

    result["scan_error"] = result.get("scan_error") or "candidate_probe_failed"
    return result


def run_scan(inventory_path: str = "research/source_inventory.yaml", out_path: str = "research/source_inventory.json", min_delay: float = 1.0) -> List[Dict[str, Any]]:
    inventory = load_inventory(inventory_path)
    results: List[Dict[str, Any]] = []

    manifest_root = Path("data/manifests")
    for item in inventory.sources:
        scanned = _scan_item(item)
        manifest_path = manifest_root / f"{item['source_id']}.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            scanned["analytical_ready"] = manifest.get("analytical_ready", False)
            scanned["disclosure_ready"] = manifest.get("disclosure_ready", False)
            scanned["evidence_status"] = manifest.get("evidence_status", "unverified")
            scanned["extraction_status"] = manifest.get("extraction_status", "unknown")
            scanned["observation_as_of"] = manifest.get("source_as_of_date")
        results.append(scanned)

        delay = scanned.get("crawl_delay_seconds") or min_delay
        if delay and delay > 0:
            time.sleep(min(2.0, float(delay)))

    write_machine_inventory(results, out_path, version=inventory.version)
    return results


def sync_catalog_metadata(inventory_path: str = "research/source_inventory.yaml",
                          out_path: str = "research/source_inventory.json",
                          catalog_path: str = "data/manifests/catalog.json") -> List[Dict[str, Any]]:
    """Synchronize publication readiness without altering endpoint check evidence."""
    inventory = load_inventory(inventory_path)
    payload = read_json(Path(out_path))
    results = payload.get("sources", [])
    expected_ids = {source["source_id"] for source in inventory.sources}
    scanned_ids = [source.get("source_id") for source in results]
    if len(scanned_ids) != len(set(scanned_ids)) or set(scanned_ids) != expected_ids:
        raise ValueError("Machine inventory source IDs differ from the registered inventory; run the source scan first")
    entries = {entry["source_id"]: entry for entry in read_json(Path(catalog_path)).get("datasets", [])}
    if expected_ids - entries.keys():
        raise ValueError(f"Published catalog missing registered sources: {sorted(expected_ids - entries.keys())}")
    for result in results:
        entry = entries[result["source_id"]]
        result.update(analytical_ready=entry.get("analytical_ready", False),
                      disclosure_ready=entry.get("disclosure_ready", False),
                      evidence_status=entry.get("evidence_status", "unverified"),
                      extraction_status=entry.get("extraction_status", "unknown"),
                      observation_as_of=entry.get("source_as_of_date"),
                      publication_date=entry.get("publication_date"),
                      refresh_outcome=entry.get("refresh_outcome"),
                      refresh_error=entry.get("refresh_error"),
                      last_refresh_checked_at=entry.get("last_checked_at"))
    payload["catalog_synced_at"] = datetime.now(timezone.utc).isoformat()
    write_json(payload, Path(out_path))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run official source inventory scan")
    parser.add_argument("--inventory", default="research/source_inventory.yaml")
    parser.add_argument("--out", default="research/source_inventory.json")
    parser.add_argument("--min-delay", type=float, default=1.0)
    parser.add_argument("--sync-catalog", action="store_true", help="Update readiness from the published catalog without network requests")
    parser.add_argument("--catalog", default="data/manifests/catalog.json")
    args = parser.parse_args()
    results = sync_catalog_metadata(args.inventory, args.out, args.catalog) if args.sync_catalog else run_scan(args.inventory, args.out, args.min_delay)

    ok = sum(1 for item in results if item.get("status_ok"))
    total = len(results)
    print(json.dumps({"status": "catalog_synced" if args.sync_catalog else "done", "checked": total, "ok": ok, "out": str(Path(args.out))}, indent=2))


if __name__ == "__main__":
    main()
