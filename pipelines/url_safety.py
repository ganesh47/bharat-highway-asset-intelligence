from __future__ import annotations

import ipaddress
from typing import Any, Iterable
from urllib.parse import urlsplit


def _normalize_host(value: str) -> str:
    return value.strip().lower().rstrip(".")


def _iter_candidate_values(value: Any) -> Iterable[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if item]
    return []


def _is_safe_public_host(host: str) -> bool:
    normalized = _normalize_host(host)
    if not normalized:
        return False
    if normalized == "localhost" or normalized.endswith(".localhost"):
        return False
    try:
        parsed_ip = ipaddress.ip_address(normalized)
    except ValueError:
        return True
    return not (
        parsed_ip.is_private
        or parsed_ip.is_loopback
        or parsed_ip.is_link_local
        or parsed_ip.is_multicast
        or parsed_ip.is_reserved
        or parsed_ip.is_unspecified
    )


def is_public_http_url(
    url: str,
    *,
    allowed_hosts: Iterable[str] | None = None,
    allowed_host_suffixes: Iterable[str] | None = None,
) -> bool:
    split = urlsplit(str(url).strip())
    if split.scheme not in {"http", "https"}:
        return False
    if split.username or split.password:
        return False
    host = _normalize_host(split.hostname or "")
    if not _is_safe_public_host(host):
        return False

    normalized_hosts = {_normalize_host(value) for value in (allowed_hosts or []) if str(value).strip()}
    normalized_suffixes = {_normalize_host(value) for value in (allowed_host_suffixes or []) if str(value).strip()}
    if normalized_hosts and host in normalized_hosts:
        return True
    if normalized_suffixes and any(host == suffix or host.endswith(f".{suffix}") for suffix in normalized_suffixes):
        return True
    if normalized_hosts or normalized_suffixes:
        return False
    return True


def sanitize_public_http_url(
    url: str,
    *,
    allowed_hosts: Iterable[str] | None = None,
    allowed_host_suffixes: Iterable[str] | None = None,
) -> str | None:
    candidate = str(url or "").strip()
    if not candidate:
        return None
    if not is_public_http_url(
        candidate,
        allowed_hosts=allowed_hosts,
        allowed_host_suffixes=allowed_host_suffixes,
    ):
        return None
    return candidate


def collect_allowed_hosts_from_source(source: dict[str, Any]) -> set[str]:
    hosts: set[str] = set()

    for key in ("domain", "url", "resource_page_url", "annual_document_url_prefix"):
        for value in _iter_candidate_values(source.get(key)):
            split = urlsplit(value if "://" in value else f"https://{value}")
            host = _normalize_host(split.hostname or "")
            if host:
                hosts.add(host)

    for key in ("resource_file_urls", "discovery_endpoints"):
        for value in _iter_candidate_values(source.get(key)):
            split = urlsplit(value if "://" in value else f"https://{value}")
            host = _normalize_host(split.hostname or "")
            if host:
                hosts.add(host)

    return hosts
