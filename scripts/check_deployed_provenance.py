"""Verify that Pages serves the validated research catalog and packaged revision."""
from __future__ import annotations

import argparse
import hashlib
import json
from urllib.parse import urljoin

import requests


def verify(url: str, bundle_sha: str, catalog_sha: str | None = None) -> dict:
    root = url.rstrip("/") + "/"
    response = requests.get(urljoin(root, "bundle-manifest.json"), timeout=30)
    response.raise_for_status()
    manifest = response.json()
    if manifest.get("bundle_sha256") != bundle_sha:
        raise ValueError("Published bundle does not match the deployment artifact")
    response = requests.get(urljoin(root, "data/manifests/catalog.json"), timeout=30)
    response.raise_for_status()
    checksum = hashlib.sha256(response.content).hexdigest()
    expected = catalog_sha or manifest.get("catalog_sha256")
    if not expected or checksum != expected:
        raise ValueError("Published catalog does not match the validated research catalog")
    catalog = response.json()
    if not catalog.get("datasets"):
        raise ValueError("Published catalog is empty")
    return {"url": root, "git_sha": manifest["git_sha"], "research_run_id": manifest["research_run_id"],
            "bundle_sha256": bundle_sha, "catalog_sha256": checksum,
            "datasets": len(catalog["datasets"])}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--bundle-sha", required=True)
    parser.add_argument("--catalog-sha")
    args = parser.parse_args()
    print(json.dumps(verify(args.url, args.bundle_sha, args.catalog_sha), indent=2))
