"""Verify the complete downloaded static-site bundle before deployment."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath


def verify_bundle(root: Path, expected_bundle_sha: str | None = None,
                  expected_source_sha: str | None = None) -> dict:
    manifest = json.loads((root / "bundle-manifest.json").read_text())
    records = manifest["files"]
    if not isinstance(records, list) or manifest["file_count"] != len(records):
        raise ValueError("Bundle manifest file count mismatch")
    if expected_source_sha is not None and manifest.get("git_sha") != expected_source_sha:
        raise ValueError("Bundle source revision mismatch")
    paths = [item["path"] for item in records]
    if len(paths) != len(set(paths)) or paths != sorted(paths):
        raise ValueError("Bundle paths must be unique and sorted")
    required = {".nojekyll", "apps/web/.nojekyll", "data/manifests/catalog.json", "data/manifests/refresh_report.json"}
    if not required <= set(paths):
        raise ValueError("Bundle manifest lacks required deployment files")
    aggregate = hashlib.sha256()
    hashes = {}
    for item in records:
        relative = PurePosixPath(item["path"])
        if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != item["path"] or "\\" in item["path"]:
            raise ValueError("Unsafe bundle manifest path")
        path = root / item["path"]
        if not path.is_file() or path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
            raise ValueError(f"Missing or unsafe bundle file: {item['path']}")
        contents = path.read_bytes()
        if len(contents) != item["bytes"]:
            raise ValueError(f"Bundle byte length mismatch: {item['path']}")
        checksum = hashlib.sha256(contents).hexdigest()
        if checksum != item["sha256"]:
            raise ValueError(f"Bundle file checksum mismatch: {item['path']}")
        hashes[item["path"]] = checksum
        aggregate.update(checksum.encode("utf-8"))
    actual = {path.relative_to(root).as_posix() for path in root.rglob("*")
              if path.is_file() and path.relative_to(root).as_posix() != "bundle-manifest.json"
              and not path.relative_to(root).as_posix().startswith(".git/")}
    if actual != set(paths):
        raise ValueError("Bundle contains unlisted files or missing manifest entries")
    bundle_sha = aggregate.hexdigest()
    if bundle_sha != manifest["bundle_sha256"] or (expected_bundle_sha is not None and bundle_sha != expected_bundle_sha):
        raise ValueError("Aggregate bundle checksum mismatch")
    for key, path in (("catalog_sha256", "data/manifests/catalog.json"),
                      ("refresh_report_sha256", "data/manifests/refresh_report.json")):
        if manifest.get(key) != hashes[path]:
            raise ValueError(f"Bundle {key} mismatch")
    return {"verified_files": len(records), "bundle_sha256": bundle_sha, "source_sha": manifest.get("git_sha")}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--expected-bundle-sha")
    parser.add_argument("--expected-source-sha")
    args = parser.parse_args()
    try:
        result = verify_bundle(args.root, args.expected_bundle_sha, args.expected_source_sha)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f"Site bundle verification failed: {exc}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
