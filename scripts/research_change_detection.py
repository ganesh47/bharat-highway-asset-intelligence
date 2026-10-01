"""Identify source/extractor changes that require rebuilding NHAI OCR evidence."""
from __future__ import annotations

import argparse
from pathlib import Path
import re
import subprocess
import sys

OCR_PATH = re.compile(
    r"^(?:\.github/workflows/(?:research-pipeline|github-pages)\.yml$|"
    r"requirements\.txt$|research/source_inventory\.yaml$|"
    r"pipelines/(?:ingest|quality)\.py$|"
    r"pipelines/connectors/nhai_annual_documents\.py$|"
    r"scripts/(?:research_change_detection|nhai_annual_report_(?:extractor|merge))\.py$|"
    r"data/(?:raw/manual|manifests|processed)/nhai_)"
)


def requires_ocr(paths: list[str]) -> bool:
    return any(OCR_PATH.search(path.strip()) for path in paths)


def push_requires_ocr(before: str, after: str, repo: str | Path = ".") -> bool:
    """Fetch a shallow checkout's comparison base; uncertain diffs require OCR."""
    if not all(re.fullmatch(r"[0-9a-fA-F]{40,64}", sha or "") for sha in (before, after)) or not before.strip("0"):
        return True

    def git(*args: str, timeout: int = 30) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=True, timeout=timeout)

    try:
        try:
            git("cat-file", "-e", f"{before}^{{commit}}")
        except subprocess.CalledProcessError:
            git("fetch", "--no-tags", "--depth=1", "origin", before, timeout=120)
        paths = git("diff", "--name-only", before, after).stdout.splitlines()
    except (OSError, subprocess.SubprocessError):
        return True
    return requires_ocr(paths)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--git-before")
    parser.add_argument("--git-after")
    parser.add_argument("--repo", default=".")
    args = parser.parse_args()
    if args.git_before is not None or args.git_after is not None:
        required = push_requires_ocr(args.git_before or "", args.git_after or "", args.repo)
    else:
        required = requires_ocr(sys.stdin.read().splitlines())
    print("true" if required else "false")
