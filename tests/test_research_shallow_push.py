"""Exercise real shallow Git history instead of mocking a successful diff."""
from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path


class ShallowPushDetectionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.origin = self.root / "origin"
        self.origin.mkdir()
        self.git(self.origin, "init", "-q")
        self.git(self.origin, "config", "user.name", "Workflow regression fixture")
        self.git(self.origin, "config", "user.email", "workflow-fixture@example.com")
        self.git(self.origin, "config", "commit.gpgsign", "false")
        self.git(self.origin, "config", "core.hooksPath", str(self.root / "disabled-hooks"))
        (self.origin / "pipelines").mkdir()
        (self.origin / "pipelines/quality.py").write_text("observation_cutoff = 'unverified'\n")
        self.baseline = self.commit("Baseline")
        (self.origin / "pipelines/quality.py").write_text("observation_cutoff = None\n")
        self.pipeline_revision = self.commit("Clear unsupported observation cutoff")
        (self.origin / "README.md").write_text("Finance analyst guidance\n")
        self.commit("Update guidance")
        (self.origin / "docs").mkdir()
        (self.origin / "docs/validation.md").write_text("Validation summary\n")
        self.head = self.commit("Record validation")

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def git(repo: Path, *arguments: str, check: bool = True):
        return subprocess.run(["git", *arguments], cwd=repo, capture_output=True, text=True,
                              check=check, timeout=15)

    def commit(self, message: str) -> str:
        self.git(self.origin, "add", ".")
        self.git(self.origin, "commit", "-q", "-m", message)
        return self.git(self.origin, "rev-parse", "HEAD").stdout.strip()

    def shallow_checkout(self) -> Path:
        checkout = self.root / "checkout"
        self.git(self.root, "clone", "-q", "--depth", "2", self.origin.as_uri(), str(checkout))
        self.assertEqual("true", self.git(checkout, "rev-parse", "--is-shallow-repository").stdout.strip())
        self.assertNotEqual(0, self.git(checkout, "cat-file", "-e", f"{self.baseline}^{{commit}}", check=False).returncode)
        self.assertNotEqual(0, self.git(checkout, "cat-file", "-e", f"{self.pipeline_revision}^{{commit}}", check=False).returncode)
        return checkout

    def test_three_commit_push_fetches_missing_base_and_detects_earlier_pipeline_change(self):
        from scripts.research_change_detection import push_requires_ocr
        checkout = self.shallow_checkout()
        self.assertTrue(push_requires_ocr(self.baseline, self.head, repo=checkout))
        self.git(checkout, "cat-file", "-e", f"{self.baseline}^{{commit}}")

    def test_documentation_only_push_can_skip_ocr_after_fetching_missing_base(self):
        from scripts.research_change_detection import push_requires_ocr
        checkout = self.shallow_checkout()
        self.assertFalse(push_requires_ocr(self.pipeline_revision, self.head, repo=checkout))
        self.git(checkout, "cat-file", "-e", f"{self.pipeline_revision}^{{commit}}")

    def test_unfetchable_base_requires_ocr_instead_of_becoming_empty_diff(self):
        from scripts.research_change_detection import push_requires_ocr
        checkout = self.shallow_checkout()
        self.assertTrue(push_requires_ocr("f" * 40, self.head, repo=checkout))


if __name__ == "__main__":
    unittest.main()
