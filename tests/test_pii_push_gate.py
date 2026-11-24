"""Exercise the installed hook at Git's real push boundary."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


HOOKS = Path(__file__).resolve().parents[1] / ".githooks"


class PushGateTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.remote = root / "remote.git"
        self.local = root / "local"
        self.local.mkdir()
        self.env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
        self.git("init", "--bare", str(self.remote), cwd=root)
        self.git("init", "-b", "main", cwd=self.local)
        self.git("config", "user.name", "Example Tester")
        self.git("config", "user.email", "tester@example.com")
        self.git("config", "core.hooksPath", str(HOOKS))
        self.git("remote", "add", "origin", str(self.remote))
        self.commit("README.md", "Safe baseline\n", "baseline")
        self.push("main", allowed=True)
        self.baseline = self.git("rev-parse", "HEAD")

    def git(self, *args, cwd=None):
        return subprocess.run(
            ["git", *args], cwd=cwd or self.local, env=self.env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True,
        ).stdout.strip()

    def commit(self, path, content, message):
        target = self.local / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content if isinstance(content, bytes) else content.encode())
        self.git("add", "--", path)
        self.git("commit", "-m", message)

    def remote_state(self):
        refs = self.git("for-each-ref", "--format=%(refname) %(objectname)", cwd=self.remote)
        objects = self.git("count-objects", "-v", cwd=self.remote)
        count = next(int(line.split(": ")[1]) for line in objects.splitlines() if line.startswith("count: "))
        return refs, count

    def push(self, ref, allowed, category=None):
        before = self.remote_state()
        proc = subprocess.run(
            ["git", "push", "origin", ref], cwd=self.local, env=self.env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.assertEqual(proc.returncode == 0, allowed, proc.stderr)
        self.assertNotIn("private.invalid", proc.stderr)
        if not allowed:
            self.assertIn("PII gate: rejected push", proc.stderr)
            if category:
                self.assertIn(category, proc.stderr)
            self.assertEqual(self.remote_state(), before, "remote received objects before rejection")
        return proc

    def test_safe_example_and_source_pattern_pass(self):
        self.commit("sample.txt", "tester@example.org\nformat: %s@provider.invalid\n", "safe sample")
        self.push("main", allowed=True)

    def test_email_phone_id_and_secret_reject(self):
        payloads = (
            ("person" + "@" + "private.invalid", "email address"),
            ("+66" + "812345678", "phone number"),
            ("passport: " + "123456789", "private identifier"),
            ("ghp_" + "a" * 30, "credential"),
        )
        for index, (payload, category) in enumerate(payloads):
            with self.subTest(index=index):
                self.commit(f"sample-{index}.txt", payload + "\n", "new data")
                self.push("main", allowed=False, category=category)
                self.git("reset", "--hard", self.baseline)

    def test_intermediate_deleted_content_rejects(self):
        self.commit("temporary.txt", "person" + "@" + "private.invalid", "temporary data")
        self.git("rm", "temporary.txt")
        self.git("commit", "-m", "remove temporary data")
        self.push("main", allowed=False, category="email address")

    def test_filename_commit_message_and_private_file_reject(self):
        self.commit("person" + "@" + "private.invalid.txt", "safe\n", "filename")
        self.push("main", allowed=False, category="in filename")
        self.git("reset", "--hard", self.baseline)
        self.commit("ordinary.txt", "safe\n", "call +66" + "812345678")
        self.push("main", allowed=False, category="phone number")
        self.git("reset", "--hard", self.baseline)
        self.commit(".env", "SAFE=1\n", "private file")
        self.push("main", allowed=False, category="private data file")

    def test_binary_rejects(self):
        self.commit("image.bin", b"\x00\x01", "binary")
        self.push("main", allowed=False, category="binary or oversized file")

    def test_new_branch_and_annotated_tag_reject(self):
        self.git("checkout", "-b", "new-branch")
        self.commit("private.txt", "person" + "@" + "private.invalid", "new branch")
        self.push("new-branch", allowed=False, category="email address")
        self.git("checkout", "main")
        self.git("tag", "-a", "v1", "-m", "phone +66" + "812345678")
        self.push("v1", allowed=False, category="phone number")

    def test_new_branch_does_not_rescan_known_remote_history(self):
        self.commit("old.txt", "person" + "@" + "private.invalid", "old data")
        self.git("push", "--no-verify", "origin", "main")  # Local bare remote only.
        self.git("checkout", "-b", "clean-branch")
        self.commit("safe.txt", "tester@example.net\n", "safe update")
        self.push("clean-branch", allowed=True)

    def test_ref_name_rejects(self):
        name = "person" + "@" + "private.invalid"
        self.git("checkout", "-b", name)
        self.push(name, allowed=False, category="in ref name")


if __name__ == "__main__":
    unittest.main()
