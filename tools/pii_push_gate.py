#!/usr/bin/env python3
"""Reject outgoing commits with likely personal data or credentials.

The hook deliberately reports categories and locations, never matched values.
"""

import re
import subprocess
import sys


ZERO = "0" * 40
MAX_BLOB = 2 * 1024 * 1024
EMAIL = re.compile(r"(?<![\w.+%$-])[\w.+-]+@(?:[\w-]+\.)+[A-Za-z]{2,}(?![\w.-])")
PHONE = re.compile(r"(?<![\w])\+\d[\d ().-]{7,}\d(?![\w])")
LABELED_PHONE = re.compile(
    r"(?i)\b(?:phone|mobile|telephone|whatsapp)\b[\s\"']*[:=][\s\"']*"
    r"\d[\d ().-]{7,}\d\b"
)
PRIVATE_ID = re.compile(
    r"(?i)\b(?:passport|national[_ -]?id|tax[_ -]?id|ssn|social[_ -]?security|"
    r"bank[_ -]?account|iban|account[_ -]?number|profile[_ -]?id)\b"
    r"[\s\"']*[:=][\s\"']*[A-Z0-9][A-Z0-9 -]{7,}\b"
)
SECRET = re.compile(
    r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|"
    r"\b(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,}|"
    r"AKIA[0-9A-Z]{16}|sk-[A-Za-z0-9_-]{20,})\b"
)
LABELED_SECRET = re.compile(
    r"(?i)\b(?:api[_ -]?key|access[_ -]?token|client[_ -]?secret|password)\b"
    r"[\s\"']*[:=][\s\"']*([A-Za-z0-9_+/=-]{20,})"
)
PRIVATE_FILE = re.compile(
    r"(?i)(?:^|/)(?:\.env(?:\..*)?|id_(?:rsa|ed25519)|"
    r"(?:auth[-_]?profiles|credentials|secrets|private[-_]?data|"
    r"customer[-_]?data|user[-_]?data)(?:[._-].*)?|.*\.(?:pem|key|p12|pfx|"
    r"sqlite3?|db|csv|xlsx?|pdf))$"
)


def git(*args, input_data=None):
    proc = subprocess.run(
        ["git", *args], input=input_data, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, check=False,
    )
    if proc.returncode:
        raise RuntimeError("Git could not inspect the outgoing objects")
    return proc.stdout


def findings(text):
    result = set()
    for match in EMAIL.finditer(text):
        address = match.group().lower()
        domain = address.rsplit("@", 1)[1]
        if domain not in {"example.com", "example.net", "example.org"}:
            # A fixed, public automation identity in existing Git trailers.
            if not (address == "noreply" + "@anthropic.com" and
                    text.strip().startswith("Co-Authored-By: Claude ")):
                result.add("email address")
    if PHONE.search(text) or LABELED_PHONE.search(text):
        result.add("phone number")
    if any(sum(char.isdigit() for char in match.group()) >= 4 for match in PRIVATE_ID.finditer(text)):
        result.add("private identifier")
    if SECRET.search(text) or LABELED_SECRET.search(text):
        result.add("credential")
    return result


def report(category, commit, path, line=None):
    suffix = f":{line}" if line is not None else ""
    print(f"PII gate: {category} in {commit[:12]}:{path}{suffix}", file=sys.stderr)


def scan_text(data, commit, path):
    if len(data) > MAX_BLOB or b"\0" in data:
        report("binary or oversized file", commit, path)
        return 1
    try:
        decoded = data.decode("utf-8")
    except UnicodeDecodeError:
        report("non-UTF-8 file", commit, path)
        return 1
    count = 0
    for number, line in enumerate(decoded.splitlines(), 1):
        for category in sorted(findings(line)):
            report(category, commit, path, number)
            count += 1
    return count


def scan_commit(commit):
    count = scan_text(git("show", "-s", "--format=%B", commit), commit, "<commit message>")
    paths = git("diff-tree", "--root", "-m", "--no-commit-id", "-r", "--name-only", "-z", commit).split(b"\0")
    for raw_path in filter(None, paths):
        path = raw_path.decode("utf-8", "surrogateescape")
        path_findings = findings(path)
        private_file = bool(PRIVATE_FILE.search(path))
        display_path = "<redacted filename>" if path_findings or private_file else path
        for category in sorted(path_findings):
            report(category + " in filename", commit, display_path)
            count += 1
        if private_file:
            report("private data file", commit, display_path)
            count += 1
        try:
            blob = git("rev-parse", "--verify", f"{commit}:{path}").strip().decode("ascii")
        except RuntimeError:
            # A deletion still has its filename checked; its prior content was
            # checked when the earlier outgoing commit introduced it.
            continue
        kind = git("cat-file", "-t", blob).strip()
        if kind != b"blob":
            report("unsupported Git object", commit, display_path)
            count += 1
            continue
        size = int(git("cat-file", "-s", blob))
        if size > MAX_BLOB:
            report("oversized file", commit, display_path)
            count += 1
            continue
        count += scan_text(git("cat-file", "blob", blob), commit, display_path)
    return count


def main():
    count = 0
    seen = set()
    remote_name = sys.argv[1] if len(sys.argv) > 1 else ""
    for raw in sys.stdin.buffer:
        fields = raw.decode("utf-8", "replace").strip().split()
        if not fields:
            continue  # Git sends no ref lines when nothing is outgoing.
        if len(fields) != 4:
            raise RuntimeError("Invalid pre-push ref input")
        local_ref, local_oid, remote_ref, remote_oid = fields
        if local_oid in (ZERO, "0" * 64):
            continue  # A deletion sends no objects.
        for ref in {local_ref, remote_ref}:
            for category in sorted(findings(ref)):
                report(category + " in ref name", local_oid, "<redacted ref>")
                count += 1
        if not all(re.fullmatch(r"[0-9a-f]{40,64}", oid) for oid in (local_oid, remote_oid)):
            raise RuntimeError("Invalid pre-push object ID")
        obj_type = git("cat-file", "-t", local_oid).strip()
        if obj_type == b"tag":
            tag_data = git("cat-file", "tag", local_oid)
            count += scan_text(tag_data.split(b"\n\n", 1)[-1], local_oid, "<tag message>")
        elif obj_type != b"commit":
            raise RuntimeError("Unsupported pushed ref")
        commit = git("rev-parse", "--verify", f"{local_oid}^{{commit}}").strip().decode("ascii")
        args = ["rev-list", "--reverse", commit]
        if remote_oid not in (ZERO, "0" * 64):
            args.append("^" + remote_oid)
        if remote_name and re.fullmatch(r"[A-Za-z0-9._-]+", remote_name):
            # History the remote already has (any of its branches) is not newly sent.
            args.append("--not")
            args.append(f"--remotes={remote_name}")
        for outgoing in git(*args).decode("ascii").splitlines():
            if outgoing not in seen:
                seen.add(outgoing)
                count += scan_commit(outgoing)
    if count:
        print(f"PII gate: rejected push ({count} finding(s)); no objects sent.", file=sys.stderr)
        return 1
    print("PII gate: outgoing commits checked.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, ValueError) as exc:
        print(f"PII gate: inspection failed closed: {exc}", file=sys.stderr)
        sys.exit(1)
