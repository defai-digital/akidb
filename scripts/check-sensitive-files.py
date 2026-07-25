#!/usr/bin/env python3
"""Reject sensitive deployment data and local-only files from Git tracking."""

from __future__ import annotations

import ipaddress
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath


LOCAL_ONLY_NAMES = {"AGENTS.md", "CLAUDE.md"}
SENSITIVE_FILENAMES = {
    "authorized_keys",
    "known_hosts",
    "known_hosts.old",
}
KEY_FILENAME_RE = re.compile(
    r"id_(?:rsa|dsa|ecdsa|ed25519)(?:[._-].*)?$", re.IGNORECASE
)
PRIVATE_KEY_RE = re.compile(
    rb"-----BEGIN (?:(?:OPENSSH|RSA|EC|DSA|ENCRYPTED) PRIVATE KEY|"
    rb"PRIVATE KEY|PGP PRIVATE KEY BLOCK)-----"
)
SSH_PUBLIC_KEY_RE = re.compile(
    rb"(?m)^[ \t]*(?:ssh-(?:rsa|ed25519)|ecdsa-sha2-nistp\d+)[ \t]+"
    rb"[A-Za-z0-9+/]{40,}={0,3}(?:[ \t]|$)"
)
TOKEN_PATTERNS = (
    re.compile(rb"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(rb"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
    re.compile(rb"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(rb"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
)
IPV4_RE = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"^\s*"
    r"([A-Za-z0-9_.-]*(?:password|passphrase|token|secret|private[_-]?key)"
    r"[A-Za-z0-9_.-]*)"
    r"\s*[:=]\s*(.*?)\s*$",
    re.IGNORECASE,
)
DOCUMENTATION_NETWORKS = (
    ipaddress.ip_network("192.0.2.0/24"),
    ipaddress.ip_network("198.51.100.0/24"),
    ipaddress.ip_network("203.0.113.0/24"),
)


def tracked_files(repo_root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repo_root,
        check=True,
        capture_output=True,
    )
    return [
        entry.decode("utf-8", errors="surrogateescape")
        for entry in result.stdout.split(b"\0")
        if entry
    ]


def is_safe_secret_reference(value: str, key: str) -> bool:
    normalized_key = key.lower().replace("-", "_")
    if normalized_key.endswith(("_file", "_path")):
        return True

    normalized_value = value.strip().strip("\"'")
    return (
        not normalized_value
        or normalized_value in {"null", "~", "unused-in-standalone-mode"}
        or "{{" in normalized_value
        or normalized_value.startswith("!vault")
        or normalized_value.startswith("$ANSIBLE_VAULT;")
    )


def main() -> int:
    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    violations: set[tuple[str, str]] = set()

    for relative in tracked_files(repo_root):
        posix_path = PurePosixPath(relative)
        path = repo_root / relative

        if posix_path.name in LOCAL_ONLY_NAMES:
            violations.add((relative, "local-only instruction file is tracked"))

        if (
            relative.startswith("deploy/ansible/inventories/")
            and not relative.startswith("deploy/ansible/inventories/example/")
        ):
            violations.add((relative, "non-example Ansible inventory is tracked"))

        if (
            ".ssh" in posix_path.parts
            or posix_path.name in SENSITIVE_FILENAMES
            or KEY_FILENAME_RE.fullmatch(posix_path.name)
            or posix_path.suffix.lower() in {".key", ".pem", ".ppk"}
        ):
            violations.add((relative, "SSH or private-key file is tracked"))

        try:
            data = path.read_bytes()
        except OSError as exc:
            violations.add((relative, f"could not inspect tracked file: {exc}"))
            continue

        if PRIVATE_KEY_RE.search(data):
            violations.add((relative, "private-key marker found"))
        if SSH_PUBLIC_KEY_RE.search(data):
            violations.add((relative, "SSH public-key material found"))
        if any(pattern.search(data) for pattern in TOKEN_PATTERNS):
            violations.add((relative, "high-confidence credential marker found"))

        if not relative.startswith("deploy/ansible/") or b"\0" in data:
            continue

        text = data.decode("utf-8", errors="replace")
        for line_number, line in enumerate(text.splitlines(), start=1):
            for candidate in IPV4_RE.findall(line):
                try:
                    address = ipaddress.ip_address(candidate)
                except ValueError:
                    continue
                documented = any(address in network for network in DOCUMENTATION_NETWORKS)
                if address.is_global and not documented:
                    violations.add(
                        (
                            f"{relative}:{line_number}",
                            "globally routable IP address found in Ansible content",
                        )
                    )

            assignment = SENSITIVE_ASSIGNMENT_RE.match(line)
            if assignment and not is_safe_secret_reference(
                assignment.group(2), assignment.group(1)
            ):
                violations.add(
                    (
                        f"{relative}:{line_number}",
                        "literal value assigned to a secret-like Ansible variable",
                    )
                )

    if violations:
        print("Sensitive-file policy violations:", file=sys.stderr)
        for relative, reason in sorted(violations):
            print(f"- {relative}: {reason}", file=sys.stderr)
        return 1

    print("Sensitive-file policy passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
