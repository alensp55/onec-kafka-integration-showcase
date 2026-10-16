from __future__ import annotations

import argparse
import ipaddress
import re
from pathlib import Path


BINARY_SUFFIXES = {
    ".cf",
    ".cfe",
    ".dll",
    ".dt",
    ".epf",
    ".erf",
    ".key",
    ".pem",
    ".pfx",
    ".p12",
    ".so",
    ".dylib",
}

TEXT_PATTERNS = {
    "email": re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE),
    "home path": re.compile(r"(?i)(?:[a-z]:\\users\\|/(?:home|users)/)[^/\\\s]+"),
    "uuid": re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b", re.I),
    "credential": re.compile(
        r"(?i)(?:password|passwd|secret|api[_-]?key|access[_-]?token)\s*[:=]\s*[\"']?[^\s\"'{}]{6,}"
    ),
}


def iter_files(root: Path):
    for path in root.rglob("*"):
        if not path.is_file() or ".git" in path.parts or "__pycache__" in path.parts:
            continue
        yield path


def private_ipv4_matches(text: str):
    for match in re.finditer(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])", text):
        try:
            address = ipaddress.ip_address(match.group())
        except ValueError:
            continue
        if address.is_private or address.is_loopback or address.is_link_local:
            yield match.group()


def scan(root: Path) -> list[str]:
    findings: list[str] = []
    scanner = Path(__file__).resolve()

    for path in iter_files(root):
        relative = path.relative_to(root)
        if path.suffix.lower() in BINARY_SUFFIXES:
            findings.append(f"forbidden file: {relative}")
            continue

        if path.resolve() == scanner:
            continue

        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            findings.append(f"non-UTF-8 or unreadable file: {relative}")
            continue

        for label, pattern in TEXT_PATTERNS.items():
            if pattern.search(text):
                findings.append(f"{label}: {relative}")

        if next(private_ipv4_matches(text), None) is not None:
            findings.append(f"private network address: {relative}")

    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Check the repository for private data and binary artifacts.")
    parser.add_argument("root", nargs="?", default=".")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    findings = scan(root)

    if findings:
        for finding in findings:
            print(finding)
        return 1

    print("Safety audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
