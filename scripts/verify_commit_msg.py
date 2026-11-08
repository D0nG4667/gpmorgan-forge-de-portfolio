"""Commit message and repository standards verification utility for GPMorgan Forge."""

from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

MAX_SUBJECT_LENGTH: Final[int] = 72
MIN_SUMMARY_LENGTH: Final[int] = 4

ALLOWED_COMMIT_TYPES: Final[tuple[str, ...]] = (
    "feat",
    "fix",
    "docs",
    "perf",
    "refactor",
    "test",
    "build",
    "ci",
    "chore",
    "revert",
)

ALLOWED_COMMIT_SCOPES: Final[tuple[str, ...]] = (
    "core",
    "ledger",
    "optimiser",
    "concurrency",
    "nosql",
    "graph",
    "distributed",
    "security",
    "privacy",
    "portfolio-1",
    "portfolio-2",
    "portfolio-3",
    "portfolio-4",
    "portfolio-5",
    "docker",
    "ci",
    "lint",
    "docs",
    "tests",
    "deps",
    "repo",
    "ops",
    "release",
)

HEADER_STRUCTURE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^(?P<type>[a-z]+)"
    r"(?:\((?P<scope>[a-z0-9-]+(?:/[a-z0-9-]+)*)\))?"
    r"(?P<breaking>!)?: "
    r"(?P<summary>.+)$"
)

CONVENTIONAL_COMMIT_PATTERN: Final[re.Pattern[str]] = re.compile(
    rf"^(?P<type>{'|'.join(ALLOWED_COMMIT_TYPES)})"
    rf"\((?P<scope>{'|'.join(ALLOWED_COMMIT_SCOPES)})(?:/[a-z0-9-]+)*\)"
    r"(?P<breaking>!)?: "
    r"(?P<summary>[a-z0-9].{3,70})$"
)

MERGE_COMMIT_PATTERN: Final[re.Pattern[str]] = re.compile(r"^Merge (branch|pull request) .+")

DISALLOWED_TERM: Final[str] = "enter" + "prise"

MARKDOWN_LINK_PATTERN: Final[re.Pattern[str]] = re.compile(r"(?<!!)\[([^\]]+)\]\(([^)]+)\)")

SCAN_EXTENSIONS: Final[frozenset[str]] = frozenset(
    {".py", ".sql", ".cql", ".cypher", ".js", ".md", ".yml", ".yaml", ".toml", ".conf"}
)


def discover_repository_files(root: Path) -> list[Path]:
    """Return tracked and untracked non-ignored files using Git as the single source of truth.

    Delegates ignore evaluation to `git ls-files --cached --others --exclude-standard`, which
    natively honours `.gitignore`, `.git/info/exclude`, and global Git exclude configurations
    without duplicating ignore rules in code. Falls back to filesystem traversal only when
    invoked outside a Git worktree (such as inside a minimal container runtime).
    """
    if (root / ".git").exists():
        result = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "ls-files",
                "-z",
                "--cached",
                "--others",
                "--exclude-standard",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )
        if result.returncode == 0:
            return [
                root / rel_entry
                for rel_entry in result.stdout.split("\0")
                if rel_entry and (root / rel_entry).is_file()
            ]

    return [
        path
        for path in root.rglob("*")
        if path.is_file()
        and "__pycache__" not in path.parts
        and not any(
            part.startswith(".") and part != ".github" for part in path.relative_to(root).parts[:-1]
        )
    ]


@dataclass(frozen=True, slots=True)
class CommitHeader:
    """Immutable value object representing a parsed Conventional Commit header."""

    commit_type: str
    scope: str
    is_breaking: bool
    summary: str


def parse_commit_header(subject: str) -> tuple[CommitHeader | None, str]:
    """Parse and validate a single Conventional Commit subject line."""
    if len(subject) > MAX_SUBJECT_LENGTH:
        return (
            None,
            f"Commit subject exceeds {MAX_SUBJECT_LENGTH} characters "
            f"(got {len(subject)} characters).",
        )

    match = HEADER_STRUCTURE_PATTERN.match(subject)
    if match is None:
        return (
            None,
            "Invalid commit subject structure. Expected format: "
            "'<type>(<scope>): <imperative summary>'.",
        )

    commit_type = match.group("type")
    scope = match.group("scope")
    is_breaking = match.group("breaking") == "!"
    summary = match.group("summary")

    if commit_type not in ALLOWED_COMMIT_TYPES:
        allowed = ", ".join(ALLOWED_COMMIT_TYPES)
        return None, f"Invalid commit type '{commit_type}'. Allowed types: {allowed}."

    if not scope:
        return None, "Commit scope is required: '<type>(<scope>): <imperative summary>'."

    root_scope = scope.split("/", 1)[0]
    if root_scope not in ALLOWED_COMMIT_SCOPES:
        allowed_scopes = ", ".join(ALLOWED_COMMIT_SCOPES)
        return None, f"Invalid commit scope '{scope}'. Allowed scopes: {allowed_scopes}."

    if not (summary[0].islower() or summary[0].isdigit()):
        return None, "Commit summary must start with a lowercase letter or digit after ': '."

    if len(summary) < MIN_SUMMARY_LENGTH:
        return (
            None,
            f"Commit summary must be at least {MIN_SUMMARY_LENGTH} characters long.",
        )

    if summary.endswith("."):
        return None, "Commit subject must not end with a period."

    return (
        CommitHeader(
            commit_type=commit_type,
            scope=scope,
            is_breaking=is_breaking,
            summary=summary,
        ),
        "Valid Conventional Commit message.",
    )


def validate_commit_message(message: str) -> tuple[bool, str]:
    """Validate that a commit message conforms to Conventional Commits and terminology rules."""
    raw_lines = [
        line.rstrip() for line in message.strip().splitlines() if not line.lstrip().startswith("#")
    ]
    if not raw_lines or not raw_lines[0].strip():
        return False, "Commit message cannot be empty."

    if DISALLOWED_TERM in message.lower():
        return False, f"Commit message must not contain the restricted term '{DISALLOWED_TERM}'."

    subject = raw_lines[0].strip()
    if MERGE_COMMIT_PATTERN.match(subject):
        return True, "Valid merge commit message."

    header, reason = parse_commit_header(subject)
    if header is None:
        return False, reason

    if len(raw_lines) > 1 and raw_lines[1].strip() != "":
        return False, "Multi-line commit message must have a blank line between subject and body."

    return True, reason


def audit_repository(root: Path) -> tuple[bool, list[str]]:
    """Scan project files to ensure no disallowed terminology or legacy compose files exist."""
    violations: list[str] = []

    for legacy_compose in ("docker-compose.yml", "docker-compose.yaml"):
        if (root / legacy_compose).exists():
            violations.append(
                f"Legacy '{legacy_compose}' found; use modern 'compose.yaml' instead."
            )

    for path in discover_repository_files(root):
        if path.suffix.lower() not in SCAN_EXTENSIONS and path.name != "Dockerfile":
            continue

        text = path.read_text(encoding="utf-8", errors="ignore")
        rel_path = path.relative_to(root)
        if DISALLOWED_TERM in text.lower():
            violations.append(f"Restricted term found in {rel_path}")

        if path.suffix.lower() == ".md":
            if "file:///" in text.lower():
                violations.append(f"Non-portable file:/// URI found in {rel_path}")
            prose_text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
            for match in MARKDOWN_LINK_PATTERN.finditer(prose_text):
                label, target = match.group(1), match.group(2).strip()
                if ".." in label or "\\" in label:
                    violations.append(
                        f"Unclean link label '[{label}]' in {rel_path}; "
                        "use clean relative or basename label."
                    )
                if target.startswith(("http://", "https://", "mailto:", "#")):
                    continue
                clean_target = target.split("#", 1)[0].strip()
                if not clean_target:
                    continue
                if "\\" in clean_target or clean_target.startswith("/"):
                    violations.append(
                        f"Non-relative or backslash link target '({target})' in {rel_path}"
                    )
                    continue
                resolved = (path.parent / clean_target).resolve()
                if not resolved.exists():
                    violations.append(f"Broken relative link '({target})' in {rel_path}")

    return len(violations) == 0, violations


def main(argv: list[str] | None = None) -> int:
    """Entry point for CLI and Lefthook execution."""
    args = argv if argv is not None else sys.argv[1:]
    repo_root = Path(__file__).resolve().parent.parent

    if "--audit-repo" in args:
        ok, issues = audit_repository(repo_root)
        if not ok:
            for issue in issues:
                print(f"[AUDIT FAILED] {issue}", file=sys.stderr)
            return 1
        print("[AUDIT PASSED] Repository standards verified.")
        return 0

    if not args:
        print("Usage: verify_commit_msg.py <commit-msg-file> | --audit-repo", file=sys.stderr)
        return 1

    msg_path = Path(args[0])
    content = msg_path.read_text(encoding="utf-8") if msg_path.is_file() else args[0]
    valid, reason = validate_commit_message(content)
    if not valid:
        print(f"[COMMIT MSG REJECTED] {reason}", file=sys.stderr)
        return 1

    print(f"[COMMIT MSG OK] {reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
