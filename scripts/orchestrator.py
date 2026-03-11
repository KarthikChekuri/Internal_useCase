#!/usr/bin/env python3
"""
Breach PII Search — Agent Orchestrator

Commands:
    python scripts/orchestrator.py plan       Output JSON manifest of launchable phases with full agent prompts
    python scripts/orchestrator.py status     Human-readable work board (replaces orchestrator.sh)
    python scripts/orchestrator.py complete "Phase X.Y"   Mark phase done, unblock dependents
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ROADMAP_PATH = PROJECT_ROOT / "plans" / "roadmap.md"
CLAUDE_MD_PATH = PROJECT_ROOT / "CLAUDE.md"
SPECS_BASE = PROJECT_ROOT / "openspec" / "changes" / "breach-pii-search"

# ANSI colors
BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[0;32m"
YELLOW = "\033[0;33m"
RED = "\033[0;31m"
CYAN = "\033[0;36m"
WHITE = "\033[0;37m"
RESET = "\033[0m"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Phase:
    id: str = ""
    goal: str = ""
    batch: str = ""
    status: str = ""           # Complete, Ready, Not Started, Blocked, In Progress
    depends_on: list[str] = field(default_factory=list)
    tasks: list[str] = field(default_factory=list)
    effort: str = ""
    done_when: str = ""
    key_files: list[str] = field(default_factory=list)
    v1_files_affected: list[str] = field(default_factory=list)
    new_files: list[str] = field(default_factory=list)
    spec_refs: list[str] = field(default_factory=list)
    tests_info: str = ""


# ---------------------------------------------------------------------------
# Roadmap parser
# ---------------------------------------------------------------------------

def _parse_status(text: str) -> str:
    """Extract canonical status from a roadmap status line."""
    t = text.lower()
    if "complete" in t:
        return "Complete"
    if "ready" in t:
        return "Ready"
    if "in progress" in t:
        return "In Progress"
    if "blocked" in t:
        return "Blocked"
    if "not started" in t:
        return "Not Started"
    return text.strip()


def _parse_list(text: str) -> list[str]:
    """Split a comma-separated markdown field into a list of trimmed strings."""
    items = []
    for item in re.split(r",\s*(?=`)", text):
        item = item.strip().strip("`")
        if item:
            items.append(item)
    return items


def _parse_spec_refs(text: str) -> list[str]:
    """Extract file paths from spec reference strings like '`path` (note), `path2` (note2)'."""
    refs = []
    for m in re.finditer(r"`([^`]+)`", text):
        refs.append(m.group(1))
    return refs


def _parse_depends(text: str) -> list[str]:
    """Extract phase IDs from depends-on text.

    Supports formats:
    - V1: 'Phase 1.1', 'Phase 2.3'
    - V2: 'V2-1.1', 'V2-2.3' (no 'Phase' prefix)
    - V2 full: 'Phase V2-1.1'
    All are normalized to 'Phase X.Y' or 'Phase V2-X.Y' for consistency.
    """
    # First try V1 format: "Phase X.Y"
    v1_matches = re.findall(r"Phase \d+\.\d+", text)
    # Then try V2 short format: "V2-X.Y" (not preceded by "Phase ")
    v2_matches = re.findall(r"(?<!Phase )V2-\d+\.\d+", text)
    # Also try V2 full format: "Phase V2-X.Y"
    v2_full_matches = re.findall(r"Phase V2-\d+\.\d+", text)
    # Normalize V2 short to "Phase V2-X.Y"
    normalized_v2 = [f"Phase {m}" for m in v2_matches]
    return v1_matches + v2_full_matches + normalized_v2


def parse_roadmap(roadmap_path: Path = ROADMAP_PATH) -> list[Phase]:
    """Parse roadmap.md into a list of Phase objects."""
    lines = roadmap_path.read_text(encoding="utf-8").splitlines()
    phases: list[Phase] = []
    current_batch = ""
    current_phase: Phase | None = None

    for line in lines:
        # Batch header: ## Batch N ... or ## V2 Batch N ...
        m = re.match(r"^## ((?:V2 )?Batch \d+.*)", line)
        if m:
            if current_phase and current_phase.id:
                phases.append(current_phase)
                current_phase = None
            current_batch = m.group(1)
            continue

        # Phase header: ### Phase X.Y: Goal  or  ### Phase V2-X.Y: Goal
        m = re.match(r"^### (Phase (?:V2-)?\d+\.\d+):\s*(.*)", line)
        if m:
            if current_phase and current_phase.id:
                phases.append(current_phase)
            current_phase = Phase(id=m.group(1), goal=m.group(2), batch=current_batch)
            continue

        if current_phase is None:
            continue

        # Status
        if "**Status:**" in line:
            current_phase.status = _parse_status(line.split("**Status:**")[1])
            continue

        # Depends On
        if "**Depends On:**" in line:
            current_phase.depends_on = _parse_depends(line.split("**Depends On:**")[1])
            continue

        # Tasks (checkbox lines)
        m = re.match(r"^\s*- \[.\]\s*(.*)", line)
        if m:
            current_phase.tasks.append(m.group(1))
            continue

        # Effort
        if "**Effort:**" in line:
            current_phase.effort = line.split("**Effort:**")[1].strip()
            continue

        # Done When
        if "**Done When:**" in line:
            current_phase.done_when = line.split("**Done When:**")[1].strip()
            continue

        # Key Files (V1 format)
        if "**Key Files:**" in line:
            current_phase.key_files = _parse_list(line.split("**Key Files:**")[1])
            continue

        # V1 Files Affected (V2 format)
        if "**V1 Files Affected:**" in line:
            current_phase.v1_files_affected = _parse_list(line.split("**V1 Files Affected:**")[1])
            continue

        # New Files (V2 format)
        if "**New Files:**" in line:
            current_phase.new_files = _parse_list(line.split("**New Files:**")[1])
            continue

        # Spec References (V1 format) or Spec Reference (V2 format)
        if "**Spec Reference" in line:
            # Handles both "**Spec References:**" and "**Spec Reference:**"
            parts = re.split(r"\*\*Spec References?:\*\*", line)
            if len(parts) > 1:
                current_phase.spec_refs = _parse_spec_refs(parts[1])
            continue

        # Tests info
        if "**Tests:**" in line:
            current_phase.tests_info = line.split("**Tests:**")[1].strip()
            continue

    if current_phase and current_phase.id:
        phases.append(current_phase)

    return phases


# ---------------------------------------------------------------------------
# Dependency resolution
# ---------------------------------------------------------------------------

def resolve_launchable(phases: list[Phase]) -> tuple[list[Phase], list[Phase], list[Phase]]:
    """Split phases into (launchable, blocked, completed)."""
    completed_ids = {p.id for p in phases if p.status == "Complete"}
    launchable = []
    blocked = []
    completed = []

    for p in phases:
        if p.status == "Complete":
            completed.append(p)
        elif p.status in ("Ready", "Not Started"):
            # Verify all deps are actually complete
            unmet = [d for d in p.depends_on if d not in completed_ids]
            if unmet:
                blocked.append(p)
            else:
                launchable.append(p)
        elif p.status == "Blocked":
            blocked.append(p)
        elif p.status == "In Progress":
            # Don't re-launch in-progress phases
            pass

    return launchable, blocked, completed


# ---------------------------------------------------------------------------
# Spec / file reader
# ---------------------------------------------------------------------------

def read_spec(ref_path: str) -> str | None:
    """Read a spec or design file, return its content or None."""
    # Try relative to project root first
    full = PROJECT_ROOT / ref_path
    if full.exists():
        return full.read_text(encoding="utf-8")
    # Try relative to specs base
    full = SPECS_BASE / ref_path
    if full.exists():
        return full.read_text(encoding="utf-8")
    return None


def read_tdd_rules() -> str:
    """Read CLAUDE.md for TDD rules."""
    if CLAUDE_MD_PATH.exists():
        return CLAUDE_MD_PATH.read_text(encoding="utf-8")
    return ""


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def build_prompt(phase: Phase) -> str:
    """Generate a complete agent prompt for a phase."""
    sections = []

    # Header
    sections.append(f"You are implementing {phase.id}: {phase.goal} for the breach-search project.")
    sections.append("")
    sections.append("## Project Location")
    sections.append("C:/Users/karth/pwc/breach-search/")

    # TDD rules
    sections.append("")
    sections.append("## TDD Rules (MANDATORY)")
    sections.append(read_tdd_rules())

    # Tasks
    sections.append("")
    sections.append("## Your Tasks")
    for i, task in enumerate(phase.tasks, 1):
        sections.append(f"{i}. {task}")

    # Done when
    if phase.done_when:
        sections.append("")
        sections.append("## Done When")
        sections.append(phase.done_when)

    # Key files (V1 format)
    if phase.key_files:
        sections.append("")
        sections.append("## Key Files to Create/Modify")
        for f in phase.key_files:
            sections.append(f"- `{f}`")

    # V1 files affected (V2 format)
    if phase.v1_files_affected:
        sections.append("")
        sections.append("## V1 Files to Modify/Replace")
        for f in phase.v1_files_affected:
            sections.append(f"- `{f}`")

    # New files (V2 format)
    if phase.new_files:
        sections.append("")
        sections.append("## New Files to Create")
        for f in phase.new_files:
            sections.append(f"- `{f}`")

    # Combine all files for test path generation
    all_files = phase.key_files + phase.v1_files_affected + phase.new_files

    # Specs — include full content inline
    if phase.spec_refs:
        sections.append("")
        sections.append("## Specifications")
        for ref in phase.spec_refs:
            content = read_spec(ref)
            if content:
                sections.append(f"### From `{ref}`:")
                sections.append(content)
            else:
                sections.append(f"### `{ref}` (file not found — read it manually)")

    # Test file locations
    sections.append("")
    sections.append("## Test File Locations")
    sections.append("Tests mirror the app/ structure:")
    for kf in all_files:
        if kf.startswith("app/"):
            test_path = kf.replace("app/", "tests/", 1)
            test_path = re.sub(r"/([^/]+)$", r"/test_\1", test_path)
            sections.append(f"- `{kf}` -> `{test_path}`")
        elif kf.startswith("scripts/"):
            sections.append(f"- `{kf}` -> `tests/test_{Path(kf).stem}.py`")
        elif kf.startswith("tests/"):
            sections.append(f"- `{kf}` (test file — create directly)")

    # Environment notes
    sections.append("")
    sections.append("## Environment Notes")
    sections.append("- Python 3.12+, Windows 11, Git Bash")
    sections.append("- Run tests: `cd C:/Users/karth/pwc/breach-search && python -m pytest <test_file> -v --tb=short`")
    sections.append("- CRITICAL: `import sqlalchemy` may HANG in the current session. Do NOT import sqlalchemy or run tests that trigger sqlalchemy imports. Mock all DB access in unit tests.")
    sections.append("- Create `__init__.py` files in test subdirectories if they don't exist")
    sections.append("- Use `import logging; logger = logging.getLogger(__name__)` for logging")
    sections.append("- If pytest hangs for more than 30 seconds, STOP retrying. Just verify code is written correctly and move on.")

    # Workflow
    sections.append("")
    sections.append("## Workflow")
    sections.append("1. Read the spec scenarios above — each Given/When/Then = one or more test cases")
    sections.append("2. Create `__init__.py` in any missing test subdirectories")
    sections.append("3. Write ALL failing tests first (Red phase)")
    sections.append("4. Implement the minimum production code to make tests pass (Green phase)")
    sections.append("5. Run pytest to verify — if it hangs, skip and move on")
    sections.append("6. Refactor if needed, keeping tests green")

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_plan() -> None:
    """Output JSON manifest of launchable phases."""
    phases = parse_roadmap()
    launchable, blocked, completed = resolve_launchable(phases)

    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "launchable_phases": [
            {
                "phase_id": p.id,
                "goal": p.goal,
                "batch": p.batch,
                "effort": p.effort,
                "key_files": p.key_files,
                "v1_files_affected": p.v1_files_affected,
                "new_files": p.new_files,
                "spec_files": p.spec_refs,
                "prompt": build_prompt(p),
            }
            for p in launchable
        ],
        "blocked_phases": [
            {
                "phase_id": p.id,
                "goal": p.goal,
                "blocked_by": p.depends_on,
                "unmet": [d for d in p.depends_on if d not in {c.id for c in completed}],
            }
            for p in blocked
        ],
        "completed_phases": [p.id for p in completed],
    }

    json.dump(manifest, sys.stdout, indent=2)
    print()  # trailing newline


def cmd_status() -> None:
    """Human-readable work board."""
    phases = parse_roadmap()
    launchable, blocked, completed = resolve_launchable(phases)

    print()
    print(f"{BOLD}================================================================={RESET}")
    print(f"{BOLD}  BREACH PII SEARCH -- Agent Work Board{RESET}")
    print(f"{BOLD}================================================================={RESET}")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Completed
    if completed:
        print(f"{BOLD}{GREEN}== Completed ({len(completed)}) =={RESET}")
        for p in completed:
            print(f"  {GREEN}[done]{RESET} {p.id}: {p.goal}")
        print()

    # Launchable
    if launchable:
        print(f"{BOLD}{CYAN}== Ready to Launch ({len(launchable)}) =={RESET}")
        for p in launchable:
            print(f"  {CYAN}[ready]{RESET} {p.id}: {p.goal}  ({p.effort})")
            if p.key_files:
                print(f"         Key files: {', '.join(p.key_files)}")
        print()

    # Blocked
    if blocked:
        print(f"{BOLD}{RED}== Blocked ({len(blocked)}) =={RESET}")
        completed_ids = {c.id for c in completed}
        for p in blocked:
            unmet = [d for d in p.depends_on if d not in completed_ids]
            print(f"  {RED}[blocked]{RESET} {p.id}: {p.goal}")
            print(f"           Waiting on: {', '.join(unmet)}")
        print()

    # Summary
    print(f"{BOLD}================================================================={RESET}")
    print(f"  {len(completed)} complete | {len(launchable)} ready | {len(blocked)} blocked")
    print(f"{BOLD}================================================================={RESET}")
    print()
    if launchable:
        print(f"  Run: {BOLD}python scripts/orchestrator.py plan{RESET}")
        print(f"  to generate agent prompts for all {len(launchable)} ready phase(s).")
        print()


def cmd_complete(phase_id: str) -> None:
    """Mark a phase complete and unblock dependents."""
    text = ROADMAP_PATH.read_text(encoding="utf-8")
    lines = text.splitlines()

    # Find the phase and update its status
    found = False
    phase_line_idx = -1
    for i, line in enumerate(lines):
        m = re.match(r"^### (Phase (?:V2-)?\d+\.\d+):", line)
        if m and m.group(1) == phase_id:
            phase_line_idx = i
            found = True
            continue

        if found and "**Status:**" in line:
            lines[i] = re.sub(
                r"\*\*Status:\*\*.*",
                "**Status:** :white_check_mark: Complete",
                line,
            )
            found = False  # done with this phase
            break

    if phase_line_idx == -1:
        print(f"ERROR: {phase_id} not found in roadmap", file=sys.stderr)
        sys.exit(1)

    # Re-parse to check what's now unblockable
    temp_text = "\n".join(lines)
    ROADMAP_PATH.write_text(temp_text, encoding="utf-8")

    phases = parse_roadmap()
    completed_ids = {p.id for p in phases if p.status == "Complete"}
    unblocked = []

    for p in phases:
        if p.status == "Blocked":
            unmet = [d for d in p.depends_on if d not in completed_ids]
            if not unmet:
                unblocked.append(p.id)

    # Update unblocked phases to Ready
    if unblocked:
        text = ROADMAP_PATH.read_text(encoding="utf-8")
        for uid in unblocked:
            # Find the phase header, then its status line
            pattern = re.compile(
                rf"(### {re.escape(uid)}:.*\n(?:.*\n)*?- \*\*Status:\*\*) [^\n]*",
            )
            text = pattern.sub(rf"\1 🟢 Ready", text)
        ROADMAP_PATH.write_text(text, encoding="utf-8")

    # Report
    print(json.dumps({
        "completed": phase_id,
        "newly_unblocked": unblocked,
    }, indent=2))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Breach PII Search — Agent Orchestrator")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("plan", help="Output JSON manifest of launchable phases")
    sub.add_parser("status", help="Human-readable work board")

    p_complete = sub.add_parser("complete", help="Mark a phase complete")
    p_complete.add_argument("phase_id", help='Phase ID, e.g. "Phase 2.1"')

    args = parser.parse_args()

    if args.command == "plan":
        cmd_plan()
    elif args.command == "status":
        cmd_status()
    elif args.command == "complete":
        cmd_complete(args.phase_id)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
