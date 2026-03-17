#!/usr/bin/env bash
# =============================================================================
# Breach PII Search — Agent Orchestrator (Single Entry Point)
# =============================================================================
# Usage:
#   bash orchestrator.sh status                    # Show work board
#   bash orchestrator.sh plan                      # Output JSON manifest with agent prompts
#   bash orchestrator.sh complete "Phase V2-X.Y"   # Mark phase done, unblock dependents
#   bash orchestrator.sh complete "Phase V3-X.Y"   # Also supports V3 phases
#   bash orchestrator.sh complete "Phase V4-X.Y"   # Also supports V4 phases
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CMD="${1:-help}"
shift 2>/dev/null || true

case "$CMD" in
  status|plan|complete)
    python - "$SCRIPT_DIR" "$CMD" "$@" <<'PYEOF'
from __future__ import annotations
import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(sys.argv[1])
COMMAND = sys.argv[2]
EXTRA_ARGS = sys.argv[3:]

ROADMAP_PATH = PROJECT_ROOT / "plans" / "roadmap.md"
CLAUDE_MD_PATH = PROJECT_ROOT / "CLAUDE.md"
SPECS_BASE_V2 = PROJECT_ROOT / "openspec" / "changes" / "breach-pii-search"
SPECS_BASE_V3 = PROJECT_ROOT / "openspec" / "changes" / "v3-azure-only"
SPECS_BASE_V4 = PROJECT_ROOT / "openspec" / "changes" / "v3-cli-poetry"

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
    status: str = ""
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
    items = []
    for item in re.split(r",\s*(?=`)", text):
        item = item.strip().strip("`")
        if item:
            items.append(item)
    return items


def _parse_spec_refs(text: str) -> list[str]:
    refs = []
    for m in re.finditer(r"`([^`]+)`", text):
        refs.append(m.group(1))
    return refs


def _parse_depends(text: str) -> list[str]:
    v1_matches = re.findall(r"Phase \d+\.\d+", text)
    v2_matches = re.findall(r"(?<!Phase )V2-\d+\.\d+", text)
    v2_full_matches = re.findall(r"Phase V2-\d+\.\d+", text)
    v3_matches = re.findall(r"(?<!Phase )V3-\d+\.\d+", text)
    v3_full_matches = re.findall(r"Phase V3-\d+\.\d+", text)
    v4_matches = re.findall(r"(?<!Phase )V4-\d+\.\d+", text)
    v4_full_matches = re.findall(r"Phase V4-\d+\.\d+", text)
    normalized_v2 = [f"Phase {m}" for m in v2_matches]
    normalized_v3 = [f"Phase {m}" for m in v3_matches]
    normalized_v4 = [f"Phase {m}" for m in v4_matches]
    return v1_matches + v2_full_matches + normalized_v2 + v3_full_matches + normalized_v3 + v4_full_matches + normalized_v4


def parse_roadmap(roadmap_path: Path = ROADMAP_PATH) -> list[Phase]:
    lines = roadmap_path.read_text(encoding="utf-8").splitlines()
    phases: list[Phase] = []
    current_batch = ""
    current_phase: Phase | None = None

    for line in lines:
        m = re.match(r"^## ((?:V[234] )?Batch \d+.*)", line)
        if m:
            if current_phase and current_phase.id:
                phases.append(current_phase)
                current_phase = None
            current_batch = m.group(1)
            continue

        m = re.match(r"^### (Phase (?:V[234]-)?\d+\.\d+):\s*(.*)", line)
        if m:
            if current_phase and current_phase.id:
                phases.append(current_phase)
            current_phase = Phase(id=m.group(1), goal=m.group(2), batch=current_batch)
            continue

        if current_phase is None:
            continue

        if "**Status:**" in line:
            current_phase.status = _parse_status(line.split("**Status:**")[1])
            continue

        if "**Depends On:**" in line:
            current_phase.depends_on = _parse_depends(line.split("**Depends On:**")[1])
            continue

        m = re.match(r"^\s*- \[.\]\s*(.*)", line)
        if m:
            current_phase.tasks.append(m.group(1))
            continue

        if "**Effort:**" in line:
            current_phase.effort = line.split("**Effort:**")[1].strip()
            continue

        if "**Done When:**" in line:
            current_phase.done_when = line.split("**Done When:**")[1].strip()
            continue

        if "**Key Files:**" in line:
            current_phase.key_files = _parse_list(line.split("**Key Files:**")[1])
            continue

        if "**V1 Files Affected:**" in line:
            current_phase.v1_files_affected = _parse_list(line.split("**V1 Files Affected:**")[1])
            continue

        if "**New Files:**" in line:
            current_phase.new_files = _parse_list(line.split("**New Files:**")[1])
            continue

        if "**Spec Reference" in line:
            parts = re.split(r"\*\*Spec References?:\*\*", line)
            if len(parts) > 1:
                current_phase.spec_refs = _parse_spec_refs(parts[1])
            continue

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
    completed_ids = {p.id for p in phases if p.status == "Complete"}
    launchable = []
    blocked = []
    completed = []

    for p in phases:
        if p.status == "Complete":
            completed.append(p)
        elif p.status in ("Ready", "Not Started"):
            unmet = [d for d in p.depends_on if d not in completed_ids]
            if unmet:
                blocked.append(p)
            else:
                launchable.append(p)
        elif p.status == "Blocked":
            blocked.append(p)
        elif p.status == "In Progress":
            pass

    return launchable, blocked, completed


# ---------------------------------------------------------------------------
# Spec / file reader
# ---------------------------------------------------------------------------

def read_spec(ref_path: str) -> str | None:
    full = PROJECT_ROOT / ref_path
    if full.is_file():
        return full.read_text(encoding="utf-8")
    full = SPECS_BASE_V2 / ref_path
    if full.is_file():
        return full.read_text(encoding="utf-8")
    full = SPECS_BASE_V3 / ref_path
    if full.is_file():
        return full.read_text(encoding="utf-8")
    full = SPECS_BASE_V4 / ref_path
    if full.is_file():
        return full.read_text(encoding="utf-8")
    return None


def read_tdd_rules() -> str:
    if CLAUDE_MD_PATH.exists():
        return CLAUDE_MD_PATH.read_text(encoding="utf-8")
    return ""


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def build_prompt(phase: Phase) -> str:
    sections = []

    sections.append(f"You are implementing {phase.id}: {phase.goal} for the breach-search project.")
    sections.append("")
    sections.append("## Project Location")
    sections.append("C:/Users/karth/pwc/breach-search/")

    sections.append("")
    sections.append("## TDD Rules (MANDATORY)")
    sections.append(read_tdd_rules())

    sections.append("")
    sections.append("## Your Tasks")
    for i, task in enumerate(phase.tasks, 1):
        sections.append(f"{i}. {task}")

    if phase.done_when:
        sections.append("")
        sections.append("## Done When")
        sections.append(phase.done_when)

    if phase.key_files:
        sections.append("")
        sections.append("## Key Files to Create/Modify")
        for f in phase.key_files:
            sections.append(f"- `{f}`")

    if phase.v1_files_affected:
        sections.append("")
        sections.append("## V1 Files to Modify/Replace")
        for f in phase.v1_files_affected:
            sections.append(f"- `{f}`")

    if phase.new_files:
        sections.append("")
        sections.append("## New Files to Create")
        for f in phase.new_files:
            sections.append(f"- `{f}`")

    all_files = phase.key_files + phase.v1_files_affected + phase.new_files

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

    sections.append("")
    sections.append("## Test File Locations")
    sections.append("Tests mirror the app/ structure:")
    for kf in all_files:
        if kf.startswith("app/"):
            test_path = kf.replace("app/", "tests/", 1)
            test_path = re.sub(r"/([^/]+)$", r"/test_\1", test_path)
            sections.append(f"- `{kf}` -> `{test_path}`")
        elif kf.startswith("scripts/"):
            stem = Path(kf).stem
            sections.append(f"- `{kf}` -> `tests/test_{stem}.py`")
        elif kf.startswith("tests/"):
            sections.append(f"- `{kf}` (test file — create directly)")

    sections.append("")
    sections.append("## Environment Notes")
    sections.append("- Python 3.12+, Windows 11, Git Bash")
    sections.append("- Run tests: `cd C:/Users/karth/pwc/breach-search && python -m pytest <test_file> -v --tb=short`")
    sections.append("- CRITICAL: `import sqlalchemy` may HANG in the current session. Do NOT import sqlalchemy or run tests that trigger sqlalchemy imports. Mock all DB access in unit tests.")
    sections.append("- Create `__init__.py` files in test subdirectories if they don't exist")
    sections.append("- Use `import logging; logger = logging.getLogger(__name__)` for logging")
    sections.append("- If pytest hangs for more than 30 seconds, STOP retrying. Just verify code is written correctly and move on.")

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
    print()


def cmd_status() -> None:
    phases = parse_roadmap()
    launchable, blocked, completed = resolve_launchable(phases)

    print()
    print(f"{BOLD}================================================================={RESET}")
    print(f"{BOLD}  BREACH PII SEARCH -- Agent Work Board{RESET}")
    print(f"{BOLD}================================================================={RESET}")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    if completed:
        print(f"{BOLD}{GREEN}== Completed ({len(completed)}) =={RESET}")
        for p in completed:
            print(f"  {GREEN}[done]{RESET} {p.id}: {p.goal}")
        print()

    if launchable:
        print(f"{BOLD}{CYAN}== Ready to Launch ({len(launchable)}) =={RESET}")
        for p in launchable:
            print(f"  {CYAN}[ready]{RESET} {p.id}: {p.goal}  ({p.effort})")
            all_files = p.key_files + p.v1_files_affected + p.new_files
            if all_files:
                print(f"         Files: {', '.join(all_files[:5])}")
        print()

    if blocked:
        print(f"{BOLD}{RED}== Blocked ({len(blocked)}) =={RESET}")
        completed_ids = {c.id for c in completed}
        for p in blocked:
            unmet = [d for d in p.depends_on if d not in completed_ids]
            print(f"  {RED}[blocked]{RESET} {p.id}: {p.goal}")
            print(f"           Waiting on: {', '.join(unmet)}")
        print()

    print(f"{BOLD}================================================================={RESET}")
    print(f"  {len(completed)} complete | {len(launchable)} ready | {len(blocked)} blocked")
    print(f"{BOLD}================================================================={RESET}")
    print()
    if launchable:
        print(f"  Run: {BOLD}bash orchestrator.sh plan{RESET}")
        print(f"  to generate agent prompts for all {len(launchable)} ready phase(s).")
        print()


def cmd_complete(phase_id: str) -> None:
    text = ROADMAP_PATH.read_text(encoding="utf-8")
    lines = text.splitlines()

    found = False
    phase_line_idx = -1
    for i, line in enumerate(lines):
        m = re.match(r"^### (Phase (?:V[234]-)?\d+\.\d+):", line)
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
            found = False
            break

    if phase_line_idx == -1:
        print(f"ERROR: {phase_id} not found in roadmap", file=sys.stderr)
        sys.exit(1)

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

    if unblocked:
        text = ROADMAP_PATH.read_text(encoding="utf-8")
        for uid in unblocked:
            pattern = re.compile(
                rf"(### {re.escape(uid)}:.*\n(?:.*\n)*?- \*\*Status:\*\*) [^\n]*",
            )
            text = pattern.sub(rf"\1 🟢 Ready", text)
        ROADMAP_PATH.write_text(text, encoding="utf-8")

    print(json.dumps({
        "completed": phase_id,
        "newly_unblocked": unblocked,
    }, indent=2))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if COMMAND == "plan":
    cmd_plan()
elif COMMAND == "status":
    cmd_status()
elif COMMAND == "complete":
    if not EXTRA_ARGS:
        print("ERROR: missing phase_id argument", file=sys.stderr)
        print('Usage: bash orchestrator.sh complete "Phase V2-X.Y"  (or V3-X.Y, V4-X.Y)', file=sys.stderr)
        sys.exit(1)
    cmd_complete(EXTRA_ARGS[0])
else:
    print(f"Unknown command: {COMMAND}", file=sys.stderr)
    sys.exit(1)
PYEOF
    ;;
  help|*)
    echo "Usage: bash orchestrator.sh {status|plan|complete \"Phase V2-X.Y|V3-X.Y|V4-X.Y\"}"
    echo ""
    echo "Commands:"
    echo "  status                            Show the agent work board"
    echo "  plan                              Output JSON manifest of launchable phases with prompts"
    echo "  complete \"Phase V2-X.Y\"           Mark a V2 phase complete and unblock dependents"
    echo "  complete \"Phase V3-X.Y\"           Mark a V3 phase complete and unblock dependents"
    echo "  complete \"Phase V4-X.Y\"           Mark a V4 phase complete and unblock dependents"
    ;;
esac
