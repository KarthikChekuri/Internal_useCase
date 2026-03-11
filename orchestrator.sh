#!/usr/bin/env bash
# =============================================================================
# Breach PII Search -- Agent Orchestrator
# =============================================================================
# Run this script to see all available and in-progress work.
# Agents use this to discover what to pick up next.
#
# Usage:  bash orchestrator.sh
#         bash orchestrator.sh --batch 2        # Show only Batch 2
#         bash orchestrator.sh --available      # Show only unclaimed phases
#         bash orchestrator.sh --in-progress    # Show only in-progress phases
# =============================================================================

set -euo pipefail

# Resolve paths relative to this script's location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROADMAP="$SCRIPT_DIR/plans/roadmap.md"

# --- Colors (safe for Git Bash / WSL / Linux) ---
BOLD='\033[1m'
DIM='\033[2m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
WHITE='\033[0;37m'
RESET='\033[0m'

# --- Args ---
FILTER_BATCH=""
FILTER_STATUS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --batch)
      FILTER_BATCH="$2"
      shift 2
      ;;
    --available)
      FILTER_STATUS="not_started"
      shift
      ;;
    --in-progress)
      FILTER_STATUS="in_progress"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# --- Header ---
echo ""
echo -e "${BOLD}=================================================================${RESET}"
echo -e "${BOLD}  BREACH PII SEARCH -- Agent Work Board${RESET}"
echo -e "${BOLD}=================================================================${RESET}"
echo ""
echo -e "  Project:    breach-search"
echo -e "  Roadmap:    $ROADMAP"
echo -e "  Generated:  $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

if [[ ! -f "$ROADMAP" ]]; then
  echo -e "${RED}ERROR: Roadmap not found at $ROADMAP${RESET}"
  exit 1
fi

# =============================================================================
# Parse roadmap.md and extract phase blocks
# =============================================================================
# Strategy: read line by line, detect batch headers and phase headers,
# collect task lines, status, done-when, key files, spec references.
# =============================================================================

current_batch=""
current_batch_note=""
in_phase=0
phase_id=""
phase_goal=""
phase_status=""
phase_status_icon=""
phase_effort=""
phase_depends=""
phase_done_when=""
phase_key_files=""
phase_spec_refs=""
phase_tasks=""
phase_count=0
shown_count=0

print_phase() {
  if [[ -z "$phase_id" ]]; then
    return
  fi

  # Apply filters
  if [[ -n "$FILTER_BATCH" ]]; then
    local batch_num
    batch_num=$(echo "$current_batch" | sed -n 's/^Batch \([0-9]*\).*/\1/p')
    if [[ "$batch_num" != "$FILTER_BATCH" ]]; then
      return
    fi
  fi

  if [[ "$FILTER_STATUS" == "not_started" && "$phase_status_icon" != "⚪" ]]; then
    return
  fi
  if [[ "$FILTER_STATUS" == "in_progress" && "$phase_status_icon" != "🟡" ]]; then
    return
  fi

  # Skip completed phases (they should not be in roadmap, but just in case)
  if [[ "$phase_status_icon" == "🟢" ]]; then
    return
  fi

  # Pick color based on status
  local status_color="$WHITE"
  case "$phase_status_icon" in
    "⚪") status_color="$WHITE" ;;
    "🟡") status_color="$YELLOW" ;;
    "🔴") status_color="$RED" ;;
  esac

  shown_count=$((shown_count + 1))

  echo -e "${BOLD}${CYAN}--- $phase_id: $phase_goal ---${RESET}"
  echo -e "  ${BOLD}Status:${RESET}    ${status_color}${phase_status}${RESET}"
  if [[ -n "$phase_effort" ]]; then
    echo -e "  ${BOLD}Effort:${RESET}    $phase_effort"
  fi
  if [[ -n "$phase_depends" ]]; then
    echo -e "  ${BOLD}Depends:${RESET}   $phase_depends"
  fi
  echo ""

  # Print tasks
  if [[ -n "$phase_tasks" ]]; then
    echo -e "  ${BOLD}Tasks:${RESET}"
    echo "$phase_tasks" | while IFS= read -r task_line; do
      if [[ -n "$task_line" ]]; then
        echo "    $task_line"
      fi
    done
    echo ""
  fi

  # Done When
  if [[ -n "$phase_done_when" ]]; then
    echo -e "  ${BOLD}Done When:${RESET} $phase_done_when"
  fi

  # Key Files
  if [[ -n "$phase_key_files" ]]; then
    echo -e "  ${BOLD}Key Files:${RESET} $phase_key_files"
  fi

  # Spec References
  if [[ -n "$phase_spec_refs" ]]; then
    echo -e "  ${BOLD}Specs:${RESET}     $phase_spec_refs"
  fi

  echo ""
}

reset_phase() {
  phase_id=""
  phase_goal=""
  phase_status=""
  phase_status_icon=""
  phase_effort=""
  phase_depends=""
  phase_done_when=""
  phase_key_files=""
  phase_spec_refs=""
  phase_tasks=""
}

last_batch_printed=""

while IFS= read -r line; do
  # Detect batch header: ## Batch N ... or ## V2 Batch N ...
  if [[ "$line" =~ ^##\ (V2\ )?Batch ]]; then
    # Print previous phase if any
    print_phase
    reset_phase

    current_batch=$(echo "$line" | sed 's/^## //')
    current_batch_note=""

    # Print batch header
    local_batch_num=$(echo "$current_batch" | sed -n 's/^Batch \([0-9]*\).*/\1/p')

    if [[ -n "$FILTER_BATCH" && "$local_batch_num" != "$FILTER_BATCH" ]]; then
      continue
    fi

    if [[ "$last_batch_printed" != "$current_batch" ]]; then
      echo -e "${BOLD}${GREEN}== $current_batch ==${RESET}"
      last_batch_printed="$current_batch"
    fi
    continue
  fi

  # Detect Backlog header
  if [[ "$line" =~ ^##\ Backlog ]]; then
    print_phase
    reset_phase
    continue
  fi

  # Detect phase header: ### Phase X.Y: Goal  or  ### Phase V2-X.Y: Goal
  if [[ "$line" =~ ^###\ Phase ]]; then
    # Print previous phase
    print_phase
    reset_phase

    phase_count=$((phase_count + 1))
    phase_id=$(echo "$line" | sed 's/^### //' | sed 's/:.*//')
    phase_goal=$(echo "$line" | sed 's/^### Phase [0-9]*\.[0-9]*: //')
    continue
  fi

  # Detect status line
  if [[ "$line" =~ "**Status:**" ]]; then
    phase_status=$(echo "$line" | sed 's/.*\*\*Status:\*\* //')
    # Extract the icon
    if [[ "$phase_status" == *"Not Started"* ]]; then
      phase_status_icon="⚪"
    elif [[ "$phase_status" == *"In Progress"* ]]; then
      phase_status_icon="🟡"
    elif [[ "$phase_status" == *"Complete"* ]]; then
      phase_status_icon="🟢"
    elif [[ "$phase_status" == *"Blocked"* ]]; then
      phase_status_icon="🔴"
    fi
    continue
  fi

  # Detect effort
  if [[ "$line" =~ "**Effort:**" ]]; then
    phase_effort=$(echo "$line" | sed 's/.*\*\*Effort:\*\* //')
    continue
  fi

  # Detect depends
  if [[ "$line" =~ "**Depends On:**" ]]; then
    phase_depends=$(echo "$line" | sed 's/.*\*\*Depends On:\*\* //')
    continue
  fi

  # Detect Done When
  if [[ "$line" =~ "**Done When:**" ]]; then
    phase_done_when=$(echo "$line" | sed 's/.*\*\*Done When:\*\* //')
    continue
  fi

  # Detect Key Files
  if [[ "$line" =~ "**Key Files:**" ]]; then
    phase_key_files=$(echo "$line" | sed 's/.*\*\*Key Files:\*\* //')
    continue
  fi

  # Detect Spec References
  if [[ "$line" =~ "**Spec References:**" ]]; then
    phase_spec_refs=$(echo "$line" | sed 's/.*\*\*Spec References:\*\* //')
    continue
  fi

  # Detect task checkbox lines
  if [[ "$line" =~ ^[[:space:]]*-\ \[.\] ]]; then
    task_text=$(echo "$line" | sed 's/^[[:space:]]*//')
    if [[ -n "$phase_tasks" ]]; then
      phase_tasks="$phase_tasks"$'\n'"$task_text"
    else
      phase_tasks="$task_text"
    fi
    continue
  fi

done < "$ROADMAP"

# Print the last phase
print_phase

# --- Summary ---
echo ""
echo -e "${BOLD}=================================================================${RESET}"
echo -e "${BOLD}  Summary:${RESET} $shown_count actionable phase(s) displayed out of $phase_count total"
echo -e "${BOLD}=================================================================${RESET}"
echo ""

# --- Instructions ---
echo -e "${BOLD}${YELLOW}  !! TDD REQUIRED: Write tests BEFORE production code. See CLAUDE.md. !!${RESET}"
echo ""
echo -e "${DIM}HOW TO CLAIM A PHASE:${RESET}"
echo -e "  1. Update its status in plans/roadmap.md:"
echo -e "     Change ${WHITE}⚪ Not Started${RESET} to ${YELLOW}🟡 In Progress | Agent: @your-agent-id${RESET}"
echo -e "  2. Read the spec files listed under 'Specs' — every Given/When/Then = a test case"
echo -e "  3. Write failing tests first (Red), then implement (Green), then clean up (Refactor)"
echo -e "  4. Create the files listed under 'Key Files'"
echo -e "  5. Run pytest — all tests must pass before marking complete"
echo ""
echo -e "${DIM}HOW TO COMPLETE A PHASE:${RESET}"
echo -e "  1. Verify all 'Done When' criteria are met"
echo -e "  2. Run pytest — all tests green"
echo -e "  3. Move the phase block from plans/roadmap.md to plans/completed/roadmap-archive.md"
echo -e "  4. Add completion date and your agent identifier"
echo -e "  5. Check if any blocked phases are now unblocked (update their status)"
echo ""
echo -e "${DIM}DEPENDENCY RULES:${RESET}"
echo -e "  - Phases marked ${RED}🔴 Blocked${RESET} cannot start until dependencies complete"
echo -e "  - Phases in the same batch with status ${WHITE}⚪ Not Started${RESET} can run in parallel"
echo -e "  - When all dependencies for a blocked phase are archived, update it to ${WHITE}⚪ Not Started${RESET}"
echo ""
