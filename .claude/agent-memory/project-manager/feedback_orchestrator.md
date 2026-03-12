---
name: Orchestrator Rules
description: Strict rules about agent launch - always use orchestrator.sh, never hand-craft prompts
type: feedback
---

## Agent Launch Rules (from user)
- NEVER launch agents interactively with hand-crafted prompts
- ALWAYS use the orchestrator: `bash orchestrator.sh plan` -> parse JSON -> launch agents using the orchestrator's generated `prompt` field
- Single entry point: `orchestrator.sh` (no separate .py file -- Python is embedded inside the .sh)
- After each agent completes: run `bash orchestrator.sh complete "<phase_id>"` then re-run `plan` to check for newly unblocked phases
- Agents run in isolated git worktrees (background, sonnet model)
- Development approach is strict TDD -- tests before production code, always
