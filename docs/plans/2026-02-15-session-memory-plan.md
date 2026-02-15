# Session Memory System Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a two-phase persistent memory system that captures session transcripts at SessionEnd and summarizes learnings at SessionStart, feeding back into the data-warhouse skill.

**Architecture:** SessionEnd hook (bash script) appends transcript references to a JSONL queue. On next SessionStart, the skill instruction triggers AI-driven transcript reading, learning extraction, and diary append. The diary is a separate markdown file referenced by SKILL.md.

**Tech Stack:** Bash (hook script), jq (JSON parsing), Claude Code hooks API, Markdown (diary format)

---

### Task 1: Create memory directory structure

**Files:**
- Create: `.claude/memory/` directory
- Create: `.claude/memory/diary.md` (seed file)

**Step 1: Create the directory**

```bash
mkdir -p .claude/memory
```

**Step 2: Create the seed diary file**

Write `.claude/memory/diary.md`:

```markdown
# Session Diary

> Persistent memory for the data-warehousers project.
> Entries are added automatically at the start of each session by summarizing the previous session's transcript.
```

**Step 3: Create empty pending queue**

Write `.claude/memory/pending-transcripts.jsonl` as an empty file.

**Step 4: Commit**

```bash
git add .claude/memory/diary.md .claude/memory/pending-transcripts.jsonl
git commit -m "feat: initialize session memory directory structure"
```

---

### Task 2: Write the SessionEnd hook script

**Files:**
- Create: `.claude/hooks/session-end.sh`

**Step 1: Create hooks directory**

```bash
mkdir -p .claude/hooks
```

**Step 2: Write the hook script**

Write `.claude/hooks/session-end.sh`:

```bash
#!/usr/bin/env bash
# SessionEnd hook: queues transcript reference for next-session processing.
# Only queues if cwd matches the data-warehousers project.

set -euo pipefail

INPUT=$(cat)

SESSION_ID=$(echo "$INPUT" | jq -r '.session_id // empty')
TRANSCRIPT_PATH=$(echo "$INPUT" | jq -r '.transcript_path // empty')
CWD=$(echo "$INPUT" | jq -r '.cwd // empty')
REASON=$(echo "$INPUT" | jq -r '.reason // "unknown"')
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Only queue if we're in the data-warehousers project
if [[ "$CWD" != *"data-warehousers"* ]]; then
  exit 0
fi

# Validate required fields
if [[ -z "$SESSION_ID" || -z "$TRANSCRIPT_PATH" ]]; then
  exit 0
fi

# Resolve memory directory relative to project
MEMORY_DIR="$CWD/.claude/memory"
QUEUE_FILE="$MEMORY_DIR/pending-transcripts.jsonl"

# Create memory dir if missing
mkdir -p "$MEMORY_DIR"

# Append to queue
echo "{\"session_id\":\"$SESSION_ID\",\"transcript_path\":\"$TRANSCRIPT_PATH\",\"timestamp\":\"$TIMESTAMP\",\"cwd\":\"$CWD\",\"reason\":\"$REASON\"}" >> "$QUEUE_FILE"

exit 0
```

**Step 3: Make it executable**

```bash
chmod +x .claude/hooks/session-end.sh
```

**Step 4: Test the script manually**

```bash
echo '{"session_id":"test-123","transcript_path":"/tmp/test.jsonl","cwd":"'$(pwd)'","reason":"other"}' | .claude/hooks/session-end.sh
cat .claude/memory/pending-transcripts.jsonl
```

Expected: One JSONL line with `test-123` session ID, timestamp, and project cwd.

**Step 5: Clean up test data**

```bash
> .claude/memory/pending-transcripts.jsonl
```

**Step 6: Commit**

```bash
git add .claude/hooks/session-end.sh
git commit -m "feat: add SessionEnd hook to queue transcript references"
```

---

### Task 3: Configure the SessionEnd hook in settings

**Files:**
- Modify: `.claude/settings.local.json`

**Step 1: Read current settings**

Read `.claude/settings.local.json` to see existing config.

**Step 2: Add hooks configuration**

Add a `hooks` key to the existing JSON. The hooks config sits alongside the existing `permissions` key:

```json
{
  "permissions": { ... },
  "hooks": {
    "SessionEnd": [
      {
        "hooks": [
          {
            "type": "command",
            "command": ".claude/hooks/session-end.sh",
            "timeout": 10
          }
        ]
      }
    ]
  }
}
```

Merge this into the existing file — do NOT overwrite the `permissions` block.

**Step 3: Verify JSON is valid**

```bash
python -c "import json; json.load(open('.claude/settings.local.json'))"
```

Expected: No output (valid JSON).

**Step 4: Do NOT commit** — `settings.local.json` is local-only, not committed.

---

### Task 4: Add Session Memory section to SKILL.md

**Files:**
- Modify: `~/.claude/skills/data-warhouse/SKILL.md` (append section at end)

**Step 1: Read current SKILL.md**

Read `~/.claude/skills/data-warhouse/SKILL.md` to confirm current end of file.

**Step 2: Append Session Memory section**

Add this section at the end of `SKILL.md`:

```markdown

---

## Session Memory

On every session start for the `data-warehousers` project:

1. **Check for pending transcripts:** Read `.claude/memory/pending-transcripts.jsonl` in the project directory. If entries exist, process each one.

2. **Process each transcript:** For each pending entry:
   - Read the transcript file at the path specified in `transcript_path`
   - Extract learnings across four categories:
     - **Preferences & Corrections** — How the user wants things done, corrections to my approach
     - **Domain Knowledge** — Oil & gas specifics, data source quirks, schema rules, business logic
     - **Mistakes** — Errors I made, overengineering, wrong assumptions to avoid repeating
     - **Decisions & Rationale** — Architecture choices made and why
   - Only record substantive learnings. Skip routine tool usage and boilerplate.

3. **Append to diary:** Add a dated entry to `.claude/memory/diary.md` using this format:
   ```markdown
   ## YYYY-MM-DD

   ### Preferences & Corrections
   - [learning]

   ### Domain Knowledge
   - [learning]

   ### Mistakes
   - [learning]

   ### Decisions
   - [learning]
   ```
   Omit any category section that has no entries for that session.

4. **Clear processed entries:** Empty `.claude/memory/pending-transcripts.jsonl` after all entries are processed.

5. **Load diary context:** Read `.claude/memory/diary.md` to inform the current session with accumulated learnings.
```

**Step 3: Verify the file renders correctly**

Read back the full `SKILL.md` and confirm the new section appears at the end without breaking existing content.

**Step 4: Commit**

This file is outside the project repo (it's in `~/.claude/skills/`), so no git commit needed.

---

### Task 5: Add .gitignore entries for memory files

**Files:**
- Modify: `.gitignore` (or create if missing)

**Step 1: Check if .gitignore exists**

```bash
ls -la .gitignore
```

**Step 2: Add memory exclusions**

Append to `.gitignore`:

```
# Session memory (diary is committed, queue and transcripts are not)
.claude/memory/pending-transcripts.jsonl
```

The `diary.md` file SHOULD be committed (it's project knowledge). The `pending-transcripts.jsonl` queue should NOT be committed (it's transient state).

**Step 3: Commit**

```bash
git add .gitignore .claude/memory/diary.md
git commit -m "feat: add gitignore rules for session memory queue"
```

---

### Task 6: End-to-end verification

**Step 1: Verify hook script runs without error**

```bash
echo '{"session_id":"e2e-test","transcript_path":"/tmp/fake.jsonl","cwd":"'$(pwd)'","reason":"other"}' | .claude/hooks/session-end.sh
```

Expected: Exit 0, line appended to queue.

**Step 2: Verify cwd filtering works**

```bash
echo '{"session_id":"wrong-project","transcript_path":"/tmp/fake.jsonl","cwd":"/some/other/project","reason":"other"}' | .claude/hooks/session-end.sh
```

Expected: Exit 0, NO line appended (cwd doesn't match).

**Step 3: Verify queue file content**

```bash
cat .claude/memory/pending-transcripts.jsonl
```

Expected: Only the `e2e-test` entry, not the `wrong-project` entry.

**Step 4: Clean up test data**

```bash
> .claude/memory/pending-transcripts.jsonl
```

**Step 5: Verify SKILL.md has Session Memory section**

Read `~/.claude/skills/data-warhouse/SKILL.md` and confirm the `## Session Memory` section exists at the end.

**Step 6: Final commit if any cleanup needed**

```bash
git status
```
