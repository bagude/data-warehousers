# Session Memory System Design

**Date:** 2026-02-15
**Scope:** data-warehousers project only

## Objective

Build a persistent memory system that captures learnings from each Claude Code session and makes them available in future sessions, creating a feedback loop that improves the data-warhouse skill over time.

## Architecture

Two-phase memory loop:

```
Session N ends -> SessionEnd hook saves transcript ref to queue
Session N+1 starts -> SessionStart hook triggers -> I read queued transcripts ->
  I summarize learnings -> I append to diary -> Skill reads diary at load
```

## Components

### 1. SessionEnd Hook (`session-end.sh`)

- **Location:** `.claude/hooks/session-end.sh`
- **Trigger:** `SessionEnd` hook event
- **Input:** JSON via stdin with `session_id`, `transcript_path`, `cwd`, `reason`
- **Behavior:** Appends a line to `pending-transcripts.jsonl` if `cwd` matches this project
- **Output format (JSONL):**
  ```json
  {"session_id": "abc123", "transcript_path": "/path/to/transcript.jsonl", "timestamp": "2026-02-15T14:30:00", "cwd": "/path/to/data-warehousers"}
  ```

### 2. Pending Transcripts Queue

- **Location:** `.claude/memory/pending-transcripts.jsonl`
- **Format:** One JSON object per line, one per ended session
- **Lifecycle:** Entries are removed after processing at next session start

### 3. Session Diary (`diary.md`)

- **Location:** `.claude/memory/diary.md`
- **Format:** Markdown with dated entries, four categories per entry
- **Categories:**
  - **Preferences & Corrections** — How the user wants things done
  - **Domain Knowledge** — Oil & gas specifics, data source quirks, business rules
  - **Mistakes** — Errors, overengineering, wrong assumptions to avoid
  - **Decisions & Rationale** — Architecture choices and why they were made
- **Template:**
  ```markdown
  # Session Diary

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

### 4. Skill Integration

- **File:** `~/.claude/skills/data-warhouse/SKILL.md`
- **Change:** Add a `## Session Memory` section instructing the SVP to read `.claude/memory/diary.md` and process `.claude/memory/pending-transcripts.jsonl` on session start
- **Effect:** Diary content informs every session without polluting the skill file itself

### 5. Hook Configuration

- **File:** `.claude/settings.local.json` (project-scoped, not committed)
- **Config:**
  ```json
  {
    "hooks": {
      "SessionEnd": [
        {
          "hooks": [
            {
              "type": "command",
              "command": ".claude/hooks/session-end.sh"
            }
          ]
        }
      ]
    }
  }
  ```

## Processing Flow

### SessionEnd (automated, no AI)
1. Hook receives session data via stdin
2. Extracts `session_id`, `transcript_path`, `cwd`, `reason`
3. Checks `cwd` matches data-warehousers project path
4. Appends entry to `.claude/memory/pending-transcripts.jsonl`
5. Exits 0

### SessionStart (AI-driven)
1. SKILL.md instruction triggers diary check
2. Read `.claude/memory/pending-transcripts.jsonl`
3. If entries exist, read each referenced transcript
4. Extract learnings across all four categories
5. Append dated entry to `.claude/memory/diary.md`
6. Clear processed entries from queue
7. Read full `diary.md` to inform current session

## Constraints

- SessionEnd hooks can only run shell commands (no AI summarization)
- SessionEnd hooks cannot block session termination
- Transcript files may be large; summarization happens at SessionStart with AI context
- Diary should stay concise; avoid verbatim transcript dumps
- Project-scoped only (cwd check in hook script)

## Out of Scope

- Global (cross-project) memory
- Automatic skill file modification
- External API calls for summarization
- User approval gate for diary entries
