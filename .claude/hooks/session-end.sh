#!/usr/bin/env bash
# SessionEnd hook: queues transcript reference for next-session processing.
# Only queues if cwd matches the data-warehousers project.

set -euo pipefail

# Read stdin and parse all fields in a single Python call to avoid
# backslash-escaping issues when re-echoing JSON through bash.
eval "$(python -c "
import sys, json
d = json.load(sys.stdin)
for key in ('session_id', 'transcript_path', 'cwd', 'reason'):
    val = d.get(key, '') or ''
    # Escape single quotes for safe bash eval
    val = val.replace(\"'\", \"'\\\"'\\\"'\")
    print(f\"HOOK_{key.upper()}='{val}'\")
")"

SESSION_ID="${HOOK_SESSION_ID:-}"
TRANSCRIPT_PATH="${HOOK_TRANSCRIPT_PATH:-}"
CWD="${HOOK_CWD:-}"
REASON="${HOOK_REASON:-unknown}"
if [[ -z "$REASON" ]]; then
  REASON="unknown"
fi
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

# Append to queue (use Python to produce valid JSON with proper escaping)
python -c "
import json, sys
entry = {
    'session_id': sys.argv[1],
    'transcript_path': sys.argv[2],
    'timestamp': sys.argv[3],
    'cwd': sys.argv[4],
    'reason': sys.argv[5]
}
print(json.dumps(entry, ensure_ascii=False))
" "$SESSION_ID" "$TRANSCRIPT_PATH" "$TIMESTAMP" "$CWD" "$REASON" >> "$QUEUE_FILE"

exit 0
