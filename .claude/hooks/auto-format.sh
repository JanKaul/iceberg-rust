#!/bin/bash

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path')

# Format Rust files with cargo fmt
if [[ "$FILE_PATH" == *.rs ]]; then
  cargo fmt -- "$FILE_PATH" 2>/dev/null
fi

# Format Markdown files with prettier
if [[ "$FILE_PATH" == *.md ]]; then
  npx prettier --write "$FILE_PATH" 2>/dev/null
fi

exit 0
