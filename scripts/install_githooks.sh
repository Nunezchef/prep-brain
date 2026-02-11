#!/bin/bash
set -e

echo "Installing Prep-Brain git hooks..."

# Create .githooks directory if not exists (though git config points to it)
# actually we can just point git core.hooksPath to our githooks folder directly
# avoiding the need to copy files.

HOOKS_DIR="githooks"

if [ ! -d "$HOOKS_DIR" ]; then
    echo "Error: $HOOKS_DIR directory not found."
    exit 1
fi

# Make sure hooks are executable
chmod +x $HOOKS_DIR/*

# Configure git to use our hooks directory
git config core.hooksPath $HOOKS_DIR

echo "✅ Git hooks installed successfully."
echo "   Pre-commit hook will now scan for secrets."
