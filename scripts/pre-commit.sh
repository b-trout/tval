#!/usr/bin/env bash
set -euo pipefail

failed=0

if [ -d src/ ] || [ -d tests/ ]; then
    targets=""
    [ -d src/ ] && targets="src/"
    [ -d tests/ ] && targets="$targets tests/"

    echo "Running ruff check..."
    ruff check $targets || failed=1

    echo "Running ruff format --check..."
    ruff format --check $targets || failed=1
fi

if [ -d src/ ]; then
    echo "Running mypy..."
    mypy src/ || failed=1
fi

if [ $failed -ne 0 ]; then
    echo "Pre-commit checks failed. Fix errors before committing."
    exit 1
fi
