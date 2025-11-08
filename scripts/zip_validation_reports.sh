#!/bin/bash
# Helper script to zip validation reports for PR attachment
# Usage: ./scripts/zip_validation_reports.sh [run_number]
#
# If run_number is provided, zips that specific run
# Otherwise, zips the most recent run

set -e

VALIDATION_DIR="run_logs/validation"

if [ ! -d "$VALIDATION_DIR" ]; then
    echo "Error: $VALIDATION_DIR directory not found"
    exit 1
fi

# Get run directory
if [ -n "$1" ]; then
    # Specific run number provided
    RUN_DIR=$(find "$VALIDATION_DIR" -maxdepth 1 -type d -name "Run $1 -*" | head -1)
    if [ -z "$RUN_DIR" ]; then
        echo "Error: Run $1 not found in $VALIDATION_DIR"
        exit 1
    fi
else
    # Get most recent run
    RUN_DIR=$(ls -dt "$VALIDATION_DIR"/Run* 2>/dev/null | head -1)
    if [ -z "$RUN_DIR" ]; then
        echo "Error: No validation runs found in $VALIDATION_DIR"
        exit 1
    fi
fi

# Extract run info
RUN_NAME=$(basename "$RUN_DIR")
BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create zip filename
ZIP_NAME="validation_${BRANCH_NAME}_${TIMESTAMP}.zip"

# Create zip
echo "Zipping validation reports from: $RUN_NAME"
cd "$(dirname "$VALIDATION_DIR")"
zip -r "$ZIP_NAME" "$(basename "$VALIDATION_DIR")/$RUN_NAME"

echo ""
echo "Created: $(pwd)/$ZIP_NAME"
echo ""
echo "To attach to PR:"
echo "  1. Go to your PR on GitHub"
echo "  2. Add a comment"
echo "  3. Drag and drop $(pwd)/$ZIP_NAME"
echo ""
