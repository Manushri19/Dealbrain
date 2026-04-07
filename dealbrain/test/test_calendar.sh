#!/usr/bin/env bash
# test_calendar.sh — Local smoke tests for the DealBrain Calendar MCP Server
# Usage: bash test_calendar.sh
# Requires: server running at https://calendar-mcp-535581122241.us-central1.run.app

set -euo pipefail

BASE_URL="https://calendar-mcp-535581122241.us-central1.run.app"

echo "========================================"
echo " Test 1 — Health check"
echo "========================================"
curl -s "${BASE_URL}/health" | python3 -m json.tool

echo ""
echo "========================================"
echo " Test 2 — Valid event creation"
echo "========================================"
curl -s -X POST "${BASE_URL}/create_event" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Simple Test Event",
    "date": "2025-06-20T14:00:00",
    "description": "Standard hyphen test - no special characters.",
    "duration_minutes": 30
  }' | python3 -m json.tool

echo ""
echo "========================================"
echo " Test 3 — Missing field validation (expect HTTP 422)"
echo "========================================"
curl -s -X POST "${BASE_URL}/create_event" \
  -H "Content-Type: application/json" \
  -d '{"title": "Incomplete event"}' | python3 -m json.tool

echo ""
echo "All tests complete."
