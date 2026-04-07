#!/usr/bin/env bash
# test_gmail.sh — DealBrain Gmail MCP Server smoke tests
# -------------------------------------------------------
# Prerequisites:
#   - gmail_server.py running locally on port 8080
#   - GOOGLE_SERVICE_ACCOUNT_JSON and GMAIL_SENDER_ADDRESS env vars set
#   - Replace YOUR_EMAIL@gmail.com with a real delivery address for Test 2/4
#
# Usage:
#   chmod +x test_gmail.sh
#   ./test_gmail.sh

BASE_URL="https://gmail-mcp-535581122241.us-central1.run.app"
RECIPIENT="mmanushri19@gmail.com"

echo "========================================================"
echo " DealBrain Gmail MCP Server — Smoke Tests"
echo " Target: ${BASE_URL}"
echo "========================================================"
echo ""

# -------------------------------------------------------
# Test 1 — Health check
# Expected: {"status": "ok"}
# -------------------------------------------------------
echo "--- Test 1: Health Check ---"
curl -s "${BASE_URL}/health" | python3 -m json.tool
echo ""

# -------------------------------------------------------
# Test 2 — Valid IC memo email (primary use case)
# Expected: HTTP 200, success=true, message_id and thread_id present
# -------------------------------------------------------
echo "--- Test 2: Valid IC Memo Email (Heredoc Fix) ---"

# We define the JSON exactly as it should look, with no extra escaping needed
read -r -d '' PAYLOAD << 'EOF'
{
  "to": "mmanushri19@gmail.com",
  "subject": "DealBrain IC Memo - AcmeCo Acquisition (CONFIDENTIAL)",
  "sender_name": "DealBrain",
  "html_body": "<html><body><h1 style='color:#1a1a2e'>Investment Committee Memo</h1><p>Target: AcmeCo GmbH. Risk Flag: <b>Pension deficit EUR 14M</b></p></body></html>",
  "cc": []
}
EOF

curl -s -X POST "${BASE_URL}/send_email" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD" | python3 -m json.tool
# -------------------------------------------------------
# Test 3 — Invalid email address (no "@")
# Expected: HTTP 422, success=false, error="Invalid recipient email address"
# -------------------------------------------------------
echo "--- Test 3: Invalid Email Address (expect 422) ---"
curl -s -X POST "${BASE_URL}/send_email" \
  -H "Content-Type: application/json" \
  -d '{
    "to": "not-an-email",
    "subject": "Test",
    "html_body": "<p>Test</p>"
  }' | python3 -m json.tool
echo ""

# -------------------------------------------------------
# Test 4 — Missing required field (html_body absent)
# Expected: HTTP 422 from Pydantic validation
# -------------------------------------------------------
echo "--- Test 4: Missing Required Field html_body (expect 422 from Pydantic) ---"
curl -s -X POST "${BASE_URL}/send_email" \
  -H "Content-Type: application/json" \
  -d '{
    "to": "'"${RECIPIENT}"'",
    "subject": "Missing body test"
  }' | python3 -m json.tool
echo ""

echo "========================================================"
echo " All tests complete."
echo "========================================================"
