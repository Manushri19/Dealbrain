"""
tools/test_calendar_mcp.py
──────────────────────────
Self-contained test script for the DealBrain Calendar MCP tool wrapper.
Run with: python tools/test_calendar_mcp.py

Live tests (tests 8 and 9) require a running Calendar MCP server.
Set the following environment variables to enable them:
  CALENDAR_MCP_URL=https://your-service.run.app
  TEST_RUN_LIVE=1
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Ensure the project root is on the path so imports resolve correctly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from tools.calendar_mcp import (
    check_calendar_mcp_health,
    create_deal_milestone,
    schedule_ma_timeline,
)

# ── Test harness ───────────────────────────────────────────────────────────────

passed = 0
failed = 0
skipped = 0


def _record(test_name: str, success: bool, detail: str = "") -> None:
    global passed, failed
    if success:
        passed += 1
        print(f"  PASS  {test_name}")
    else:
        failed += 1
        print(f"  FAIL  {test_name}")
        if detail:
            print(f"        detail: {detail}")


# ── TEST 1 — Health check ──────────────────────────────────────────────────────
print("\n[TEST 1] Health check")
result = check_calendar_mcp_health()
if result["healthy"] is True:
    _record("check_calendar_mcp_health() → healthy=True", True)
else:
    _record(
        "check_calendar_mcp_health() → healthy=True",
        False,
        f"full result: {result}",
    )

# ── TEST 2 — Validation: empty title ──────────────────────────────────────────
print("\n[TEST 2] Validation: empty title")
result = create_deal_milestone(title="", date="2025-09-01T10:00:00", description="Test")
ok_false = result["success"] is False
ok_msg = "title" in result.get("error", "").lower()
_record("success is False on empty title", ok_false, str(result))
_record("'title' in error message", ok_msg, result.get("error", ""))

# ── TEST 3 — Validation: invalid date format ───────────────────────────────────
print("\n[TEST 3] Validation: invalid date format")
result = create_deal_milestone(title="Test", date="01-09-2025", description="Test")
ok_false = result["success"] is False
ok_msg = "ISO 8601" in result.get("error", "")
_record("success is False on bad date format", ok_false, str(result))
_record("'ISO 8601' in error message", ok_msg, result.get("error", ""))

# ── TEST 4 — Validation: empty description ────────────────────────────────────
print("\n[TEST 4] Validation: empty description")
result = create_deal_milestone(title="Test", date="2025-09-01T10:00:00", description="")
ok_false = result["success"] is False
ok_msg = "description" in result.get("error", "").lower()
_record("success is False on empty description", ok_false, str(result))
_record("'description' in error message", ok_msg, result.get("error", ""))

# ── TEST 5 — Validation: duration_minutes = 0 ─────────────────────────────────
print("\n[TEST 5] Validation: duration_minutes = 0")
result = create_deal_milestone(
    title="Test",
    date="2025-09-01T10:00:00",
    description="Test",
    duration_minutes=0,
)
ok_false = result["success"] is False
ok_msg = "duration_minutes" in result.get("error", "").lower()
_record("success is False when duration_minutes=0", ok_false, str(result))
_record("'duration_minutes' in error message", ok_msg, result.get("error", ""))

# ── TEST 6 — Validation: duration_minutes > 1440 ──────────────────────────────
print("\n[TEST 6] Validation: duration_minutes > 1440")
result = create_deal_milestone(
    title="Test",
    date="2025-09-01T10:00:00",
    description="Test",
    duration_minutes=1441,
)
ok_false = result["success"] is False
ok_msg = "1440" in result.get("error", "")
_record("success is False when duration_minutes=1441", ok_false, str(result))
_record("'1440' in error message", ok_msg, result.get("error", ""))

# ── TEST 7 — Validation: invalid signing_date for schedule_ma_timeline() ──────
print("\n[TEST 7] Validation: invalid signing_date for schedule_ma_timeline()")
result = schedule_ma_timeline(deal_name="AcmeCo", signing_date="not-a-date")
ok_false = result["success"] is False
ok_count = result["created_count"] == 0
_record("success is False on invalid signing_date", ok_false, str(result))
_record("created_count == 0", ok_count, str(result))

# ── TEST 8 — Live: single event ────────────────────────────────────────────────
print("\n[TEST 8] Live: single event creation")
if os.getenv("TEST_RUN_LIVE") != "1":
    skipped += 1
    print("  SKIP  — set TEST_RUN_LIVE=1 to run live tests")
else:
    live_date = (datetime.now(timezone.utc) + timedelta(days=7)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    result = create_deal_milestone(
        title="DealBrain TEST — AcmeCo IC Review",
        date=live_date,
        description="Automated test event created by tools/test_calendar_mcp.py. Safe to delete.",
        duration_minutes=60,
    )
    ok_success = result.get("success") is True
    ok_id = "event_id" in result
    ok_link = "event_link" in result
    _record("success is True (live)", ok_success, str(result))
    _record("event_id present in response", ok_id, str(result))
    _record("event_link present in response", ok_link, str(result))
    if ok_success and ok_link:
        print(f"        event_link: {result['event_link']}")

# ── TEST 9 — Live: full M&A timeline ──────────────────────────────────────────
print("\n[TEST 9] Live: full M&A timeline via schedule_ma_timeline()")
if os.getenv("TEST_RUN_LIVE") != "1":
    skipped += 1
    print("  SKIP  — set TEST_RUN_LIVE=1 to run live tests")
else:
    timeline_signing = (datetime.now(timezone.utc) + timedelta(days=90)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    result = schedule_ma_timeline(
        deal_name="DealBrain TEST — AcmeCo",
        signing_date=timeline_signing,
        acquirer="TestCo Holdings",
        target="AcmeCo GmbH",
    )
    ok_success = result.get("success") is True
    ok_count = result.get("created_count") == 6
    _record("success is True (live full timeline)", ok_success, str(result))
    _record("created_count == 6", ok_count, str(result))
    print(
        f"        created_count={result.get('created_count')}  "
        f"failed_count={result.get('failed_count')}"
    )

# ── Summary ────────────────────────────────────────────────────────────────────
total = passed + failed + skipped
print(f"\n{'─' * 50}")
print(f"{passed}/9 tests passed  ({skipped} skipped)")
print(f"{'─' * 50}\n")

if failed > 0:
    sys.exit(1)
