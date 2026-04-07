"""
tools/calendar_mcp.py
─────────────────────
ADK-compatible synchronous HTTP wrapper around the DealBrain Calendar MCP
server. Agents call these tools to create Google Calendar events for M&A
deal milestones.

The MCP server must already be running (local or Cloud Run). This file
never touches the Google APIs directly — it only POSTs to the MCP server.
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
import httpx

# ── Module-level config ────────────────────────────────────────────────────────
CALENDAR_MCP_URL: str = os.getenv("CALENDAR_MCP_URL", "http://localhost:8080")
CALENDAR_MCP_URL = CALENDAR_MCP_URL.rstrip("/")

logger = logging.getLogger("tools.calendar_mcp")

DEFAULT_TIMEOUT: float = 30.0   # seconds — calendar API can be slow
HEALTH_TIMEOUT: float = 10.0    # seconds — health check should be fast


# ── Private helpers ────────────────────────────────────────────────────────────

def _validate_iso_datetime(date_str: str) -> tuple[bool, str]:
    """
    Validate that date_str can be parsed as an ISO 8601 datetime.
    Returns (True, "") on success.
    Returns (False, "<reason>") on failure.
    """
    if not date_str:
        return False, "date cannot be empty"
    try:
        datetime.fromisoformat(date_str)
    except ValueError:
        return (
            False,
            f"date must be ISO 8601 format e.g. '2025-06-20T14:00:00', got: {date_str!r}",
        )
    return True, ""


# ── Primary tool ───────────────────────────────────────────────────────────────

def create_deal_milestone(
    title: str,
    date: str,
    description: str,
    duration_minutes: int = 60,
) -> dict:
    """
    Create a Google Calendar event for an M&A deal milestone via the
    DealBrain Calendar MCP server.

    Use this tool when the timeline agent needs to book a deal milestone
    such as an IC review, exclusivity deadline, signing date, or closing date.

    Args:
        title:            Event title shown in Google Calendar.
                          Should include deal name and milestone type,
                          e.g. "AcmeCo — IC Review" or "AcmeCo — Signing".
        date:             Event start datetime in ISO 8601 format.
                          Examples: "2025-06-20T14:00:00"
                                    "2025-06-20T14:00:00+00:00"
                          The server treats naive datetimes as UTC.
        description:      Event body text. Include deal context, attendees,
                          and any relevant risk flags or agenda items.
        duration_minutes: Length of the event in minutes. Default is 60.
                          Use 30 for quick check-ins, 120 for IC meetings,
                          480 (full day) for signing or closing events.

    Returns:
        dict with keys:
          success    (bool) — True if event was created successfully
          event_id   (str)  — Google Calendar event ID, present on success
          event_link (str)  — URL to the event in Google Calendar UI
          title      (str)  — echoed event title
          date       (str)  — echoed event start date
          error      (str)  — present only on failure, contains error message
    """
    # a) Validate title
    if not title or title.strip() == "":
        return {"success": False, "error": "title cannot be empty"}

    # b) Validate date
    valid, reason = _validate_iso_datetime(date)
    if not valid:
        return {"success": False, "error": reason}

    # c) Validate description
    if not description or description.strip() == "":
        return {"success": False, "error": "description cannot be empty"}

    # d) Validate duration_minutes
    if not isinstance(duration_minutes, int) or duration_minutes < 1:
        return {"success": False, "error": "duration_minutes must be a positive integer"}
    if duration_minutes > 1440:
        return {"success": False, "error": "duration_minutes cannot exceed 1440 (24 hours)"}

    # e) Build request payload (description intentionally excluded from logs below)
    payload = {
        "title": title,
        "date": date,
        "description": description,
        "duration_minutes": duration_minutes,
    }

    # f) Log at INFO — description deliberately omitted (sensitive deal data)
    logger.info(
        "Creating calendar milestone | title=%s | date=%s | duration=%dmin",
        title, date, duration_minutes,
    )

    # g/h/i) HTTP call with full exception handling
    try:
        response = httpx.post(
            f"{CALENDAR_MCP_URL}/create_event",
            json=payload,
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        if data.get("success"):
            logger.info(
                "Milestone created | event_id=%s | link=%s",
                data.get("event_id"), data.get("event_link"),
            )
            return data

        error_msg = data.get("error", "Unknown error from Calendar MCP server")
        logger.error("Calendar MCP returned failure | error=%s", error_msg)
        return {"success": False, "error": error_msg}

    except httpx.TimeoutException:
        logger.error("Calendar MCP timed out after %ss", DEFAULT_TIMEOUT)
        return {"success": False, "error": f"Request timed out after {DEFAULT_TIMEOUT}s"}

    except httpx.HTTPStatusError as e:
        logger.error(
            "Calendar MCP HTTP error | status=%s | body=%s",
            e.response.status_code, e.response.text,
        )
        return {
            "success": False,
            "error": f"HTTP {e.response.status_code}: {e.response.text}",
        }

    except Exception as e:
        logger.exception("Unexpected error calling Calendar MCP")
        return {"success": False, "error": str(e)}


# ── Secondary tool ─────────────────────────────────────────────────────────────

def schedule_ma_timeline(
    deal_name: str,
    signing_date: str,
    acquirer: str = "",
    target: str = "",
) -> dict:
    """
    Schedule the complete standard M&A deal timeline in Google Calendar.
    Automatically calculates and creates all key milestones working backwards
    from the signing date.

    Use this tool when the timeline agent has confirmed a signing date and
    needs to populate the entire deal calendar in one operation.

    Args:
        deal_name:    Name of the deal, e.g. "AcmeCo Acquisition".
                      Used as a prefix in all event titles.
        signing_date: Target signing date in ISO 8601 format.
                      e.g. "2025-09-01T10:00:00"
                      All other milestones are calculated relative to this date.
        acquirer:     Name of the acquiring entity. Included in descriptions.
        target:       Name of the target company. Included in descriptions.

    Returns:
        dict with keys:
          success          (bool)      — True if ALL events were created
          created_count    (int)       — number of events successfully created
          failed_count     (int)       — number of events that failed
          milestones       (list[dict])— list of result dicts from each
                                         create_deal_milestone() call
          error            (str)       — present only if signing_date is invalid
    """
    # a) Validate signing_date
    valid, reason = _validate_iso_datetime(signing_date)
    if not valid:
        return {
            "success": False,
            "error": reason,
            "created_count": 0,
            "failed_count": 0,
            "milestones": [],
        }

    # b) Parse into datetime, apply UTC if naive
    signing_dt = datetime.fromisoformat(signing_date)
    if signing_dt.tzinfo is None:
        signing_dt = signing_dt.replace(tzinfo=timezone.utc)

    # c) Define standard M&A milestones
    MILESTONES = [
        {
            "offset_days": -42,
            "title_suffix": "— Management Presentation",
            "duration_minutes": 180,
            "description_tmpl": (
                f"Management presentation for {deal_name}. "
                f"Acquirer: {acquirer or 'TBC'}. Target: {target or 'TBC'}. "
                "Agenda: business overview, financials, growth strategy."
            ),
        },
        {
            "offset_days": -28,
            "title_suffix": "— Final Bid Deadline",
            "duration_minutes": 60,
            "description_tmpl": (
                f"Final binding bid submission deadline for {deal_name}. "
                f"Acquirer: {acquirer or 'TBC'} to submit final offer by end of day."
            ),
        },
        {
            "offset_days": -21,
            "title_suffix": "— IC Review",
            "duration_minutes": 120,
            "description_tmpl": (
                f"Investment Committee review for {deal_name}. "
                f"Acquirer: {acquirer or 'TBC'}. Target: {target or 'TBC'}. "
                "Agenda: DealBrain IC memo review, risk flags, valuation sign-off."
            ),
        },
        {
            "offset_days": -14,
            "title_suffix": "— Due Diligence Completion",
            "duration_minutes": 60,
            "description_tmpl": (
                f"Due diligence completion checkpoint for {deal_name}. "
                "All workstreams (financial, legal, regulatory, commercial) to confirm completion."
            ),
        },
        {
            "offset_days": -7,
            "title_suffix": "— Final Regulatory Clearance",
            "duration_minutes": 60,
            "description_tmpl": (
                f"Final regulatory clearance confirmation for {deal_name}. "
                "Confirm antitrust and merger control notifications are cleared."
            ),
        },
        {
            "offset_days": 0,
            "title_suffix": "— Signing",
            "duration_minutes": 120,
            "description_tmpl": (
                f"Signing of definitive agreements for {deal_name}. "
                f"Acquirer: {acquirer or 'TBC'}. Target: {target or 'TBC'}."
            ),
        },
    ]

    # d) Create each milestone event
    milestones: list[dict] = []
    for milestone in MILESTONES:
        milestone_dt = signing_dt + timedelta(days=milestone["offset_days"])
        milestone_date = milestone_dt.isoformat()
        event_title = f"{deal_name} {milestone['title_suffix']}"

        result = create_deal_milestone(
            title=event_title,
            date=milestone_date,
            description=milestone["description_tmpl"],
            duration_minutes=milestone["duration_minutes"],
        )
        milestones.append(result)

    # e) Compute success/failure counts
    created = sum(1 for r in milestones if r.get("success"))
    failed = len(milestones) - created

    # f) Log summary
    logger.info(
        "Timeline scheduled for %s | created=%d | failed=%d",
        deal_name, created, failed,
    )

    # g) Return summary dict
    return {
        "success": failed == 0,
        "created_count": created,
        "failed_count": failed,
        "milestones": milestones,
    }


# ── Utility tool ───────────────────────────────────────────────────────────────

def check_calendar_mcp_health() -> dict:
    """
    Ping the Calendar MCP server health endpoint. Use before scheduling
    milestones in critical workflows to confirm the MCP server is reachable.

    Returns:
        dict with keys:
          healthy (bool) — True if server responded with status "ok"
          status  (str)  — raw status string from server or error description
    """
    try:
        response = httpx.get(
            f"{CALENDAR_MCP_URL}/health",
            timeout=HEALTH_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        return {
            "healthy": data.get("status") == "ok",
            "status": data.get("status", "unknown"),
        }
    except Exception as e:
        logger.error("Calendar MCP health check failed | error=%s", e)
        return {"healthy": False, "status": str(e)}


# ── ADK tool registry ──────────────────────────────────────────────────────────
# Import CALENDAR_TOOLS in agents/manager.py and agents/timeline.py to
# register these as callable tools.
CALENDAR_TOOLS = [
    create_deal_milestone,
    schedule_ma_timeline,
    check_calendar_mcp_health,
]
