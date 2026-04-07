"""
tools/gmail_mcp.py

ADK-compatible tool wrapper for sending emails via the DealBrain Gmail MCP
server (mcp_servers/gmail_server.py). All communication is synchronous HTTP
using httpx — no async, no Google API libraries.
"""

import os
import logging
import httpx
from typing import Optional

# ── Module-level config ──────────────────────────────────────────────────────

GMAIL_MCP_URL: str = os.getenv("GMAIL_MCP_URL", "http://localhost:8080").rstrip("/")

logger = logging.getLogger("tools.gmail_mcp")

DEFAULT_TIMEOUT = 30.0  # seconds — email sending can be slow over SMTP


# ── Primary tool ─────────────────────────────────────────────────────────────


def send_ic_memo(
    to: str,
    subject: str,
    html_body: str,
    cc: Optional[list[str]] = None,
    sender_name: str = "DealBrain",
) -> dict:
    """
    Send an Investment Committee memo or deal summary email via the DealBrain
    Gmail MCP server.

    Use this tool when the reporting agent has finished composing the IC memo
    and needs to deliver it to the deal team by email.

    Args:
        to:          Recipient email address. Must be a valid email format.
        subject:     Email subject line. Should include deal name and
                     classification, e.g. "IC Memo — AcmeCo (CONFIDENTIAL)".
        html_body:   Complete HTML content of the IC memo. Must be non-empty.
        cc:          Optional list of additional recipient email addresses.
        sender_name: Display name shown in the From field. Default "DealBrain".

    Returns:
        dict with keys:
          success (bool)   — True if email was sent successfully
          to (str)         — recipient address echoed from server
          subject (str)    — subject line echoed from server
          error (str)      — present only on failure, contains error message
    """
    # ── a) Input validation (before any HTTP call) ───────────────────────────
    if not to or "@" not in to:
        return {"success": False, "error": "Invalid recipient email address"}

    if not html_body:
        return {"success": False, "error": "html_body cannot be empty"}

    if not subject:
        return {"success": False, "error": "subject cannot be empty"}

    # ── b) Normalise cc ───────────────────────────────────────────────────────
    if cc is None:
        cc = []
    cc = [x for x in cc if x.strip()]

    # ── c) Build request payload ──────────────────────────────────────────────
    payload = {
        "to": to,
        "subject": subject,
        "html_body": html_body,
        "cc": cc,
        "sender_name": sender_name,
    }

    # ── d) Log intent — html_body intentionally omitted (sensitive deal data) ─
    logger.info("Sending IC memo | to=%s | subject=%s", to, subject)

    # ── e–i) HTTP call with full exception handling ───────────────────────────
    try:
        response = httpx.post(
            f"{GMAIL_MCP_URL}/send_email",
            json=payload,
            timeout=DEFAULT_TIMEOUT,
        )

        # f) Raise for non-2xx so HTTPStatusError is caught below
        response.raise_for_status()

        # g) Parse JSON
        data = response.json()

        # h) Check server-side success flag
        if data.get("success") is True:
            logger.info("IC memo sent successfully | to=%s", to)
            return data
        else:
            error_msg = data.get("error", "Unknown error from Gmail MCP server")
            logger.error("Gmail MCP server returned failure | error=%s", error_msg)
            return {"success": False, "error": error_msg}

    except httpx.TimeoutException:
        logger.error("Gmail MCP server timed out after %ss", DEFAULT_TIMEOUT)
        return {"success": False, "error": f"Request timed out after {DEFAULT_TIMEOUT}s"}

    except httpx.HTTPStatusError as e:
        logger.error("Gmail MCP HTTP error | status=%s", e.response.status_code)
        return {
            "success": False,
            "error": f"HTTP {e.response.status_code}: {e.response.text}",
        }

    except Exception as e:
        logger.exception("Unexpected error calling Gmail MCP server")
        return {"success": False, "error": str(e)}


# ── Secondary tool ────────────────────────────────────────────────────────────


def send_deal_alert(
    to: str,
    deal_name: str,
    alert_type: str,
    message: str,
    cc: Optional[list[str]] = None,
) -> dict:
    """
    Send a short deal alert notification email. Use for quick status updates
    such as risk flag escalations, deadline reminders, or deal stage changes.
    Not for full IC memo delivery — use send_ic_memo() for that.

    Args:
        to:         Recipient email address.
        deal_name:  Name of the deal this alert relates to, e.g. "AcmeCo".
        alert_type: Category of alert. One of: "RISK_FLAG", "DEADLINE",
                    "STAGE_CHANGE", "GENERAL".
        message:    Plain English description of the alert (1–3 sentences).
        cc:         Optional list of additional recipient email addresses.

    Returns:
        Same dict shape as send_ic_memo().
    """
    # ── a) Map alert_type to a header colour ─────────────────────────────────
    ALERT_COLORS = {
        "RISK_FLAG":    "#C0392B",
        "DEADLINE":     "#E67E22",
        "STAGE_CHANGE": "#2980B9",
        "GENERAL":      "#27AE60",
    }
    colour = ALERT_COLORS.get(alert_type, "#555555")

    # ── b) Build minimal HTML using only inline CSS ───────────────────────────
    html_body = (
        f'<html><body style="font-family:sans-serif;max-width:600px;margin:0 auto">'
        f'<div style="background:{colour};color:#fff;padding:16px 20px;border-radius:6px 6px 0 0">'
        f"<strong>DealBrain Alert \u2014 {alert_type}</strong>"
        f"</div>"
        f'<div style="border:1px solid #e0e0e0;border-top:none;padding:20px;border-radius:0 0 6px 6px">'
        f'<p style="font-size:15px;margin:0 0 12px"><strong>Deal:</strong> {deal_name}</p>'
        f'<p style="font-size:14px;color:#333;line-height:1.6;margin:0">{message}</p>'
        f"</div>"
        f'<p style="font-size:11px;color:#999;margin-top:12px">'
        f"Generated by DealBrain \u00b7 Powered by Gemini 2.0 Flash on GCP"
        f"</p>"
        f"</body></html>"
    )

    # ── c) Compose subject ────────────────────────────────────────────────────
    subject = f"[DealBrain] {alert_type} \u2014 {deal_name}"

    # ── d) Delegate to send_ic_memo() for actual delivery ─────────────────────
    return send_ic_memo(to=to, subject=subject, html_body=html_body, cc=cc)


# ── Utility ───────────────────────────────────────────────────────────────────


def check_gmail_mcp_health() -> dict:
    """
    Ping the Gmail MCP server health endpoint. Use before sending emails in
    critical workflows to confirm the MCP server is reachable.

    Returns:
        dict with keys:
          healthy (bool) — True if server responded with status "ok"
          status (str)   — raw status string from server, or error description
          method (str)   — transport method reported by server (e.g. "smtp")
    """
    try:
        response = httpx.get(f"{GMAIL_MCP_URL}/health", timeout=10.0)
        response.raise_for_status()
        data = response.json()
        return {
            "healthy": data.get("status") == "ok",
            "status": data.get("status", "unknown"),
            "method": data.get("method", "unknown"),
        }
    except Exception as e:
        return {"healthy": False, "status": str(e), "method": "unknown"}


# ── ADK tool registry ─────────────────────────────────────────────────────────
# Import this list in agents/manager.py and agents/reporting.py to register
# these as callable tools.
GMAIL_TOOLS = [
    send_ic_memo,
    send_deal_alert,
    check_gmail_mcp_health,
]
