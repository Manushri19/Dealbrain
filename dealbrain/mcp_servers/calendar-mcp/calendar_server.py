"""
DealBrain — Google Calendar MCP Server
Exposes POST /create_event for ADK agents to schedule Calendar events via
a GCP service account. Runs headlessly on Cloud Run (no OAuth browser flow).
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
load_dotenv()  # no-op when .env is absent (Cloud Run)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("calendar_server")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCOPES = ["https://www.googleapis.com/auth/calendar"]
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
CALENDAR_ID = os.getenv("GOOGLE_CALENDAR_ID", "primary")

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="DealBrain Calendar MCP Server",
    description="Creates Google Calendar events for M&A due-diligence milestones.",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------
class CreateEventRequest(BaseModel):
    title: str
    date: str  # ISO 8601 — e.g. "2025-06-15T10:00:00"
    description: str
    duration_minutes: int = 60


# ---------------------------------------------------------------------------
# Google Calendar helper
# ---------------------------------------------------------------------------
import google.auth
from googleapiclient.discovery import build

def _build_calendar_service():
    # This checks for local 'gcloud login' credentials OR Cloud Run's identity automatically
    credentials, project = google.auth.default(scopes=SCOPES)
    return build("calendar", "v3", credentials=credentials)


def _calculate_end_time(start_iso: str, duration_minutes: int) -> str:
    """
    Parse an ISO 8601 datetime string, add duration_minutes, and return the
    result as an ISO 8601 string. Assumes UTC when no timezone is supplied.
    """
    start_dt = datetime.fromisoformat(start_iso)

    # If the datetime is naive, treat it as UTC
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)

    end_dt = start_dt + timedelta(minutes=duration_minutes)
    return end_dt.isoformat()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health", summary="Liveness probe for Cloud Run")
def health_check():
    """Returns 200 OK so Cloud Run knows the container is ready."""
    return {"status": "ok"}


@app.post("/create_event", summary="Create a Google Calendar event")
def create_event(request: CreateEventRequest):
    """
    Creates a Google Calendar event using service-account credentials.

    - **title**: Event summary shown in Calendar UI
    - **date**: ISO 8601 start datetime (UTC assumed when no tz offset given)
    - **description**: Body text / agenda for the event
    - **duration_minutes**: Length of the event in minutes (default 60)
    """
    logger.info(
        "Incoming create_event request — title=%r, date=%r, duration=%d min",
        request.title,
        request.date,
        request.duration_minutes,
    )

    try:
        service = _build_calendar_service()

        end_iso = _calculate_end_time(request.date, request.duration_minutes)

        event_body = {
            "summary": request.title,
            "description": request.description,
            "start": {
                "dateTime": request.date,
                "timeZone": "UTC",
            },
            "end": {
                "dateTime": end_iso,
                "timeZone": "UTC",
            },
        }

        created_event = (
            service.events()
            .insert(calendarId=CALENDAR_ID, body=event_body)
            .execute()
        )

        logger.info(
            "Event created successfully — id=%s, link=%s",
            created_event.get("id"),
            created_event.get("htmlLink"),
        )

        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "event_id": created_event.get("id"),
                "event_link": created_event.get("htmlLink"),
                "title": request.title,
                "date": request.date,
            },
        )

    except Exception as exc:  # noqa: BLE001 — intentional catch-all
        logger.exception(
            "Failed to create calendar event for title=%r, date=%r",
            request.title,
            request.date,
        )
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(exc),
            },
        )


# ---------------------------------------------------------------------------
# Entry point (local dev only — Cloud Run uses the CMD in Dockerfile)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run("calendar_server:app", host="0.0.0.0", port=8080, reload=False)
