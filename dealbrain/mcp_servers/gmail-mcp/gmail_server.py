"""
DealBrain Gmail MCP Server
--------------------------
FastAPI service that sends structured HTML emails via the Gmail API using
Google service account credentials with domain-wide delegation.

Deployment target: Google Cloud Run (headless, no browser interaction).
Called by the DealBrain reporting agent to deliver IC memos to the deal team.
"""
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("gmail_server")

# Configuration from Environment (Securely handled by Cloud Run / Secret Manager)
GMAIL_USER = os.getenv("GMAIL_SENDER_ADDRESS")
GMAIL_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")

app = FastAPI(title="DealBrain Gmail MCP Server (SMTP)")

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class EmailRequest(BaseModel):
    to: EmailStr
    subject: str
    html_body: str
    cc: list[EmailStr] = []
    sender_name: str = "DealBrain"

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
async def health_check():
    return {"status": "ok", "method": "smtp"}

@app.post("/send_email")
async def send_email(request: EmailRequest):
    if not GMAIL_USER or not GMAIL_PASSWORD:
        return JSONResponse(status_code=500, content={"error": "Credentials not configured"})

    try:
        # 1. Create Message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = request.subject
        msg["To"] = request.to
        msg["From"] = f"{request.sender_name} <{GMAIL_USER}>"
        
        if request.cc:
            msg["Cc"] = ", ".join(request.cc)

        msg.attach(MIMEText(request.html_body, "html", "utf-8"))

        # 2. Send via SMTP
        # Gmail SMTP uses port 465 for SSL
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.send_message(msg)

        logger.info("Email sent successfully to %s", request.to)
        return {"success": True, "to": request.to, "subject": request.subject}

    except Exception as e:
        logger.exception("SMTP Error")
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

if __name__ == "__main__":
    uvicorn.run("gmail_server:app", host="0.0.0.0", port=8080)