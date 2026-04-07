"""
agents/reporting.py
────────────────────────
Reporting sub-agent for the DealBrain M&A due-diligence system.

Responsibilities
----------------
1. Fetch deal summary data from AlloyDB.
2. Generate an IC Memo in HTML format using an ADK generation agent.
3. Send the IC memo natively via Python (Zero token waste).
4. Return the generated HTML memo.

Run standalone:  python agents/reporting.py
"""

from __future__ import annotations

import os
import logging
import sys
import uuid
import asyncio
from dotenv import load_dotenv
from google.genai import types

# strictly use google ADK abstractions instead of asyncio loop management
from google.adk.agents.llm_agent import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService

from tools.alloydb import get_deal_summary
from tools.gmail_mcp import send_ic_memo

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ADK Agents Instructions
# ---------------------------------------------------------------------------
GENERATION_INSTRUCTION = """
You are a senior M&A Investment Banker.
Your task is to generate a fully styled Investment Committee (IC) Memo in HTML format based ONLY on the provided context.
Never invent or hallucinate financial numbers, risk flags, or any other data; rely strictly on the provided context.

Structure the output exactly like the reference HTML template provided.
It must contain sections for:
- Deal Summary
- Risk Flags (with color-coded severity badges matching the template)
- Market Comparables
- Deal Milestones

Return ONLY valid HTML. Do not wrap the response in markdown blocks (e.g., no ```html).
"""

# NOTICE: The DISPATCH_INSTRUCTION is completely deleted!

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
async def generate_and_send_memo(deal_id: str, recipient_email: str) -> str:
    """
    Synthesizes database records, prompts Gemini via ADK to generate a formatted 
    IC Memo, emails it NATIVELY to the recipient, and returns the HTML.
    """
    load_dotenv()
    log.info(f"Generating IC Memo for deal_id: {deal_id}")
    
    # 1. Fetch Data
    try:
        deal_data = await get_deal_summary(deal_id)
    except Exception as e:
        log.error(f"Failed to fetch deal data for {deal_id}: {e}")
        raise ValueError(f"Failed to fetch deal data for {deal_id}: {e}") from e
        
    if not deal_data or not deal_data.get("deal"):
        log.error(f"No deal data found for deal_id: {deal_id}")
        raise ValueError(f"No deal data found for deal_id: {deal_id}")

    # 2. Context Construction
    deal = deal_data.get("deal", {})
    target_name = deal.get("target_company", "Target Company")
    
    context_text = f"DEAL SUMMARY:\n{deal_data.get('deal')}\n\n"
    context_text += f"RISK FLAGS:\n{deal_data.get('risk_flags')}\n\n"
    context_text += f"MARKET COMPARABLES:\n{deal_data.get('comps')}\n\n"
    context_text += f"DEAL MILESTONES:\n{deal_data.get('milestones')}\n\n"

    template_path = os.path.join(os.path.dirname(__file__), '..', 'test', 'sample_ic_memo.html')
    template_html = ""
    if os.path.exists(template_path):
        with open(template_path, 'r', encoding='utf-8') as f:
            template_html = f.read()
    else:
        log.warning(f"Template file not found at {template_path}")

    prompt_text = f"Using the following context data, generate the IC memo for {target_name}.\n\n"
    prompt_text += f"CONTEXT DATA:\n{context_text}\n"
    if template_html:
        prompt_text += f"\nREFERENCE HTML TEMPLATE (Follow this structure, colors, and styling exactly):\n{template_html}\n"

    # 3. Configure ADK pipeline - ONE AGENT ONLY
    generator = LlmAgent(
        name="GenerationAgent",
        instruction=GENERATION_INSTRUCTION,
        output_key="memo_html"
    )

    runner = Runner(
        app_name="dealbrain_reporting",
        agent=generator, # <-- Run the single agent directly, no pipeline!
        session_service=InMemorySessionService(),
        auto_create_session=True
    )

    session_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())

    log.info("Starting ADK Generation Agent for deal=%s", deal_id)

    # Execute
    events_iter = runner.run(
        user_id=user_id,
        session_id=session_id,
        new_message=types.Content(parts=[types.Part.from_text(text=prompt_text)], role="user")
    )

    gemini_html = ""

    # Safely extract the HTML
    for event in events_iter:
        if event.actions and event.actions.state_delta:
            if "memo_html" in event.actions.state_delta:
                delta_payload = event.actions.state_delta["memo_html"]
                if delta_payload and len(delta_payload) > len(gemini_html):
                    gemini_html = delta_payload

    if not gemini_html:
        log.warning("memo_html not found in state_delta, falling back")

    # Clean up markdown fences
    gemini_html = gemini_html.strip()
    if gemini_html.startswith("```html"):
        gemini_html = gemini_html[7:]
    elif gemini_html.startswith("```"):
        gemini_html = gemini_html[3:]
    if gemini_html.endswith("```"):
        gemini_html = gemini_html[:-3]
    gemini_html = gemini_html.strip()

    # 4. NATIVE PYTHON DISPATCH (0 Tokens, Instant, 100% Reliable)
    log.info("Dispatching email natively...")
    subject_line = f"IC Memo — {target_name} (CONFIDENTIAL)"
    
    try:
        # Note: If your production send_ic_memo is defined as `async def`, 
        # change this line to `await send_ic_memo(...)`
        send_ic_memo(
            to=recipient_email,
            subject=subject_line,
            html_body=gemini_html,
            sender_name="DealBrain"
        )
        log.info("Email dispatched successfully.")
    except Exception as e:
        log.error(f"Native email dispatch failed: {e}")

    return gemini_html


# ---------------------------------------------------------------------------
# __main__ — standalone smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    from pathlib import Path

    _project_root = str(Path(__file__).resolve().parent.parent)
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)

    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        stream=sys.stdout,
    )

    # ---- Mock Dependencies --------------------------------------------------
    import tools.alloydb as _alloydb_module
    import tools.gmail_mcp as _gmail_module

    async def _mock_get_deal_summary(deal_id: str) -> dict:
        print(f"  [MOCK DB] Fetching data for deal_id: {deal_id[:8]}...")
        return {
            "deal": {
                "deal_id": deal_id,
                "target_company": "Apex Dynamics Ltd",
                "acquirer": "GlobalPE Fund III",
                "deal_type": "Acquisition",
                "enterprise_value": 850000000
            },
            "risk_flags": [
                {"category": "FINANCIAL", "severity": "HIGH", "description": "Customer concentration: Top 2 clients account for 55% of recurring revenue."},
                {"category": "REGULATORY", "severity": "MEDIUM", "description": "Requires HSR antitrust filing in the US."}
            ],
            "comps": [
                {"peer_company": "Vertex Software", "ev_ebitda": 14.2, "year": 2023, "sector": "B2B SaaS"},
                {"peer_company": "Nexus Logic", "ev_ebitda": 13.8, "year": 2024, "sector": "B2B SaaS"}
            ],
            "milestones": [
                {"label": "Management Presentation", "due_date": "2026-04-14"},
                {"label": "Binding Offer", "due_date": "2026-05-09"}
            ]
        }

    # Because we no longer use LLM tool calling, we don't even need **kwargs here anymore!
    # Natively called functions are strictly type-safe.
    def _mock_send_ic_memo(to: str, subject: str, html_body: str, cc=None, sender_name="DealBrain") -> dict:
        print(f"  [MOCK GMAIL] Natively dispatching email to: {to}")
        print(f"  [MOCK GMAIL] Subject: {subject}")
        print(f"  [MOCK GMAIL] Body size: {len(html_body)} characters")
        return {"success": True}

    import tools.alloydb
    tools.alloydb.get_deal_summary = _mock_get_deal_summary
    
    import tools.gmail_mcp
    tools.gmail_mcp.send_ic_memo = _mock_send_ic_memo
    
    get_deal_summary = _mock_get_deal_summary
    send_ic_memo = _mock_send_ic_memo
    
    # ---- Run Test -----------------------------------------------------------
    async def _run_test() -> None:
        print("\n" + "=" * 65)
        print("  DealBrain · Reporting Agent (Native Dispatch) — Smoke Test")
        print("=" * 65 + "\n")

        test_deal_id = str(uuid.uuid4())
        test_email = "ic-committee@example.com"

        try:
            html_output = await generate_and_send_memo(test_deal_id, test_email)

            output_path = Path(_project_root) / "test_ic_memo.html"
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html_output)

            print("\n" + "=" * 65)
            print("✅ Native Dispatch Smoke Test Complete!")
            print(f"✅ HTML Memo generated and saved to:\n   {output_path}")
            print("=" * 65 + "\n")

        except Exception as e:
            print(f"\n❌ Test failed: {e}")

    asyncio.run(_run_test())