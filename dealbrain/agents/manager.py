"""
agents/manager.py
─────────────────
The Master Orchestrator for the DealBrain M&A system.
Uses a Google ADK LlmAgent to extract the "Mega-JSON" deal context,
creates the master database record, and routes data to sub-agents in parallel.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import uuid
from typing import Any
# Import our custom tools and sub-agents
from tools.alloydb import create_deal, init_pool, insert_document
from agents import financial_risk, market_comps, regulatory, timeline, reporting

from dotenv import load_dotenv
from google.genai import types
from pydantic import BaseModel, Field

# ADK Imports for the Manager's Brain
from google.adk.agents.llm_agent import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService



logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data Schema (The "Mega-JSON")
# ---------------------------------------------------------------------------
class DealMetadata(BaseModel):
    target_company: str = Field(description="Name of the target company")
    acquirer: str = Field(description="Name of the acquiring company")
    deal_type: str = Field(description="Must be strictly: acquisition, merger, asset_sale, or ipo")
    sector: str = Field(description="Industry sector, e.g., Defense/Aerospace, B2B SaaS")
    estimated_deal_value_usd_m: float = Field(description="Estimated value in millions USD")
    acquirer_hq: str = Field(description="Headquarters country of the acquirer")
    target_hq: str = Field(description="Headquarters country of the target")
    operating_geographies: list[str] = Field(description="List of countries where the target operates")
    target_revenue_usd_m: float = Field(description="Target's revenue in millions USD")
    acquirer_revenue_usd_m: float = Field(description="Acquirer's revenue in millions USD")
    target_market_share_pct: float = Field(description="Target's market share percentage. 0 if unknown.")
    signing_target_date: str = Field(description="Target signing date in ISO 8601 (YYYY-MM-DD) format.")
    regulatory_excerpts: list[str] = Field(description="3-5 verbatim sentences relevant to antitrust or FDI review.")

# ---------------------------------------------------------------------------
# ADK Agent Definition
# ---------------------------------------------------------------------------
EXTRACTION_INSTRUCTION = """
You are the Lead M&A Pipeline Manager for DealBrain.
Your job is to read a raw deal document and extract ALL essential metadata into a single, comprehensive structured object.

RULES:
1. Return ONLY valid data matching the schema.
2. If a specific number or string is missing, infer it reasonably based on the text. If inference is impossible, use 0 for numbers or 'Unknown' for strings.
3. For "signing_target_date", if no date is explicitly mentioned, calculate a date exactly 60 days from today in YYYY-MM-DD format.
"""

# ---------------------------------------------------------------------------
# Orchestrator Execution
# ---------------------------------------------------------------------------
async def run_deal_pipeline(doc_text: str, recipient_email: str) -> str:
    """The master controller: extracts data, routes payloads, and runs the pipeline."""
    load_dotenv()
    logger.info("Manager Agent: Initializing pipeline...")

    # 0. Initialize the Database Connection Pool
    logger.info("Connecting to AlloyDB...")
    try:
        await init_pool()
    except Exception as e:
        logger.warning(f"Pool initialization note: {e}")

    # 1. ADK Extraction Phase
    manager_agent = LlmAgent(
        name="OrchestratorAgent",
        instruction=EXTRACTION_INSTRUCTION,
        output_schema=DealMetadata,
        output_key="deal_metadata"
    )

    runner = Runner(
        app_name="dealbrain_manager",
        agent=manager_agent,
        session_service=InMemorySessionService(),
        auto_create_session=True
    )

    logger.info("Manager Agent: Extracting Mega-JSON from document...")
    session_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    
    events_iter = runner.run(
        user_id=user_id,
        session_id=session_id,
        new_message=types.Content(parts=[types.Part.from_text(text=f"DOCUMENT TEXT:\n\n{doc_text}")], role="user")
    )

    extracted_metadata: DealMetadata | None = None
    for event in events_iter:
        if event.actions and event.actions.state_delta:
            if "deal_metadata" in event.actions.state_delta:
                payload = event.actions.state_delta["deal_metadata"]
                if payload:
                    extracted_metadata = payload

    if not extracted_metadata:
        raise RuntimeError("Manager Agent failed to extract deal metadata from the document.")

    # Convert Pydantic model to dictionary for easy routing
    metadata_dict = extracted_metadata if isinstance(extracted_metadata, dict) else extracted_metadata.model_dump()

    # 2. Database Initialization
    deal_id = await create_deal(
        target_company=metadata_dict.get("target_company", "Unknown Target"),
        acquirer=metadata_dict.get("acquirer", "Unknown Acquirer"),
        deal_type=metadata_dict.get("deal_type", "acquisition").lower(),
        enterprise_value=metadata_dict.get("estimated_deal_value_usd_m", 0.0)
    )
    logger.info(f"Manager Agent: Master deal record created -> {deal_id}")

    await insert_document(
        deal_id=deal_id,
        doc_type="CIM",
        gcs_uri="synthetic_memory://cim_document.txt"
    )
    logger.info("Manager Agent: Document record linked to deal.")
    
    # 3. Prepare Payload for Regulatory Agent
    reg_payload = {
        "deal_id": deal_id,
        "target": metadata_dict.get("target_company"),
        "acquirer": metadata_dict.get("acquirer"),
        "acquirer_hq": metadata_dict.get("acquirer_hq"),
        "target_hq": metadata_dict.get("target_hq"),
        "operating_geographies": metadata_dict.get("operating_geographies", []),
        "sector": metadata_dict.get("sector"),
        "target_revenue_usd_m": metadata_dict.get("target_revenue_usd_m"),
        "acquirer_revenue_usd_m": metadata_dict.get("acquirer_revenue_usd_m"),
        "estimated_deal_value_usd_m": metadata_dict.get("estimated_deal_value_usd_m"),
        "target_market_share_pct": metadata_dict.get("target_market_share_pct"),
        "document_excerpts": metadata_dict.get("regulatory_excerpts", [])
    }

    # 4. Dispatch Parallel Sub-Agents
    logger.info("Manager Agent: Dispatching Financial, Regulatory, and Comps agents in parallel...")
    await asyncio.gather(
        financial_risk.analyze(deal_id, doc_text),
        regulatory.analyze(reg_payload),
        market_comps.run_market_comps(
            deal_id, 
            metadata_dict.get("target_company", ""), 
            metadata_dict.get("sector", "")
        ),
        return_exceptions=True
    )

    # 5. Dispatch Sequential Timeline Agent
    logger.info("Manager Agent: Parallel agents finished. Triggering Timeline...")
    await timeline.run(
        deal_id=deal_id,
        deal_type=metadata_dict.get("deal_type", "acquisition"),
        signing_target=metadata_dict.get("signing_target_date", "2026-10-31")
    )

    # 6. Dispatch Sequential Reporting Agent
    logger.info("Manager Agent: Triggering Reporting and Dispatch...")
    final_html = await reporting.generate_and_send_memo(deal_id, recipient_email)
    
    logger.info("Manager Agent: DealBrain Pipeline execution complete.")
    return final_html


# ---------------------------------------------------------------------------
# __main__ — Standalone Smoke Test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    from pathlib import Path
    
    _project_root = str(Path(__file__).resolve().parent.parent)
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", stream=sys.stdout)

    MOCK_CIM = """
    CONFIDENTIAL INFORMATION MEMORANDUM
    Project Nova - Acquisition of CloudSec Tech by GlobalPE Fund III.
    CloudSec Tech is a B2B SaaS company headquartered in the United States, operating across the US, UK, and Germany. 
    They specialize in defense-grade cloud security. GlobalPE Fund III is headquartered in China.
    Financials: CloudSec generated $120M in revenue last year. GlobalPE Fund III has global revenues exceeding $5B.
    The estimated deal value is $850M. We are targeting a signing date of 2026-09-15.
    Regulatory note: The company processes data for EU citizens and supplies software to the US Department of Defense.
    """

    import tools.alloydb as db
    async def mock_create_deal(*args, **kwargs):
        print(f"\n  [MOCK ALLOYDB] Created Deal -> ID: mock-deal-123 | Target: {kwargs.get('target_company')}")
        return "mock-deal-123"
        
    db.create_deal = mock_create_deal
    
    # THE FIX: Explicitly overwrite the locally imported reference!
    global create_deal
    create_deal = mock_create_deal

    # Mock the agents so we can test routing without burning API tokens
    async def mock_financial(*args, **kwargs): print("  [AGENT RUN] Financial Risk complete.")
    async def mock_regulatory(*args, **kwargs): print("  [AGENT RUN] Regulatory Risk complete.")
    async def mock_market_comps(*args, **kwargs): print("  [AGENT RUN] Market Comps complete.")
    async def mock_timeline(*args, **kwargs): print("  [AGENT RUN] Timeline scheduled.")
    async def mock_reporting(*args, **kwargs):
        print(f"  [AGENT RUN] Report sent to {args[1]}")
        return "<html><body><h1>Mock IC Memo</h1></body></html>"

    financial_risk.analyze = mock_financial
    regulatory.analyze = mock_regulatory
    market_comps.run_market_comps = mock_market_comps
    timeline.run = mock_timeline
    reporting.generate_and_send_memo = mock_reporting

    async def _run_test():
        print("="*65)
        print(" DealBrain · Master Orchestrator Smoke Test")
        print("="*65)
        await run_deal_pipeline(MOCK_CIM, "committee@dealbrain.com")
        print("="*65)

    asyncio.run(_run_test())