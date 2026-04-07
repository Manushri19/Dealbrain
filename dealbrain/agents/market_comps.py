"""
agents/market_comps.py — DealBrain Market Comparables Agent
Runs in parallel with financial_risk and regulatory agents; called by manager agent.

OPTIMIZATION APPLIED: 
Uses ADK strictly for Extraction (High-IQ task). 
Bypasses LLM Tool-Calling for database inserts, using native async Python loops 
instead to achieve 0-token, 0-latency persistence.
"""

from __future__ import annotations

import logging
import sys
import uuid
import asyncio
from dotenv import load_dotenv
from google.genai import types
from pydantic import BaseModel, Field

# Removed SequentialAgent, we only need the LlmAgent now
from google.adk.agents.llm_agent import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data Schemas
# ---------------------------------------------------------------------------
class PeerComp(BaseModel):
    company: str
    ev_ebitda: float | None = None
    ev_revenue: float | None = None
    year: int | None = None
    source: str = Field(description="Must be 'estimated' or 'historical'")

class Benchmarking(BaseModel):
    ev_ebitda: float | None = None
    ev_revenue: float | None = None
    implied_ev_low: float | None = None
    implied_ev_high: float | None = None
    positioning: str = Field(description="Must be 'premium', 'in-line', or 'discount'")
    rationale: str = Field(description="Max 2 sentences")

class MarketCompsResult(BaseModel):
    target: str | None = None
    sector: str | None = None
    peer_comps: list[PeerComp] = []
    benchmarking: Benchmarking | None = None
    flags: list[str] = []
    error: str | None = Field(
        description="If sector is unknown, set to 'unknown_sector', else None",
        default=None
    )

# ---------------------------------------------------------------------------
# ADK Agents
# ---------------------------------------------------------------------------
EXTRACTION_INSTRUCTION = """
You are an M&A valuation analyst.
Given a target company, its sector, and historical comparable transactions from the user, return structured output.

Rules:
- peer_comps must include 3-6 companies. Use historical rows first; supplement with your knowledge if <3 exist.
- Mark supplemented comps as source: "estimated"
- implied_ev range = median multiples x target EBITDA (use sector average if unknown)
- flags = list of valuation risks (empty list if none)
- If the sector is unknown, return an object setting the `error` property to "unknown_sector".
"""

# NOTICE: PERSISTENCE_INSTRUCTION has been completely deleted to save tokens!

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
async def run_market_comps(deal_id: str, company: str, sector: str) -> dict:
    """Fetch historical comps, run ADK extraction, natively persist, return dict."""
    
    from tools.alloydb import insert_comp, get_comps_by_sector  # noqa: PLC0415
    load_dotenv()

    # 1. Query AlloyDB for historical comps by sector
    historical_comps: list[dict] = []
    try:
        historical_comps = await get_comps_by_sector(sector)
    except Exception as exc:
        logger.warning("AlloyDB comps query failed for sector=%s: %s", sector, exc)

    # 2. Format historical comps as compact context string
    if historical_comps:
        rows = [
            f"{c['peer_company']} | EV/EBITDA={c.get('ev_ebitda')} | EV/Revenue={c.get('ev_revenue')} | Year={c.get('year')}"
            for c in historical_comps
        ]
        historical_ctx = "Historical comps:\n" + "\n".join(rows)
    else:
        historical_ctx = "No historical comps available."

    user_message = (
        f"Target company: {company}\n"
        f"Sector: {sector}\n"
        f"{historical_ctx}\n"
    )

    # 3. Configure a single ADK Extraction Agent
    extractor = LlmAgent(
        name="ExtractionAgent",
        instruction=EXTRACTION_INSTRUCTION,
        output_schema=MarketCompsResult,
        output_key="market_comps_data"
    )

    # NO PIPELINE NEEDED: Pass the extractor directly into the Runner
    runner = Runner(
        app_name="dealbrain_comps",
        agent=extractor, 
        session_service=InMemorySessionService(),
        auto_create_session=True
    )

    # 4. Execute synchronously via ADK Runner
    extracted_data: MarketCompsResult | None = None
    
    try:
        session_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        
        # Because we aren't using async tools inside the ADK, 
        # a standard synchronous runner call works perfectly and safely.
        events_iter = runner.run(
            user_id=user_id,
            session_id=session_id,
            new_message=types.Content(parts=[types.Part.from_text(text=user_message)], role="user")
        )

        for event in events_iter:
            if event.actions and event.actions.state_delta:
                if "market_comps_data" in event.actions.state_delta:
                    delta_payload = event.actions.state_delta["market_comps_data"]
                    if delta_payload:
                        extracted_data = delta_payload

    except Exception as exc:
        logger.error("ADK runner execution failed for deal_id=%s: %s", deal_id, exc)
        return {
            "status": "error", "deal_id": deal_id, "company": company,
            "sector": sector, "message": "llm_call_failure",
        }

    if not extracted_data:
        return {"status": "error", "deal_id": deal_id, "company": company, "sector": sector, "message": "llm_parse_failure"}

    if getattr(extracted_data, 'error', None) == "unknown_sector":
        return {"status": "error", "deal_id": deal_id, "company": company, "sector": sector, "message": "unknown_sector"}

    # Safely convert to dictionary format
    dump = extracted_data if isinstance(extracted_data, dict) else extracted_data.model_dump()
    peers = dump.get("peer_comps", [])

    # =======================================================================
    # 5. THE FIX: Native Python Persistence (0 tokens, 0 LLM latency)
    # =======================================================================
    inserted_count = 0
    for peer in peers:
        try:
            await insert_comp(
                deal_id=deal_id,
                peer_company=peer.get("company"),
                ev_ebitda=peer.get("ev_ebitda"),
                ev_revenue=peer.get("ev_revenue"),
                year=peer.get("year"),
                sector=sector
            )
            inserted_count += 1
        except Exception as db_err:
            logger.error("Failed to insert native peer %s: %s", peer.get("company"), db_err)
    # =======================================================================

    return {
        "status": "success",
        "deal_id": deal_id,
        "company": company,
        "sector": sector,
        "benchmarking": dump.get("benchmarking", {}),
        "peer_comps": peers,
        "flags": dump.get("flags", []),
        "comps_inserted": inserted_count,
    }

# ---------------------------------------------------------------------------
# __main__ — standalone smoke test (Remains Unchanged)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import pprint
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

    DEAL_ID = "smoke-deal-001"
    COMPANY = "Acme Robotics"
    SECTOR = "Industrial Automation"

    import tools.alloydb as _alloydb_module  # noqa: E402

    async def _mock_get_comps_by_sector(sector: str) -> list[dict]:
        print(f"  [MOCK DB] get_comps_by_sector -> {sector}")
        return [
            {"peer_company": "Detroit Automatics", "ev_ebitda": 14.5, "ev_revenue": 3.2, "year": 2023, "sector": sector}
        ]

    async def _mock_insert_comp(
        deal_id: str,
        peer_company: str,
        ev_ebitda: float | None,
        ev_revenue: float | None,
        year: int | None,
        sector: str,
    ) -> str:
        mock_id = str(uuid.uuid4())
        print(f"  [MOCK DB] insert_comp -> {peer_company}  EBITDA={ev_ebitda} EV/Rev={ev_revenue} Sector={sector}")
        return mock_id

    _alloydb_module.get_comps_by_sector = _mock_get_comps_by_sector
    _alloydb_module.insert_comp = _mock_insert_comp

    async def _run_test() -> None:
        print("\n" + "=" * 60)
        print("  DealBrain · Market Comps ADK Agent — Native Python Save Test")
        print("=" * 60)
        print(f"  deal_id   : {DEAL_ID}")
        print(f"  company   : {COMPANY}")
        print(f"  sector    : {SECTOR}")
        print("=" * 60 + "\n")

        result = await run_market_comps(DEAL_ID, COMPANY, SECTOR)

        print("\n" + "=" * 60)
        print("  RESULT")
        print("=" * 60)
        pprint.pprint(result, sort_dicts=False)

    asyncio.run(_run_test())