"""
agents/financial_risk.py
────────────────────────
Financial-risk sub-agent for the DealBrain M&A due-diligence system.

Responsibilities
----------------
1. Extract ALL financial risk flags using an ADK LlmAgent and structured output schema.
2. Natively persist the flags to AlloyDB using standard Python (Zero token waste).
3. Return the parsed and persisted objects.

Run standalone:  python agents/financial_risk.py
"""

from __future__ import annotations

import logging
import sys
import uuid
import asyncio
from dotenv import load_dotenv
from google.genai import types
from pydantic import BaseModel, Field

# ADK abstractions
from google.adk.agents.llm_agent import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data Schemas
# ---------------------------------------------------------------------------
class RiskFlag(BaseModel):
    flag_id: str = Field(description="Unique string, e.g. a short slug", default_factory=lambda: str(uuid.uuid4())[:8])
    category: str = Field(
        description="Must be one of: hidden_liability, covenant_breach, earn_out_trigger, customer_concentration, pension_deficit, revenue_recognition, related_party, off_balance_sheet, going_concern, litigation_exposure"
    )
    severity: str = Field(description="HIGH | MEDIUM | LOW")
    description: str = Field(description="Concise explanation of the risk, <= 80 words")
    source_doc: str = Field(description="Filename or document title if identifiable, else 'unknown'", default="unknown")
    source_excerpt: str = Field(description="Verbatim quote <= 30 words from the input text that proves this flag")

# ---------------------------------------------------------------------------
# ADK Agents (Optimized: Extraction ONLY)
# ---------------------------------------------------------------------------
EXTRACTION_INSTRUCTION = """
You are a senior M&A financial-risk analyst.
Your task: extract ALL financial risk flags from the deal document text supplied by the user.

RULES:
1. Return ONLY a valid JSON array matching the required output schema.
2. If no flags are found, return an empty array: []

SEVERITY GUIDE:
  HIGH   — imminent, material, deal-breaking risk
  MEDIUM — significant but manageable; warrants negotiation or indemnity
  LOW    — minor; flag for awareness only

Do NOT invent flags that are not evidenced by the text.
"""

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
async def analyze(deal_id: str, document_text: str) -> list[dict]:
    """Extract flags via ADK, and persist them natively."""
    from tools.alloydb import insert_risk_flag  # noqa: PLC0415
    load_dotenv()

    # 1. Configure ADK Extraction Agent natively
    extractor = LlmAgent(
        name="ExtractionAgent",
        model="gemini-2.5-flash",
        instruction=EXTRACTION_INSTRUCTION,
        output_schema=list[RiskFlag],
        output_key="extracted_flags"
    )

    # Note: No SequentialAgent needed! We just run the extractor.
    runner = Runner(
        app_name="dealbrain_risk",
        agent=extractor,
        session_service=InMemorySessionService(),
        auto_create_session=True
    )

    log.info("Starting ADK extraction for deal=%s  doc_len=%d chars", deal_id, len(document_text))

    prompt_text = f"Analyze the following deal document text:\n\nDOCUMENT:\n{document_text}"
    session_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())

    extracted_data = []

    # 2. Execute ADK Runner to get structured JSON
    events_iter = runner.run(
        user_id=user_id,
        session_id=session_id,
        new_message=types.Content(parts=[types.Part.from_text(text=prompt_text)], role="user")
    )

    for event in events_iter:
        if event.actions and event.actions.state_delta:
            if "extracted_flags" in event.actions.state_delta:
                delta_payload = event.actions.state_delta["extracted_flags"]
                if delta_payload:
                    extracted_data = delta_payload

    # 3. NATIVE PYTHON PERSISTENCE (Zero tokens used!)
    # We loop over the clean objects returned by the LLM and insert them directly.
    result = []
    for item in extracted_data:
        # Normalize to dict
        flag_dict = item if isinstance(item, dict) else item.model_dump()
        result.append(flag_dict)
        
        # Call the asynchronous database tool natively
        try:
            await insert_risk_flag(
                deal_id=deal_id,
                category=flag_dict.get("category", "unknown"),
                severity=flag_dict.get("severity", "LOW"),
                description=flag_dict.get("description", ""),
                source_doc=flag_dict.get("source_excerpt", None)
            )
        except Exception as e:
            log.error(f"Failed to persist flag to AlloyDB: {e}")

    log.info(f"Successfully extracted and persisted {len(result)} flags.")
    return result


# ---------------------------------------------------------------------------
# __main__ — standalone smoke test
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

    DEAL_ID = "test-deal-001"
    TEST_TEXT = (
        "The Company's five largest customers accounted for 73% of total revenue in fiscal 2024. "
        "The Company has $47.2M in unfunded pension obligations as of December 31, 2024. "
        "Pursuant to the credit agreement, the Company must maintain a leverage ratio not exceeding "
        "4.5x EBITDA; as of Q3 2024 the ratio stood at 4.3x. Management has identified conditions "
        "that raise substantial doubt about the Company's ability to continue as a going concern."
    )

    # Mock DB Tool
    import tools.alloydb as _alloydb_module  # noqa: E402

    async def _mock_insert_risk_flag(
        deal_id: str,
        category: str,
        severity: str,
        description: str,
        source_doc: str | None = None,
    ) -> str:
        mock_id = str(uuid.uuid4())
        print(f"  [MOCK DB] Natively inserted flag -> {mock_id[:8]} | Cat={category} | Sev={severity}")
        return mock_id

    _alloydb_module.insert_risk_flag = _mock_insert_risk_flag

    async def _run_test() -> None:
        print("\n" + "=" * 60)
        print("  DealBrain · Optimized Financial Risk Agent — Smoke Test")
        print("=" * 60)
        
        flags = await analyze(DEAL_ID, TEST_TEXT)

        print("\n" + "=" * 60)
        print(f"  FLAGS RETURNED: {len(flags)}")
        print("=" * 60)
        pprint.pprint(flags, sort_dicts=False)

        categories = {f.get("category", "").lower() for f in flags}
        expected = {"customer_concentration", "pension_deficit", "covenant_breach", "going_concern"}
        missing = expected - categories
        if missing:
            print(f"\n⚠  WARNING: expected categories not found: {missing}")
        else:
            print(f"\n✅  All {len(expected)} expected flag types detected.")

        assert len(flags) >= 4, f"Expected >= 4 flags, got {len(flags)}"

    asyncio.run(_run_test())