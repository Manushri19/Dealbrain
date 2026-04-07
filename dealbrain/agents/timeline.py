"""
agents/timeline.py
──────────────────
Timeline Orchestrator Agent for the DealBrain M&A system.
Calculates standard M&A milestones from a target signing date and persists
them to both Google Calendar and AlloyDB.
"""

import uuid
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse as dateutil_parse

from tools.alloydb import insert_milestone
from tools.calendar_mcp import create_deal_milestone

logger = logging.getLogger("agents.timeline")

async def run(deal_id: str, deal_type: str, signing_target: str) -> list[dict]:
    """
    Calculate and schedule M&A milestones based on signing_target.

    Args:
        deal_id: UUID of the deal.
        deal_type: e.g., 'acquisition', 'asset_sale', 'merger'.
        signing_target: Target signing date in ISO 8601 format.

    Returns:
        A list of dictionaries representing the created milestones.
    """
    logger.info("Timeline Agent started | deal_id=%s | deal_type=%s | target=%s", 
                deal_id, deal_type, signing_target)
    
    # 1. Parse signing_target and format to UTC noon
    try:
        parsed_dt = dateutil_parse(signing_target)
        # Ensure it's exactly 12:00:00 UTC on the target day
        base_date = datetime(
            parsed_dt.year, 
            parsed_dt.month, 
            parsed_dt.day, 
            12, 0, 0, 
            tzinfo=timezone.utc
        )
    except Exception as e:
        logger.error("Failed to parse signing_target '%s': %s", signing_target, e)
        raise ValueError(f"Invalid signing_target date format: {signing_target}") from e

    # 2. Define schedule and calculate milestones
    schedule = [
        {"label": "LOI Execution", "offset": -63},
        {"label": "Exclusivity Start", "offset": -56},
        {"label": "Management Presentations", "offset": -42},
        {"label": "Final Due Diligence", "offset": -28},
        {"label": "Purchase Agreement Signing", "offset": 0},
        {"label": "Expected Closing", "offset": 45},
    ]

    if deal_type.strip().lower() == "merger":
        schedule.append({"label": "Proxy Filing", "offset": 14})
        
    schedule.sort(key=lambda x: x["offset"])

    # 3. Execution flow for each milestone
    results = []
    
    for milestone in schedule:
        target_date = base_date + timedelta(days=milestone["offset"])
        iso_date_str = target_date.isoformat()
        
        # Calendar Event formatting
        title = f"{milestone['label']} (Deal {deal_id[:8]})"
        description = f"M&A Milestone: {milestone['label']}\nDeal UUID: {deal_id}"
        duration_minutes = 60
        
        event_id = None
        
        # Step: Push to Google Calendar via synchronous wrapper in thread
        try:
            cal_result = await asyncio.to_thread(
                create_deal_milestone,
                title=title,
                date=iso_date_str,
                description=description,
                duration_minutes=duration_minutes
            )
            
            if cal_result and cal_result.get("success"):
                event_id = cal_result.get("event_id")
                logger.debug("Calendar event created for %s => %s", 
                             milestone['label'], event_id)
            else:
                err = cal_result.get("error") if cal_result else "Unknown Error"
                logger.warning("Calendar event failed for %s: %s", 
                               milestone['label'], err)
        except Exception as e:
            logger.error("Exception calling Calendar MCP for %s: %s", milestone["label"], e)

        # Step: Persist to AlloyDB
        db_milestone_id = None
        try:
            db_milestone_id = await insert_milestone(
                deal_id=deal_id,
                label=milestone['label'],
                due_date=target_date.date(),
                calendar_event_id=event_id
            )
            logger.debug("AlloyDB milestone created for %s => %s", 
                         milestone['label'], db_milestone_id)
        except Exception as e:
             logger.error("Exception calling AlloyDB for %s: %s", 
                          milestone['label'], e)

        # Append to return list
        results.append({
            "label": milestone["label"],
            "target_date": iso_date_str,
            "calendar_event_id": event_id,
            "db_milestone_id": db_milestone_id
        })

    logger.info("Timeline Agent complete | milestones processed=%d", len(results))
    return results

# ---------------------------------------------------------------------------
# __main__ — standalone smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    import pprint
    from pathlib import Path
    from dotenv import load_dotenv

    # Ensure project root is on sys.path when run directly
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
    # 1. Mock Calendar MCP: Prevent actual HTTP calls to the Calendar server
    def _mock_create_deal_milestone(title: str, date: str, description: str, duration_minutes: int) -> dict:
        mock_event_id = f"cal_evt_{str(uuid.uuid4())[:8]}"
        print(f"  [MOCK CALENDAR] Created Event: '{title}' on {date} (ID: {mock_event_id})")
        return {"success": True, "event_id": mock_event_id}

    # 2. Mock DB Write: Prevent actual inserts into AlloyDB
    async def _mock_insert_milestone(deal_id: str, label: str, due_date, calendar_event_id: str) -> str:
        mock_db_id = str(uuid.uuid4())
        print(f"  [MOCK DB] Inserted Milestone: '{label}' -> DB ID: {mock_db_id[:8]}...")
        return mock_db_id

    # Override the real imported functions directly in this file's namespace
    create_deal_milestone = _mock_create_deal_milestone
    insert_milestone = _mock_insert_milestone

    # ---- Run Test -----------------------------------------------------------
    async def _run_test() -> None:
        print("\n" + "=" * 65)
        print("  DealBrain · Timeline Agent — Smoke Test")
        print("=" * 65 + "\n")

        test_deal_id = str(uuid.uuid4())
        test_deal_type = "merger" # Using "merger" to test the conditional "Proxy Filing" milestone
        test_signing_target = "2026-10-31"

        try:
            # Run the agent
            result = await run(
                deal_id=test_deal_id,
                deal_type=test_deal_type,
                signing_target=test_signing_target
            )

            print("\n" + "=" * 65)
            print("✅ Smoke Test Complete!")
            print(f"✅ Successfully scheduled and saved {len(result)} milestones.")
            print("=" * 65 + "\n")
            
            print("Agent Return Payload:")
            pprint.pprint(result, sort_dicts=False)

        except Exception as e:
            print(f"\n❌ Test failed: {e}")

    asyncio.run(_run_test())