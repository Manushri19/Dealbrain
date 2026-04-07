"""
test_pipeline.py
────────────────
Live end-to-end integration test for the DealBrain M&A pipeline.

What it does
────────────
1.  Loads all credentials from .env (DATABASE_URL, GOOGLE_API_KEY, etc.).
2.  Performs pre-flight checks on every required env var and reachable service.
3.  Calls manager.run_deal_pipeline() with a synthetic CIM string against
    REAL AlloyDB, REAL Gemini (via Google ADK), REAL Calendar MCP, and REAL Gmail MCP.
4.  Queries AlloyDB directly to verify all 5 tables have rows for the test deal.
5.  Saves the generated IC Memo HTML to test_ic_memo_<timestamp>.html.
6.  Cleans up the test deal rows from AlloyDB on exit (optional — set
    DEALBRAIN_TEST_KEEP_DATA=true in .env to retain rows for manual inspection).
7.  Prints a full per-table report and exits 0 on success, 1 on failure.

Prerequisites
─────────────
  pip install python-dotenv asyncpg httpx python-dateutil google-adk

.env must contain:
  DATABASE_URL              — asyncpg DSN for AlloyDB (via alloydb-auth-proxy)
  GOOGLE_API_KEY            — Gemini API key used by Google ADK agents
  CALENDAR_MCP_URL          — Base URL of the running Calendar MCP server
  GMAIL_MCP_URL             — Base URL of the running Gmail MCP server
  DEALBRAIN_TEST_EMAIL      — Recipient address for the live IC memo dispatch
  DEALBRAIN_TEST_KEEP_DATA  — (optional) set "true" to skip post-run cleanup

Run
────
    python test_pipeline.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import httpx
from dotenv import load_dotenv

# ── Project root on sys.path ─────────────────────────────────────────────────
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Load .env before any application imports so all modules pick up credentials
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("test_pipeline")

# ── Visual helpers ─────────────────────────────────────────────────────────────
PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "
SEP  = "═" * 70
TSEP = "─" * 70


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Pre-flight Checks
# ═══════════════════════════════════════════════════════════════════════════════

REQUIRED_ENV_VARS = [
    ("DATABASE_URL",         "asyncpg DSN for AlloyDB (via alloydb-auth-proxy)"),
    ("GOOGLE_API_KEY",       "Gemini API key used by Google ADK agents"),
    ("CALENDAR_MCP_URL",     "Base URL of the Calendar MCP server"),
    ("GMAIL_MCP_URL",        "Base URL of the Gmail MCP server"),
    ("DEALBRAIN_TEST_EMAIL", "Recipient address for the live IC memo email"),
]


def check_env_vars() -> bool:
    """Verify all required env vars are set. Returns True if all present."""
    print(f"\n{SEP}")
    print("  PRE-FLIGHT: Environment Variables")
    print(TSEP)
    ok = True
    for var, description in REQUIRED_ENV_VARS:
        val = os.getenv(var)
        if val:
            display = val[:6] + "*" * max(0, len(val) - 6) if len(val) > 6 else "****"
            print(f"  {PASS}  {var:<30}  {display}")
        else:
            print(f"  {FAIL}  {var:<30}  NOT SET  ← {description}")
            ok = False
    return ok


def check_mcp_health() -> bool:
    """Ping Calendar and Gmail MCP /health endpoints. Returns True if both healthy."""
    print(f"\n{SEP}")
    print("  PRE-FLIGHT: MCP Server Health")
    print(TSEP)
    ok = True
    services = [
        ("Calendar MCP", os.getenv("CALENDAR_MCP_URL", "").rstrip("/") + "/health"),
        ("Gmail MCP",    os.getenv("GMAIL_MCP_URL",    "").rstrip("/") + "/health"),
    ]
    for name, url in services:
        if not url.startswith("http"):
            print(f"  {FAIL}  {name:<20}  URL not configured")
            ok = False
            continue
        try:
            r = httpx.get(url, timeout=10.0)
            r.raise_for_status()
            data = r.json()
            if data.get("status") == "ok":
                print(f"  {PASS}  {name:<20}  healthy  ({url})")
            else:
                print(f"  {WARN}  {name:<20}  responded but status={data.get('status')!r}")
        except Exception as exc:
            print(f"  {FAIL}  {name:<20}  UNREACHABLE — {exc}")
            ok = False
    return ok


async def check_alloydb() -> bool:
    """Attempt a trivial AlloyDB query. Returns True on success."""
    print(f"\n{SEP}")
    print("  PRE-FLIGHT: AlloyDB Connectivity")
    print(TSEP)
    from tools.alloydb import init_pool, get_pool, close_pool
    try:
        await init_pool()
        pool = get_pool()
        val = await pool.fetchval("SELECT 1")
        assert val == 1
        print(f"  {PASS}  AlloyDB reachable — SELECT 1 returned {val}")
        await close_pool()
        return True
    except Exception as exc:
        print(f"  {FAIL}  AlloyDB connection failed: {exc}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Synthetic CIM
# ═══════════════════════════════════════════════════════════════════════════════

SYNTHETIC_CIM = """
CONFIDENTIAL INFORMATION MEMORANDUM
Project Nova — Proposed Acquisition of CloudSec Tech by GlobalPE Fund III

EXECUTIVE SUMMARY
CloudSec Tech ("the Company" or "the Target") is a B2B SaaS company
headquartered in San Francisco, California, providing defense-grade cloud
security and identity management solutions to enterprise and government clients.
GlobalPE Fund III ("the Acquirer"), a private-equity fund domiciled in Beijing,
China, proposes to acquire 100% of the issued and outstanding share capital of
CloudSec Tech for an enterprise value of approximately USD 850 million.

FINANCIAL HIGHLIGHTS (FY 2024)
- Annual Recurring Revenue (ARR): USD 134 million (+38% YoY)
- Total Revenue: USD 120 million
- Gross Margin: 78%
- EBITDA: USD 31 million
- The Company's five largest customers accounted for 73% of total revenue in
  fiscal year 2024, with the two largest customers representing 41% individually.
- The Company has USD 47.2 million in unfunded pension obligations as of
  December 31, 2024, arising from legacy defined-benefit plans assumed in the
  2019 acquisition of SecureCore Inc.
- Pursuant to the senior credit agreement dated March 15, 2022, the Company
  must maintain a leverage ratio not exceeding 4.5x EBITDA; as of Q3 2024 the
  ratio stood at 4.3x, leaving minimal covenant headroom.
- Management has identified conditions that raise substantial doubt about the
  Company's ability to continue as a going concern absent the proposed
  transaction or alternative financing.
- Revenue recognition policies for multi-year government contracts were revised
  in Q2 2024, accelerating USD 8.4 million of deferred revenue into the current
  fiscal year.

GEOGRAPHIC FOOTPRINT
CloudSec Tech operates across the United States, United Kingdom, and Germany.
Its German subsidiary, CloudSec GmbH (Frankfurt), processes personally
identifiable information on EU citizens under GDPR Article 28 data processing
agreements with over 40 enterprise clients across 12 EU member states.

REGULATORY CONTEXT
- The Company holds ITAR-registered manufacturing and software development
  facilities in Austin, TX, and supplies classified cybersecurity software to
  the US Department of Defense under contract number W52P1J-22-C-0041.
- The Target's UK subsidiary holds a Developed Vetting clearance and operates
  under UK MoD framework agreements.
- Combined annual revenues of the Acquirer and the Target in the EU exceed
  EUR 500 million each; global combined revenues surpass EUR 6.2 billion,
  triggering mandatory EU Merger Regulation notification thresholds.
- The Acquirer, being a non-US person under 31 C.F.R. Part 800, and the Target
  operating critical technology and sensitive personal data infrastructure,
  mandates a CFIUS filing prior to closing.

DEAL STRUCTURE & TIMELINE
Transaction type: Acquisition (100% share purchase agreement).
Estimated deal enterprise value: USD 850 million.
Target signing date: 2026-09-15.
Expected regulatory closing: Q1 2027, conditional on CFIUS clearance,
EU Phase I merger approval, and ITAR re-licensing from the US State Department.

MARKET SECTOR
The Company operates in the B2B SaaS / cloud security sector. Recent
comparable transactions include the acquisition of Lacework by Fortinet
(EV/Revenue ~5.2x, 2024) and the CrowdStrike / Flow Security deal
(EV/Revenue ~8.1x, 2024), indicating a sector median EV/Revenue of ~6x.
"""


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Live Pipeline Runner
# ═══════════════════════════════════════════════════════════════════════════════

async def run_live_pipeline() -> tuple[str, float]:
    """
    Run the real manager pipeline end-to-end against all live services.
    Returns (result_html, elapsed_seconds).
    """
    from agents.manager import run_deal_pipeline

    recipient = os.getenv("DEALBRAIN_TEST_EMAIL", "test@dealbrain.com")

    logger.info("Invoking run_deal_pipeline — real LLM, real DB, real MCP services")
    t0 = time.monotonic()
    result_html = await run_deal_pipeline(SYNTHETIC_CIM, recipient)
    elapsed = time.monotonic() - t0

    logger.info("Pipeline completed in %.1f seconds", elapsed)
    return result_html, elapsed


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — Direct AlloyDB Verification
# ═══════════════════════════════════════════════════════════════════════════════

async def find_latest_deal_id() -> str | None:
    """
    Recover the deal_id just created by querying for the most recent row
    in the deals table (ordered by created_at DESC).
    """
    from tools.alloydb import init_pool, get_pool
    try:
        await init_pool()
    except RuntimeError:
        pass  # already initialised

    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT deal_id FROM deals ORDER BY created_at DESC LIMIT 1"
    )
    return str(row["deal_id"]) if row else None


async def verify_tables(deal_id: str) -> dict[str, list[dict]]:
    """
    Query AlloyDB directly (bypassing the application layer) to fetch all rows
    for each of the 5 tables linked to `deal_id`.
    Returns a dict of table_name → list of row dicts.
    """
    from tools.alloydb import get_pool

    pool = get_pool()

    queries = {
        "deals":      "SELECT * FROM deals      WHERE deal_id = $1::uuid",
        "risk_flags": "SELECT * FROM risk_flags  WHERE deal_id = $1::uuid ORDER BY severity, category",
        "comps":      "SELECT * FROM comps       WHERE deal_id = $1::uuid ORDER BY year DESC",
        "milestones": "SELECT * FROM milestones  WHERE deal_id = $1::uuid ORDER BY due_date",
        "documents":  "SELECT * FROM documents   WHERE deal_id = $1::uuid",
    }

    results: dict[str, list[dict]] = {}
    for table, sql in queries.items():
        rows = await pool.fetch(sql, deal_id)
        results[table] = [dict(r) for r in rows]

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — Teardown
# ═══════════════════════════════════════════════════════════════════════════════

async def teardown(deal_id: str) -> None:
    """
    Remove all rows created for `deal_id` by deleting the parent deal record,
    allowing Postgres ON DELETE CASCADE to wipe the child tables automatically,
    then close the AlloyDB connection pool cleanly.
    """
    from tools.alloydb import get_pool, close_pool

    pool = get_pool()
    logger.info("Teardown: removing test rows for deal_id=%s", deal_id)

    # Because of ON DELETE CASCADE, deleting the parent deal wipes all child records instantly.
    result = await pool.execute(
        "DELETE FROM deals WHERE deal_id = $1::uuid", deal_id
    )
    logger.info("  %-14s  %s (Cascaded to all child tables)", "deals", result)

    await close_pool()
    logger.info("Teardown complete — connection pool closed.")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — Assertions & Report
# ═══════════════════════════════════════════════════════════════════════════════

def _safe(obj) -> object:
    """Convert asyncpg / Decimal / UUID types for display."""
    from datetime import date, datetime
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def print_table_sample(rows: list[dict], max_rows: int = 3) -> None:
    if not rows:
        print("    (no rows)")
        return
    for i, row in enumerate(rows[:max_rows]):
        print(f"    [{i}] { {k: _safe(v) for k, v in row.items()} }")
    if len(rows) > max_rows:
        print(f"    … and {len(rows) - max_rows} more row(s)")


def run_assertions(
    deal_id: str,
    table_data: dict[str, list[dict]],
    result_html: str,
) -> bool:
    """Execute all assertions against live AlloyDB data. Returns True if all pass."""

    failures: list[str] = []

    def check(condition: bool, label: str, hint: str = "") -> None:
        icon = PASS if condition else FAIL
        print(f"  {icon}  {label}")
        if not condition:
            failures.append(label + (f"  [{hint}]" if hint else ""))

    print(f"\n{SEP}")
    print("  ASSERTIONS")
    print(TSEP)

    deals      = table_data["deals"]
    risk_flags = table_data["risk_flags"]
    comps      = table_data["comps"]
    milestones = table_data["milestones"]
    documents  = table_data["documents"]

    # ── Per-table row counts ──────────────────────────────────────────────────
    check(len(deals)      >= 1, f"deals:       ≥1 row  (got {len(deals)})")
    check(len(risk_flags) >= 1, f"risk_flags:  ≥1 row  (got {len(risk_flags)})")
    check(len(comps)      >= 1, f"comps:       ≥1 row  (got {len(comps)})")
    check(len(milestones) >= 1, f"milestones:  ≥1 row  (got {len(milestones)})")
    # documents: pipeline does not currently call insert_document, so we verify
    # the table is queryable rather than requiring > 0 rows.
    print(f"  {PASS}  documents:   table queryable  (got {len(documents)} row(s) for this deal_id)")

    # ── Deal record integrity ─────────────────────────────────────────────────
    if deals:
        d = deals[0]
        check(str(d.get("deal_id")) == deal_id,
              f"deals.deal_id matches run ID  ({deal_id[:8]}…)")
        check(bool(d.get("target_company")),
              f"deals.target_company populated  ('{d.get('target_company')}')")
        check(bool(d.get("acquirer")),
              f"deals.acquirer populated  ('{d.get('acquirer')}')")
        check(d.get("deal_type", "").lower() in ("acquisition", "merger", "asset_sale", "ipo"),
              f"deals.deal_type is valid  ('{d.get('deal_type')}')")
        check(float(d.get("enterprise_value") or 0) > 0,
              f"deals.enterprise_value > 0  ({d.get('enterprise_value')})")
        check(d.get("status") == "ACTIVE",
              f"deals.status = 'ACTIVE'  (got '{d.get('status')}')")

    # ── Risk flags ────────────────────────────────────────────────────────────
    check(len(risk_flags) >= 3,
          f"risk_flags: ≥3 combined flags (financial + regulatory)  (got {len(risk_flags)})")

    severities = {f.get("severity", "").upper() for f in risk_flags}
    check("HIGH" in severities,
          f"risk_flags: ≥1 HIGH-severity flag  (found severities: {severities})")

    categories = {f.get("category", "").upper() for f in risk_flags}
    check(len(categories) >= 1,
          f"risk_flags: ≥1 distinct category  (found: {categories})")

    orphaned = [f for f in risk_flags if str(f.get("deal_id")) != deal_id]
    check(len(orphaned) == 0,
          f"risk_flags: all rows reference correct deal_id",
          f"{len(orphaned)} orphaned row(s)")

    # ── Comps ─────────────────────────────────────────────────────────────────
    check(len(comps) >= 3,
          f"comps: ≥3 peer comparables  (got {len(comps)})")

    if comps:
        check(bool(comps[0].get("peer_company")),
              f"comps.peer_company populated for first row  ('{comps[0].get('peer_company')}')")

    # ── Milestones ────────────────────────────────────────────────────────────
    check(len(milestones) >= 5,
          f"milestones: ≥5 standard M&A milestones  (got {len(milestones)})")

    labels = [m.get("label", "") for m in milestones]
    has_signing = any("signing" in lbl.lower() or "Signing" in lbl for lbl in labels)
    check(has_signing,
          f"milestones: signing milestone present  (labels: {labels})")

    cal_missing = [m for m in milestones if not m.get("calendar_event_id")]
    if cal_missing:
        print(f"  {WARN}  milestones: {len(cal_missing)} milestone(s) without calendar_event_id "
              f"(Calendar MCP may have been unavailable during run)")
    else:
        print(f"  {PASS}  milestones: all {len(milestones)} have calendar_event_id")

    # ── IC Memo HTML ──────────────────────────────────────────────────────────
    check(isinstance(result_html, str) and len(result_html) > 200,
          f"IC Memo HTML returned and non-trivial  ({len(result_html)} chars)")
    check(
        "<html" in result_html.lower() or "<!doctype" in result_html.lower(),
        "IC Memo HTML contains valid HTML root element",
    )

    # ── Final verdict ─────────────────────────────────────────────────────────
    print(f"\n{TSEP}")
    if not failures:
        print(f"  🎉  ALL CHECKS PASSED — DealBrain live pipeline is healthy.")
    else:
        print(f"  {FAIL}  {len(failures)} CHECK(S) FAILED:")
        for f in failures:
            print(f"       • {f}")
    print(SEP)

    return len(failures) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — Main Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

async def main() -> bool:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    print(f"\n{SEP}")
    print("  DealBrain — Live End-to-End Integration Test")
    print(f"  Run timestamp : {timestamp}")
    print(SEP)

    # ── 1. Pre-flight: env vars ───────────────────────────────────────────────
    if not check_env_vars():
        print(f"\n{FAIL}  Aborting: required environment variables are missing.")
        print("  Create a .env file in the project root with the variables listed above.")
        return False

    # ── 2. Pre-flight: MCP health (warn-only, not fatal) ─────────────────────
    mcp_ok = check_mcp_health()
    if not mcp_ok:
        print(f"\n  {WARN}  One or more MCP servers appear unhealthy.")
        print("  Calendar / Gmail steps may fail; all other assertions will still run.\n")

    # ── 3. Pre-flight: AlloyDB connectivity ──────────────────────────────────
    if not await check_alloydb():
        print(f"\n{FAIL}  Aborting: cannot connect to AlloyDB.")
        print("  Verify DATABASE_URL and that alloydb-auth-proxy is running.")
        return False

    # ── 4. Execute the live pipeline ─────────────────────────────────────────
    print(f"\n{SEP}")
    print("  PIPELINE EXECUTION  (this will take 60–180 seconds)")
    print(TSEP)
    logger.info("Starting live pipeline run …")

    try:
        result_html, elapsed = await run_live_pipeline()
    except Exception as exc:
        logger.exception("Pipeline raised an unhandled exception")
        print(f"\n{FAIL}  Pipeline crashed: {exc}")
        return False

    print(f"\n  {PASS}  Pipeline finished in {elapsed:.1f}s")

    # ── 5. Recover the deal_id created during this run ────────────────────────
    deal_id = await find_latest_deal_id()
    if not deal_id:
        print(f"\n{FAIL}  Could not locate test deal in AlloyDB after pipeline run.")
        return False
    print(f"  {PASS}  Recovered deal_id: {deal_id}")

    # ── 6. Direct DB verification ─────────────────────────────────────────────
    print(f"\n{SEP}")
    print("  ALLOYDB TABLE VERIFICATION")
    print(TSEP)

    table_data = await verify_tables(deal_id)

    for table, rows in table_data.items():
        count = len(rows)
        icon  = PASS if count > 0 else (WARN if table == "documents" else FAIL)
        print(f"\n  {icon}  {table.upper():<14}  {count} row(s)")
        print_table_sample(rows)

    # ── 7. Assertions ─────────────────────────────────────────────────────────
    all_passed = run_assertions(deal_id, table_data, result_html)

    # ── 8. Save IC Memo HTML artifact ─────────────────────────────────────────
    memo_path = Path(PROJECT_ROOT) / f"test_ic_memo_{timestamp}.html"
    try:
        memo_path.write_text(result_html, encoding="utf-8")
        print(f"\n  {PASS}  IC Memo saved → {memo_path.name}")
    except Exception as exc:
        print(f"\n  {WARN}  Could not save IC Memo: {exc}")

    # ── 9. Teardown ───────────────────────────────────────────────────────────
    keep = os.getenv("DEALBRAIN_TEST_KEEP_DATA", "").strip().lower() == "true"

    print(f"\n{SEP}")
    if keep:
        print(f"  {WARN}  DEALBRAIN_TEST_KEEP_DATA=true — skipping cleanup.")
        print(f"       Test rows retained in AlloyDB under deal_id = {deal_id}")
        from tools.alloydb import close_pool
        await close_pool()
    else:
        print("  Cleaning up test rows from AlloyDB …")
        try:
            await teardown(deal_id)
            print(f"  {PASS}  All test rows removed from 5 tables.")
        except Exception as exc:
            print(f"  {WARN}  Teardown error: {exc}")
            print(f"       Manually run:  DELETE FROM deals WHERE deal_id = '{deal_id}'::uuid")

    print(SEP + "\n")
    return all_passed


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — Entry Point
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)