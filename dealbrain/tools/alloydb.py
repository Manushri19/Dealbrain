"""
----------------
Single data-access layer for all DealBrain agents.
No agent writes raw SQL — everything goes through these 8 async functions.

Database  : AlloyDB (PostgreSQL-compatible) via alloydb-auth-proxy
Driver    : asyncpg (no ORM / SQLAlchemy)
Pool      : created once at startup, min=2 / max=10 connections
"""

from decimal import Decimal
import asyncio
import json
import logging
import os
import uuid
from datetime import date, datetime
from typing import Optional

import asyncpg

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection pool — module-level singleton
# ---------------------------------------------------------------------------
# Dictionary to hold one connection pool per active thread/event loop
_pools = {}

async def init_pool():
    """Initializes the pool for the main Uvicorn thread on startup."""
    await get_pool()

async def get_pool():
    """
    Returns the connection pool for the CURRENT thread's event loop.
    If the ADK creates a background thread, this safely spins up a new mini-pool for it.
    """
    loop = asyncio.get_running_loop()
    
    if loop not in _pools:
        log.info(f"Creating new AlloyDB pool for thread loop {id(loop)}")
        _pools[loop] = await asyncpg.create_pool(
            os.getenv("DATABASE_URL"),
            min_size=1,
            max_size=10,
            timeout=10.0,
            command_timeout=10.0
        )
    
    return _pools[loop]

async def close_pool():
    """Closes all active pools across all threads."""
    for loop, pool in _pools.items():
        await pool.close()
    _pools.clear()
    log.info("All AlloyDB connection pools closed.")

# ---------------------------------------------------------------------------
# Helper: convert asyncpg Record → plain dict
# ---------------------------------------------------------------------------
def _record_to_dict(record: asyncpg.Record) -> dict:
    """Convert a single asyncpg Record to a plain Python dict."""
    return dict(record)

def _records_to_list(records) -> list[dict]:
    """Convert a list of asyncpg Records to a list of plain Python dicts."""
    return [dict(r) for r in records]


# ---------------------------------------------------------------------------
# 1. create_deal
# ---------------------------------------------------------------------------
async def create_deal(
    target_company: str,
    acquirer: str,
    deal_type: str,
    enterprise_value: float,
) -> str:
    deal_id = str(uuid.uuid4())
    sql = """
        INSERT INTO deals (deal_id, target_company, acquirer, deal_type, enterprise_value, status)
        VALUES ($1::uuid, $2, $3, $4, $5, 'ACTIVE')
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(sql, deal_id, target_company, acquirer, deal_type, enterprise_value)
        log.info("Created deal %s (%s / %s)", deal_id, target_company, acquirer)
        return deal_id
    except asyncpg.PostgresError as exc:
        log.error("create_deal failed: %s", exc)
        raise

# ---------------------------------------------------------------------------
# 2. update_deal_status
# ---------------------------------------------------------------------------
async def update_deal_status(deal_id: str, status: str) -> None:
    sql = """
        UPDATE deals
        SET    status     = $2,
               updated_at = NOW()
        WHERE  deal_id = $1::uuid
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(sql, deal_id, status)
        if result == "UPDATE 0":
            log.warning("update_deal_status: no deal found with id=%s", deal_id)
        else:
            log.info("Deal %s status → %s", deal_id, status)
    except asyncpg.PostgresError as exc:
        log.error("update_deal_status failed: %s", exc)
        raise

# ---------------------------------------------------------------------------
# 3. insert_document
# ---------------------------------------------------------------------------
async def insert_document(
    deal_id: str,
    doc_type: str,
    gcs_uri: str,
) -> str:
    doc_id = str(uuid.uuid4())
    sql = """
        INSERT INTO documents (doc_id, deal_id, doc_type, gcs_uri)
        VALUES ($1::uuid, $2::uuid, $3, $4)
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(sql, doc_id, deal_id, doc_type, gcs_uri)
        log.info("Inserted document %s [%s] for deal %s", doc_id, doc_type, deal_id)
        return doc_id
    except asyncpg.PostgresError as exc:
        log.error("insert_document failed: %s", exc)
        raise

# ---------------------------------------------------------------------------
# 4. insert_risk_flag
# ---------------------------------------------------------------------------
async def insert_risk_flag(
    deal_id: str,
    category: str,
    severity: str,
    description: str,
    source_doc: Optional[str] = None,
) -> str:
    flag_id = str(uuid.uuid4())
    sql = """
        INSERT INTO risk_flags (flag_id, deal_id, category, severity, description, source_doc)
        VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6)
        ON CONFLICT (deal_id, category, severity, description) DO NOTHING
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(sql, flag_id, deal_id, category, severity, description, source_doc)
        log.info("Inserted risk flag %s [%s/%s] for deal %s", flag_id, category, severity, deal_id)
        return flag_id
    except asyncpg.PostgresError as exc:
        log.error("insert_risk_flag failed: %s", exc)
        raise

# ---------------------------------------------------------------------------
# 5. insert_comp
# ---------------------------------------------------------------------------
async def insert_comp(
    deal_id: str,
    peer_company: str,
    ev_ebitda: float,
    ev_revenue: float,
    year: int,
    sector: str,
) -> str:
    comp_id = str(uuid.uuid4())
    sql = """
        INSERT INTO comps (comp_id, deal_id, peer_company, ev_ebitda, ev_revenue, year, sector)
        VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7)
        ON CONFLICT (deal_id, peer_company, year) DO NOTHING
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(sql, comp_id, deal_id, peer_company, ev_ebitda, ev_revenue, year, sector)
        log.info("Inserted comp %s [%s, %s, %s] for deal %s", comp_id, peer_company, sector, year, deal_id)
        return comp_id
    except asyncpg.PostgresError as exc:
        log.error("insert_comp failed: %s", exc)
        raise

# ---------------------------------------------------------------------------
# 6. insert_milestone
# ---------------------------------------------------------------------------
async def insert_milestone(
    deal_id: str,
    label: str,
    due_date: date,
    calendar_event_id: Optional[str] = None,
) -> str:
    milestone_id = str(uuid.uuid4())
    sql = """
        INSERT INTO milestones (milestone_id, deal_id, label, due_date, calendar_event_id)
        VALUES ($1::uuid, $2::uuid, $3, $4, $5)
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(sql, milestone_id, deal_id, label, due_date, calendar_event_id)
        log.info("Inserted milestone %s [%s] for deal %s", milestone_id, label, deal_id)
        return milestone_id
    except asyncpg.PostgresError as exc:
        log.error("insert_milestone failed: %s", exc)
        raise

# ---------------------------------------------------------------------------
# 7. get_deal_summary
# ---------------------------------------------------------------------------
async def get_deal_summary(deal_id: str) -> dict:
    sql_deal = "SELECT * FROM deals       WHERE deal_id = $1::uuid"
    sql_flags = "SELECT * FROM risk_flags WHERE deal_id = $1::uuid ORDER BY severity, category"
    sql_comps = "SELECT * FROM comps      WHERE deal_id = $1::uuid ORDER BY year DESC"
    sql_miles = "SELECT * FROM milestones WHERE deal_id = $1::uuid ORDER BY due_date"

    pool = await get_pool()

    try:
        deal_row, flag_rows, comp_rows, mile_rows = await asyncio.gather(
            pool.fetchrow(sql_deal, deal_id),
            pool.fetch(sql_flags, deal_id),
            pool.fetch(sql_comps, deal_id),
            pool.fetch(sql_miles, deal_id),
        )

        return {
            "deal":       _record_to_dict(deal_row) if deal_row else None,
            "risk_flags": _records_to_list(flag_rows),
            "comps":      _records_to_list(comp_rows),
            "milestones": _records_to_list(mile_rows),
        }
    except asyncpg.PostgresError as exc:
        log.error("get_deal_summary failed for deal %s: %s", deal_id, exc)
        raise

# ---------------------------------------------------------------------------
# 8. get_comps_by_sector
# ---------------------------------------------------------------------------
async def get_comps_by_sector(sector_keyword: str) -> list[dict]:
    sql = """
        SELECT * FROM comps
        WHERE  sector ILIKE $1
        ORDER  BY year DESC
    """
    pattern = f"%{sector_keyword}%"
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, pattern)
        log.info("get_comps_by_sector('%s') returned %d row(s)", sector_keyword, len(rows))
        return _records_to_list(rows)
    except asyncpg.PostgresError as exc:
        log.error("get_comps_by_sector failed: %s", exc)
        raise

# ---------------------------------------------------------------------------
# JSON serialisation helper (handles date/datetime/UUID fields from asyncpg)
# ---------------------------------------------------------------------------
def _json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, Decimal):
        return float(obj) 
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serialisable")

# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        stream=sys.stdout,
    )

    async def _smoke_test():
        print("\n=== DealBrain AlloyDB Smoke Test ===\n")
        await init_pool()

        deal_id = await create_deal(
            target_company="Acme Corp",
            acquirer="GlobalPE Fund III",
            deal_type="acquisition",
            enterprise_value=850_000_000,
        )
        print(f"[OK] Created deal: {deal_id}")

        flag1 = await insert_risk_flag(
            deal_id=deal_id,
            category="REGULATORY",
            severity="HIGH",
            description="Antitrust review expected in EU and US",
        )
        print(f"[OK] Inserted risk flag 1: {flag1}")

        flag2 = await insert_risk_flag(
            deal_id=deal_id,
            category="FINANCIAL",
            severity="MEDIUM",
            description="Revenue concentration in top 3 customers exceeds 60%",
            source_doc="cim_acme_2024.pdf",
        )
        print(f"[OK] Inserted risk flag 2: {flag2}")

        flag2_dup = await insert_risk_flag(
            deal_id=deal_id,
            category="FINANCIAL",
            severity="MEDIUM",
            description="Revenue concentration in top 3 customers exceeds 60%",
            source_doc="cim_acme_2024.pdf",
        )
        print(f"[OK] Duplicate risk flag silently ignored (new id generated: {flag2_dup})")

        await insert_comp(
            deal_id=deal_id,
            peer_company="Acme Software Ltd",
            ev_ebitda=14.5,
            ev_revenue=3.2,
            year=2023,
            sector="B2B SaaS"
        )
        print("[OK] Inserted comp")

        await insert_milestone(
            deal_id=deal_id,
            label="IOI Submissions",
            due_date=date(2025, 6, 15),
        )
        print("[OK] Inserted milestone")

        summary = await get_deal_summary(deal_id)
        print("\n=== get_deal_summary result ===")
        print(json.dumps(summary, indent=2, default=_json_default))

        comps = await get_comps_by_sector("Acme")
        print(f"\n=== get_comps_by_sector('Acme') → {len(comps)} row(s) ===")
        print(json.dumps(comps, indent=2, default=_json_default))

        await close_pool()
        print("\n=== Smoke test complete ===\n")
        
    asyncio.run(_smoke_test())