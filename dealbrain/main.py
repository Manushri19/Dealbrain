import json
import logging
from contextlib import asynccontextmanager
import os 
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

# Import your master orchestrator and DB tools
from agents.manager import run_deal_pipeline
from tools.alloydb import init_pool, close_pool, get_deal_summary, get_pool

# Setup DB connection pool lifecycle
import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

log = logging.getLogger(__name__)

# Setup DB connection pool lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info(f"🚨 STARTUP: Connecting to Database at: {os.getenv('DATABASE_URL')} 🚨")
    
    try:
        log.info("Initializing AlloyDB connection pool...")
        await init_pool()
        log.info("✅ AlloyDB pool ready.")
    except Exception as exc:
        # CRITICAL FIX: Log the error but DO NOT CRASH
        log.error(f"❌ AlloyDB pool failed to initialize at startup: {exc}")
        
    yield
    
    from tools.alloydb import close_pool
    log.info("Closing AlloyDB connection pool...")
    await close_pool()

app = FastAPI(title="DealBrain API", version="1.0", lifespan=lifespan)

# Allow your frontend (React, Vue, etc.) to talk to this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", include_in_schema=False)
async def root():
    """Redirects the root URL straight to the testing docs."""
    return RedirectResponse(url="/docs")

@app.get("/health")
async def health_check():
    """Simple health check for Cloud Run or Docker."""
    return {"status": "ok", "message": "DealBrain API is running."}

@app.post("/analyze")
async def analyze_deal(
    file: UploadFile = File(...),
    metadata: str = Form(...)
):
    """
    Accepts a multipart form with a file upload and a JSON metadata string.
    Triggers the full DealBrain multi-agent pipeline.
    """
    try:
        # 1. Parse metadata JSON
        metadata_dict = json.loads(metadata)
        recipient_email = metadata_dict.get("recipient_email")
        
        if not recipient_email:
            raise ValueError("recipient_email is required in the metadata JSON.")

        # 2. Read and decode the uploaded file
        content = await file.read()
        doc_text = content.decode("utf-8")

        logging.info(f"Received deal analysis request for email: {recipient_email}")

        # 3. Execute the pipeline
        final_html = await run_deal_pipeline(
            doc_text=doc_text,
            recipient_email=recipient_email
        )
        
        # 4. Fetch the generated deal_id to return to the frontend
        pool = await get_pool()
        row = await pool.fetchrow("SELECT deal_id FROM deals ORDER BY created_at DESC LIMIT 1")
        deal_id = str(row["deal_id"]) if row else None

        return {
            "status": "success",
            "deal_id": deal_id,
            "memo_html": final_html
        }

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid metadata JSON string.")
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/deal/{deal_id}")
async def get_deal(deal_id: str):
    """
    Returns all stored data for a deal from AlloyDB.
    """
    try:
        data = await get_deal_summary(deal_id)
        
        if not data or not data.get("deal"):
            raise HTTPException(status_code=404, detail=f"Deal {deal_id} not found.")
            
        return data

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Failed to fetch deal {deal_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))