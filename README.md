# 🏛️ DealBrain: M&A Due Diligence Intelligence Agent
DealBrain is a high-performance, multi-agent AI system designed to automate the grueling process of M&A due diligence. By leveraging Google's **Agent Development Kit (ADK)** and the **Gemini 2.5 Flash** model, DealBrain transforms weeks of manual document review into minutes of automated, high-fidelity analysis.
## 🚀 Overview
M&A analysts typically spend 3-6 weeks manually combing through data rooms (10,000+ documents) to surface deal risks. DealBrain ingests these documents, classifies them, extracts critical metadata, identifies financial and regulatory risks, benchmarks valuation, and generates structured investment memos—all in real-time.
---
## 🔄 End-to-End Workflow
The system follows a sophisticated orchestration pattern, delegating specialized tasks to dedicated sub-agents:
1.  **Ingestion & Classification**: A banker uploads deal documents (CIM, 10-K, contracts, financials) via the `/analyze` endpoint.
2.  **Orchestration**: The **Manager Agent (Gemini 2.5 Flash)** classifies the documents, extracts initial deal metadata, and delegates work to specialized sub-agents.
3.  **Specialized Analysis**:
    *   **Financial Risk Agent**: Extracts flags such as covenant breaches, debt triggers, and hidden liabilities.
    *   **Market Comps Agent**: Pulls historical benchmark data and calculates valuation multiples (EV/EBITDA).
    *   **Regulatory Agent**: Scans for antitrust concerns and sector-specific compliance blockers.
    *   **Timeline Agent**: Calculates M&A milestones and syncs them with **Google Calendar** via MCP.
4.  **Synthesis**: Data is persisted to **AlloyDB**, and the **Reporting Agent** synthesizes all findings into a structured HTML Investment Committee (IC) Memo.
5.  **Delivery**: The final IC Memo is delivered to the deal team via **Gmail** using the Gmail MCP server.
---
## 🏗️ GCP Infrastructure
DealBrain is built on a modern, serverless architecture on Google Cloud Platform:
*   **Compute**: FastAPI entry point and ADK orchestration layer running on **Cloud Run**.
*   **Database**: **AlloyDB for PostgreSQL** for high-performance structured data storage (deals, flags, comps, milestones).
*   **Storage**: **Cloud Storage** for raw deal documents.
*   **Intelligence**: **Gemini 2.5 Flash** providing the reasoning engine for all agents.
*   **Integration**: **MCP (Model Context Protocol)** servers for Gmail and Calendar, deployed as independent Cloud Run services.
---
## 📂 Project Structure
```text
dealbrain/
├── agents/             # Specialized AI Agents (Manager, Risk, Comps, etc.)
│   ├── financial_risk.py
│   ├── manager.py
│   ├── market_comps.py
│   ├── regulatory.py
│   ├── reporting.py
│   └── timeline.py
├── mcp_servers/        # Model Context Protocol server implementations
│   ├── calendar-mcp/
│   └── gmail-mcp/
├── tools/              # Shared utilities and data access layers
│   ├── alloydb.py      # AlloyDB (PostgreSQL) async client
│   ├── calendar_mcp.py # Calendar MCP wrapper
│   ├── gmail_mcp.py    # Gmail MCP wrapper
│   └── document_parser.py
├── main.py             # FastAPI entry point & API routes
├── Dockerfile          # Container configuration for Cloud Run
└── pyproject.toml      # Dependency management (uv)
```
---
## 🛠️ Technologies Involved
*   **Core**: Python 3.12+, FastAPI
*   **AI/LLM**: Google Generative AI (Gemini 2.5 Flash), Agent Development Kit (ADK)
*   **Database**: AlloyDB (PostgreSQL)
*   **Cloud**: Google Cloud Run, Cloud Storage, Secret Manager
*   **Tooling**: MCP (Model Context Protocol), `asyncpg` for database connectivity
---
## 🔌 API Reference
### `POST /analyze`
Triggers the full multi-agent pipeline for a new deal document.
*   **Request Type**: `multipart/form-data`
*   **Parameters**:
    *   `file`: The deal document (PDF or TXT).
    *   `metadata`: A JSON string containing:
        *   `recipient_email`: The email address where the final IC Memo should be sent.
*   **Response**:
    ```json
    {
      "status": "success",
      "deal_id": "uuid-string",
      "memo_html": "<html>...</html>"
    }
    ```
### `GET /deal/{deal_id}`
Retrieves the comprehensive summary for a specific deal.
*   **Input/Payload**: `deal_id` (string) provided as a path parameter.
*   **Response**: Returns a JSON object containing the deal metadata, risk flags, market comps, and milestones stored in AlloyDB.
---
## 🏛️ Database Schema
### `deals` Table
Central entity storing core metadata extracted by the Manager Agent.
```sql
CREATE TABLE deals (
    deal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target_company TEXT NOT NULL,
    acquirer TEXT,
    deal_type TEXT,
    enterprise_value NUMERIC, -- In millions
    status TEXT DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);
```
### `documents` Table
References to files uploaded for analysis.
```sql
CREATE TABLE documents (
    doc_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id UUID REFERENCES deals(deal_id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    content_type TEXT,
    file_path TEXT, -- Link to Cloud Storage
    uploaded_at TIMESTAMPTZ DEFAULT NOW()
);
```
### `risk_flags` Table
Populated by Risk/Regulatory Agents.
```sql
CREATE TABLE risk_flags (
    flag_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id UUID REFERENCES deals(deal_id) ON DELETE CASCADE,
    category TEXT, -- e.g., 'REGULATORY', 'LITIGATION'
    severity TEXT, -- 'HIGH', 'MEDIUM', 'LOW'
    description TEXT,
    source_doc TEXT, -- Text snippet trigger
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```
### `milestones` Table
Generated by the Timeline Agent and synced with Google Calendar.
```sql
CREATE TABLE milestones (
    milestone_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id UUID REFERENCES deals(deal_id) ON DELETE CASCADE,
    label TEXT NOT NULL,
    due_date DATE NOT NULL,
    calendar_event_id TEXT, -- ID from Google Calendar
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```
### `comps` Table
Benchmark data stored by the Marketplace Comps Agent.
```sql
CREATE TABLE comps (
    comp_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id UUID REFERENCES deals(deal_id) ON DELETE CASCADE,
    company_name TEXT NOT NULL,
    revenue_multiple NUMERIC,
    ebitda_multiple NUMERIC,
    source_year INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```
