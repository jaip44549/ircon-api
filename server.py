"""FastAPI application for case management reporting system."""
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from typing import List, Dict, Any
import uuid
import os
import tempfile
import time
import asyncio

from dotenv import load_dotenv
load_dotenv()

from src.services.report_service import ReportService
from src.logger import get_logger
from src.exceptions import BaseAppException
from src.models.request_models import ConsolidatedReportRequest

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown."""
    logger.info("Application starting up...")

    async def _periodic_cleanup():
        while True:
            await asyncio.sleep(300)  # run every 5 minutes
            _cleanup_pdf_store()

    task = asyncio.create_task(_periodic_cleanup())
    yield
    task.cancel()
    logger.info("Application shutting down...")


app = FastAPI(
    title="Case Management Reporting System",
    description="Production-ready case management and reporting API",
    version="2.0.0",
    lifespan=lifespan,
    root_path="/api/legal-intelligence"
)

templates = Jinja2Templates(directory="templates")
report_service = ReportService()

# In-memory store for temporary PDF files: token -> (file_path, expires_at)
_PDF_TTL = 600  # seconds (10 minutes)
_pdf_store: Dict[str, tuple[str, float]] = {}


def _store_pdf(file_path: str) -> str:
    """Store a PDF path with a TTL and return its access token."""
    token = str(uuid.uuid4())
    _pdf_store[token] = (file_path, time.monotonic() + _PDF_TTL)
    return token


def _cleanup_pdf_store() -> None:
    """Remove expired entries and delete their temp files from disk."""
    now = time.monotonic()
    expired = [t for t, (_, exp) in _pdf_store.items() if now > exp]
    for token in expired:
        file_path, _ = _pdf_store.pop(token)
        try:
            os.unlink(file_path)
        except OSError:
            pass
    if expired:
        logger.info(f"PDF store: cleaned up {len(expired)} expired entries")


@app.exception_handler(BaseAppException)
async def app_exception_handler(request: Request, exc: BaseAppException):
    """Handle application-specific exceptions."""
    logger.error(f"Application error: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": str(exc),
            "type": exc.__class__.__name__
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions."""
    logger.error(f"Unexpected error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc)
        }
    )


def get_table_configs(report_type: str = "all") -> List[Dict[str, Any]]:
    """Get table configurations for different report types."""
    all_configs = {
        "arb_lit_contractor": {
            "type": "arb_lit_contractor",
            "title": "Over all position of Arbitration and Court cases with Contractors"
        },
        "arb_contractor": {
            "type": "arb_contractor",
            "title": "Over all position of Arbitration cases with Contractors"
        },
        "rev_arb_contractor": {
            "type": "rev_arb_contractor",
            "title": "Details of Arbitration cases with Contractors Accreted/Revised"
        },
        "close_arb_contractor": {
            "type": "close_arb_contractor",
            "title": "Details of Arbitration cases with Contractors Closed"
        },
        "court_contractor": {
            "type": "court_contractor",
            "title": "Over all position of Court cases with Contractors"
        },
        "rev_court_contractor": {
            "type": "rev_court_contractor",
            "title": "Details of Court cases with Contractors Accretion"
        },
        "close_court_contractor": {
            "type": "close_court_contractor",
            "title": "Details of Court cases with Contractors Closed"
        },
        "arb_lit_client": {
            "type": "arb_lit_client",
            "title": "Over all position of Arbitration and Court cases with Clients"
        },
        "close_court_client": {
            "type": "close_court_client",
            "title": "Details of Court cases with Client Closed"
        },
    }
    
    if report_type == "all":
        return list(all_configs.values())
    
    config = all_configs.get(report_type)
    return [config] if config else []


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    logger.info("Health check requested")
    return {"status": "healthy", "version": "2.0.0"}


@app.post("/v1/report", response_class=HTMLResponse)
@app.post("/v1/consolidated-report", response_class=HTMLResponse)
async def generate_consolidated_report(request: Request, request_data: ConsolidatedReportRequest):
    """
    Generate consolidated report from provided case data and return HTML.
    
    Supports both V1 (two tables) and V2 (single table with quarter) formats.
    """
    try:
        logger.info("Generating consolidated report (HTML)")
        if request_data.is_v2_format():
            logger.info(f"V2 format: Received {len(request_data.tbl_cases)} cases with quarter field")
        else:
            logger.info(f"V1 format: Received {len(request_data.tbl_cases)} cases and {len(request_data.tbl_case_past or [])} past cases")
        
        table_configs = get_table_configs("all")
        tables = report_service.build_report_tables(request_data, table_configs)
        
        return templates.TemplateResponse(
            request=request,
            name="report.html",
            context={"tables": tables}
        )
    except Exception as e:
        logger.error(f"Error generating consolidated report: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/report/{report_type}", response_class=HTMLResponse)
async def generate_specific_report(request: Request, report_type: str, request_data: ConsolidatedReportRequest):
    """
    Generate a specific report from provided case data and return HTML.
    
    Supports both V1 (two tables) and V2 (single table with quarter) formats.
    
    Available report types:
    - arb-lit-contractor
    - arb-contractor
    - court-contractor
    - rev-arb-contractor
    - close-arb-contractor
    - rev-court-contractor
    - close-court-contractor
    - arb-lit-client
    - close-court-client
    """
    try:
        logger.info(f"Generating report (HTML): {report_type}")
        
        # Convert URL format to config key
        config_key = report_type.replace("-", "_")
        table_configs = get_table_configs(config_key)
        
        if not table_configs:
            logger.warning(f"Unknown report type: {report_type}")
            raise HTTPException(status_code=404, detail=f"Report type '{report_type}' not found")
        
        tables = report_service.build_report_tables(request_data, table_configs)
        
        return templates.TemplateResponse(
            request=request,
            name="report.html",
            context={"tables": tables}
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating report '{report_type}': {e}")
        raise HTTPException(status_code=500, detail=str(e))

# V2 HTML endpoints (single table with quarter field)
@app.get("/v2/report/pdf/{token}.pdf")
async def download_pdf(token: str):
    """Download a previously generated PDF report by token."""
    entry = _pdf_store.get(token)
    if not entry:
        raise HTTPException(status_code=404, detail="PDF not found or expired")
    file_path, expires_at = entry
    if time.monotonic() > expires_at or not os.path.exists(file_path):
        _pdf_store.pop(token, None)
        raise HTTPException(status_code=404, detail="PDF not found or expired")
    return FileResponse(
        path=file_path,
        media_type="application/pdf",
        filename="report.pdf",
        headers={"Content-Disposition": "attachment; filename=report.pdf"}
    )


_BOOTSTRAP_CSS_URL = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"
_BOOTSTRAP_CSS: str | None = None


def _get_bootstrap_css() -> str:
    """Fetch and cache Bootstrap CSS for PDF rendering."""
    global _BOOTSTRAP_CSS
    if _BOOTSTRAP_CSS is None:
        import urllib.request
        with urllib.request.urlopen(_BOOTSTRAP_CSS_URL) as resp:
            _BOOTSTRAP_CSS = resp.read().decode("utf-8")
        logger.info("Bootstrap CSS fetched and cached for PDF rendering")
    return _BOOTSTRAP_CSS


_PDF_PAGE_CSS = """
@page {
    size: A4 landscape;
    margin: 12mm 10mm;
}
body {
    font-size: 11px;
}
.table {
    width: 100%;
    border-collapse: collapse;
}
.table-container {
    margin-bottom: 16px;
}
h2 {
    font-size: 13px;
    margin-bottom: 6px;
}
thead th, tbody th {
    background-color: #dbeafe !important;
    color: #1e3a5f;
}
"""


def _generate_pdf(html_content: str) -> str:
    """Render HTML string to a PDF temp file, return the file path."""
    from weasyprint import HTML
    full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>{_get_bootstrap_css()}</style>
<style>{_PDF_PAGE_CSS}</style>
</head>
<body>{html_content}</body>
</html>"""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    HTML(string=full_html).write_pdf(tmp.name)
    return tmp.name


@app.post("/v2/report", response_class=HTMLResponse)
@app.post("/v2/consolidated-report", response_class=HTMLResponse)
async def generate_consolidated_report_v2(request: Request, request_data: ConsolidatedReportRequest):
    """
    Generate consolidated report from provided case data (HTML response) - V2 API.
    
    V2 uses single table with quarter column (QC/QL) instead of separate tables.
    Returns HTML report.
    """
    try:
        logger.info("Generating consolidated report V2 (HTML)")
        logger.info(f"Received {len(request_data.tbl_cases)} cases")
        
        table_configs = get_table_configs("all")
        tables = report_service.build_report_tables(request_data, table_configs)

        # Generate PDF and store with a token
        raw_html = templates.get_template("report.html").render({"tables": tables, "request": request})
        token = _store_pdf(_generate_pdf(raw_html))
        base_url = str(request.base_url).rstrip("/")
        root_path = request.scope.get("root_path", "").rstrip("/")
        pdf_url = f"{base_url}{root_path}/v2/report/pdf/{token}.pdf"

        return templates.TemplateResponse(
            request=request,
            name="report.html",
            context={"tables": tables, "pdf_url": pdf_url}
        )
    except Exception as e:
        logger.error(f"Error generating consolidated report V2: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v2/report/{report_type}", response_class=HTMLResponse)
async def generate_specific_report_v2(request: Request, report_type: str, request_data: ConsolidatedReportRequest):
    """
    Generate a specific report from provided case data (HTML response) - V2 API.
    
    V2 uses single table with quarter column (QC/QL) instead of separate tables.
    Returns HTML report.
    
    Available report types:
    - arb-lit-contractor
    - arb-contractor
    - court-contractor
    - rev-arb-contractor
    - close-arb-contractor
    - rev-court-contractor
    - close-court-contractor
    - arb-lit-client
    - close-court-client
    """
    try:
        logger.info(f"Generating report V2 (HTML): {report_type}")
        
        # Convert URL format to config key
        config_key = report_type.replace("-", "_")
        table_configs = get_table_configs(config_key)
        
        if not table_configs:
            logger.warning(f"Unknown report type: {report_type}")
            raise HTTPException(status_code=404, detail=f"Report type '{report_type}' not found")
        
        tables = report_service.build_report_tables(request_data, table_configs)

        # Generate PDF and store with a token
        raw_html = templates.get_template("report.html").render({"tables": tables, "request": request})
        token = _store_pdf(_generate_pdf(raw_html))
        base_url = str(request.base_url).rstrip("/")
        root_path = request.scope.get("root_path", "").rstrip("/")
        pdf_url = f"{base_url}{root_path}/v2/report/pdf/{token}.pdf"

        return templates.TemplateResponse(
            request=request,
            name="report.html",
            context={"tables": tables, "pdf_url": pdf_url}
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating report V2 '{report_type}': {e}")
        raise HTTPException(status_code=500, detail=str(e))