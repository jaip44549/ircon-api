"""FastAPI application for case management reporting system."""
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from typing import List, Dict, Any

from dotenv import load_dotenv
load_dotenv()

from src.services.report_service import ReportService
from src.logger import get_logger
from src.exceptions import BaseAppException
from src.models.request_models import ConsolidatedReportRequest
from src.models.response_models import ReportResponse, ErrorResponse, HealthResponse

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown."""
    logger.info("Application starting up...")
    yield
    logger.info("Application shutting down...")


app = FastAPI(
    title="Case Management Reporting System",
    description="Production-ready case management and reporting API",
    version="2.0.0",
    lifespan=lifespan
)

templates = Jinja2Templates(directory="templates")
report_service = ReportService()


@app.exception_handler(BaseAppException)
async def app_exception_handler(request: Request, exc: BaseAppException):
    """Handle application-specific exceptions."""
    logger.error(f"Application error: {exc}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error=str(exc),
            type=exc.__class__.__name__
        ).model_dump()
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions."""
    logger.error(f"Unexpected error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal server error",
            detail=str(exc)
        ).model_dump()
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


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    logger.info("Health check requested")
    return HealthResponse(status="healthy", version="2.0.0")


@app.post("/report", response_class=HTMLResponse)
@app.post("/consolidated-report", response_class=HTMLResponse)
async def generate_consolidated_report(request: Request, request_data: ConsolidatedReportRequest):
    """
    Generate consolidated report from provided case data and return HTML.
    
    This endpoint accepts case data in JSON format and generates all reports as HTML.
    """
    try:
        logger.info("Generating consolidated report")
        logger.info(f"Received {len(request_data.tbl_cases)} cases and {len(request_data.tbl_case_past)} past cases")
        
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

@app.post("/report/{report_type}", response_class=HTMLResponse)
async def generate_specific_report(request: Request, report_type: str, request_data: ConsolidatedReportRequest):
    """
    Generate a specific report from provided case data and return HTML.
    
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
        logger.info(f"Generating report: {report_type}")
        
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

# JSON API endpoints for programmatic access
@app.post("/api/report", response_model=ReportResponse)
@app.post("/api/consolidated-report", response_model=ReportResponse)
async def generate_consolidated_report_json(request_data: ConsolidatedReportRequest):
    """
    Generate consolidated report from provided case data (JSON response).
    
    This endpoint accepts case data in JSON format and returns JSON response.
    Use this for API integrations.
    """
    try:
        logger.info("Generating consolidated report (JSON)")
        logger.info(f"Received {len(request_data.tbl_cases)} cases and {len(request_data.tbl_case_past)} past cases")
        
        table_configs = get_table_configs("all")
        tables = report_service.build_report_tables(request_data, table_configs)
        
        return ReportResponse(
            success=True,
            message="Report generated successfully",
            tables=tables,
            total_tables=len(tables)
        )
    except Exception as e:
        logger.error(f"Error generating consolidated report: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/report/{report_type}", response_model=ReportResponse)
async def generate_specific_report_json(report_type: str, request_data: ConsolidatedReportRequest):
    """
    Generate a specific report from provided case data (JSON response).
    
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
    
    Use this for API integrations.
    """
    try:
        logger.info(f"Generating report (JSON): {report_type}")
        
        # Convert URL format to config key
        config_key = report_type.replace("-", "_")
        table_configs = get_table_configs(config_key)
        
        if not table_configs:
            logger.warning(f"Unknown report type: {report_type}")
            raise HTTPException(status_code=404, detail=f"Report type '{report_type}' not found")
        
        tables = report_service.build_report_tables(request_data, table_configs)
        
        return ReportResponse(
            success=True,
            message=f"Report '{report_type}' generated successfully",
            tables=tables,
            total_tables=len(tables)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating report '{report_type}': {e}")
        raise HTTPException(status_code=500, detail=str(e))