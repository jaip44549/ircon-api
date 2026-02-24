"""FastAPI application for case management reporting system."""
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from typing import List, Dict, Any

from dotenv import load_dotenv
load_dotenv()

from src.database import close_db, get_db
from src.services.report_service import ReportService
from src.logger import get_logger
from src.exceptions import BaseAppException

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown."""
    logger.info("Application starting up...")
    
    # Test database connection
    try:
        db = get_db()
        if db.test_connection():
            logger.info("Database connection successful")
        else:
            logger.error("Database connection test failed")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
    
    yield
    
    # Shutdown
    logger.info("Application shutting down...")
    close_db()
    logger.info("Application shutdown complete")


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
        content={"error": str(exc), "type": exc.__class__.__name__}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions."""
    logger.error(f"Unexpected error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
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
    try:
        db = get_db()
        db_healthy = db.test_connection()
        
        return {
            "status": "healthy" if db_healthy else "degraded",
            "database": "connected" if db_healthy else "disconnected"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Root endpoint showing all reports."""
    try:
        logger.info("Generating all reports")
        table_configs = get_table_configs("all")
        tables = report_service.build_report_tables(table_configs)
        
        return templates.TemplateResponse(
            request=request,
            name="report.html",
            context={"tables": tables}
        )
    except Exception as e:
        logger.error(f"Error generating root report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/report/{report_type}", response_class=HTMLResponse)
async def get_report(request: Request, report_type: str):
    """Generic endpoint for individual reports."""
    try:
        logger.info(f"Generating report: {report_type}")
        
        # Convert URL format to config key
        config_key = report_type.replace("-", "_")
        table_configs = get_table_configs(config_key)
        
        if not table_configs:
            logger.warning(f"Unknown report type: {report_type}")
            raise HTTPException(status_code=404, detail=f"Report type '{report_type}' not found")
        
        tables = report_service.build_report_tables(table_configs)
        
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