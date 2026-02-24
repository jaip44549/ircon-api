from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager

from dotenv import load_dotenv
load_dotenv()

from src.utils import build_report_tables
from src.database import close_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown."""
    # Startup
    yield
    # Shutdown
    close_db()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def read_root(request: Request):
    """Root endpoint showing all reports."""
    table_configs = [
        {
            "type": "arb_lit_contractor",
            "title": "Over all position of Arbitration and Court cases with Contractors"
        },
        {
            "type": "arb_contractor",
            "title": "Over all position of Arbitration cases with Contractors"
        },
        {
            "type": "rev_arb_contractor",
            "title": "Details of Arbitration cases with Contractors Accreted/Revised"
        },
        {
            "type": "close_arb_contractor",
            "title": "Details of Arbitration cases with Contractors Closed"
        },
        {
            "type": "court_contractor",
            "title": "Over all position of Court cases with Contractors"
        },
        {
            "type": "rev_court_contractor",
            "title": "Details of Court cases with Contractors Accretion"
        },
        {
            "type": "close_court_contractor",
            "title": "Details of Court cases with Contractors Closed"
        },
        {
            "type": "arb_lit_client",
            "title": "Over all position of Arbitration and Court cases with Clients"
        },
        {
            "type": "close_court_client",
            "title": "Details of Court cases with Client Closed"
        },
    ]
    
    tables = build_report_tables(table_configs)
    
    return templates.TemplateResponse(
        request=request,
        name="report.html",
        context={"tables": tables}
    )

@app.get("/report/arb-lit-contractor", response_class=HTMLResponse)
def arb_lit_contractor_report(request: Request):
    """Endpoint for arbitration and litigation with contractor report only."""
    table_configs = [
        {
            "type": "arb_lit_contractor",
            "title": "Over all position of Arbitration and Court cases with Contractors"
        }
    ]
    
    tables = build_report_tables(table_configs)
    
    return templates.TemplateResponse(
        request=request,
        name="report.html",
        context={"tables": tables}
    )

@app.get("/report/arb-contractor", response_class=HTMLResponse)
def arb_contractor_report(request: Request):
    """Endpoint for position summary report only."""
    table_configs = [
        {
            "type": "arb_contractor",
            "title": "Over all position of Arbitration cases with Contractors"
        }
    ]
    
    tables = build_report_tables(table_configs)
    
    return templates.TemplateResponse(
        request=request,
        name="report.html",
        context={"tables": tables}
    )

@app.get("/report/rev-arb-contractor", response_class=HTMLResponse)
def rev_arb_contractor(request: Request):
    table_configs = [
        {
            "type": "rev_arb_contractor",
            "title": "Details of Arbitration cases with Contractors Accreted/Revised"
        }
    ]
    
    tables = build_report_tables(table_configs)
    
    return templates.TemplateResponse(
        request=request,
        name="report.html",
        context={"tables": tables}
    )


@app.get("/report/close-arb-contractor", response_class=HTMLResponse)
def close_arb_contractor(request: Request):
    table_configs = [
        {
            "type": "close_arb_contractor",
            "title": "Details of Arbitration cases with Contractors Closed"
        }
    ]
    
    tables = build_report_tables(table_configs)
    
    return templates.TemplateResponse(
        request=request,
        name="report.html",
        context={"tables": tables}
    )


@app.get("/report/court-contractor", response_class=HTMLResponse)
def arb_contractor_report(request: Request):
    """Endpoint for position summary report only."""
    table_configs = [
        {
            "type": "court_contractor",
            "title": "Over all position of Court cases with Contractors"
        }
    ]
    
    tables = build_report_tables(table_configs)
    
    return templates.TemplateResponse(
        request=request,
        name="report.html",
        context={"tables": tables}
    )


@app.get("/report/rev-court-contractor", response_class=HTMLResponse)
def rev_court_contractor(request: Request):
    """Endpoint for position summary report only."""
    table_configs = [
        {
            "type": "rev_court_contractor",
            "title": "Details of Court cases with Contractors Accretion"
        }
    ]
    
    tables = build_report_tables(table_configs)
    
    return templates.TemplateResponse(
        request=request,
        name="report.html",
        context={"tables": tables}
    )

@app.get("/report/close-court-contractor", response_class=HTMLResponse)
def close_court_contractor(request: Request):
    table_configs = [
        {
            "type": "close_court_contractor",
            "title": "Details of Court cases with Contractors Closed"
        }
    ]
    
    tables = build_report_tables(table_configs)
    
    return templates.TemplateResponse(
        request=request,
        name="report.html",
        context={"tables": tables}
    )

@app.get("/report/close-court-client", response_class=HTMLResponse)
def close_court_client(request: Request):
    table_configs = [
        {
            "type": "close_court_client",
            "title": "Details of Court cases with Client Closed"
        }
    ]
    
    tables = build_report_tables(table_configs)
    
    return templates.TemplateResponse(
        request=request,
        name="report.html",
        context={"tables": tables}
    )


@app.get("/report/arb-lit-client", response_class=HTMLResponse)
def arb_lit_client_report(request: Request):
    """Endpoint for arbitration and litigation with client report only."""
    table_configs = [
        {
            "type": "arb_lit_client",
            "title": "Over all position of Arbitration and Court cases with Clients"
        },
    ]
    
    tables = build_report_tables(table_configs)
    
    return templates.TemplateResponse(
        request=request,
        name="report.html",
        context={"tables": tables}
    )