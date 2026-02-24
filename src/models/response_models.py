"""Response models for API."""
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class TableData(BaseModel):
    """Table data structure for reports."""
    type: str = Field(..., description="Type of table/report")
    title: str = Field(..., description="Title of the report")
    data: Dict[str, Any] = Field(..., description="Processed report data")
    
    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "example": {
                "type": "arb_contractor",
                "title": "Over all position of Arbitration cases with Contractors",
                "data": {
                    "rows": []
                }
            }
        }


class ReportResponse(BaseModel):
    """Response model for report generation."""
    success: bool = Field(default=True, description="Whether report generation was successful")
    message: str = Field(default="Report generated successfully", description="Status message")
    tables: List[TableData] = Field(..., description="List of generated report tables")
    total_tables: int = Field(..., description="Total number of tables generated")
    
    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Report generated successfully",
                "tables": [
                    {
                        "type": "arb_contractor",
                        "title": "Over all position of Arbitration cases with Contractors",
                        "data": {"rows": []}
                    }
                ],
                "total_tables": 1
            }
        }


class ErrorResponse(BaseModel):
    """Error response model."""
    success: bool = Field(default=False, description="Always false for errors")
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    type: Optional[str] = Field(None, description="Error type/class")
    
    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "example": {
                "success": False,
                "error": "Validation error",
                "detail": "tbl_cases cannot be empty",
                "type": "ValidationError"
            }
        }


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Health status: healthy, degraded, unhealthy")
    version: str = Field(default="2.0.0", description="Application version")
    
    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "version": "2.0.0"
            }
        }
