"""Request models for API validation."""
from typing import List, Optional, Any
from pydantic import BaseModel, Field, field_validator
from datetime import datetime


class CaseRecord(BaseModel):
    """Single case record from tbl_cases."""
    id: Optional[int] = None
    case_type: str = Field(..., description="Type of case: Arbitration or Litigation")
    user_type: str = Field(..., description="User type: Contractor or Client")
    borne_by: str = Field(..., description="Who bears the case: Client or Ircon")
    ircon_claim: float = Field(default=0.0, description="IRCON's claim amount")
    contractor_claim: float = Field(default=0.0, description="Contractor's claim amount")
    client_claim: float = Field(default=0.0, description="Client's claim amount")
    case_status: Optional[str] = Field(default="In Progress", description="Status of the case")
    claimant: Optional[str] = Field(default="", description="Name of claimant")
    case_pertain: Optional[str] = Field(default="", description="What the case pertains to")
    award_amount_contractor: float = Field(default=0.0, description="Award amount for contractor")
    award_amount_ircon: float = Field(default=0.0, description="Award amount for IRCON")
    
    @field_validator('case_type')
    @classmethod
    def validate_case_type(cls, v: str) -> str:
        """Validate case type."""
        allowed = ['arbitration', 'litigation']
        if v.lower() not in allowed:
            raise ValueError(f"case_type must be one of {allowed}")
        return v.capitalize()
    
    @field_validator('user_type')
    @classmethod
    def validate_user_type(cls, v: str) -> str:
        """Validate user type."""
        allowed = ['contractor', 'client']
        if v.lower() not in allowed:
            raise ValueError(f"user_type must be one of {allowed}")
        return v.capitalize()
    
    @field_validator('borne_by')
    @classmethod
    def validate_borne_by(cls, v: str) -> str:
        """Validate borne_by."""
        allowed = ['client', 'ircon']
        if v.lower() not in allowed:
            raise ValueError(f"borne_by must be one of {allowed}")
        return v.capitalize()
    
    class Config:
        """Pydantic config."""
        str_strip_whitespace = True
        json_schema_extra = {
            "example": {
                "id": 1,
                "case_type": "Arbitration",
                "user_type": "Contractor",
                "borne_by": "Ircon",
                "ircon_claim": 150.5,
                "contractor_claim": 200.0,
                "client_claim": 0.0,
                "case_status": "In Progress",
                "claimant": "ABC Contractors Ltd",
                "case_pertain": "Project XYZ",
                "award_amount_contractor": 0.0,
                "award_amount_ircon": 0.0
            }
        }


class PastCaseRecord(BaseModel):
    """Single past case record from tbl_case_past."""
    id: Optional[int] = None
    case_id: int = Field(..., description="Reference to case in tbl_cases")
    case_type: str = Field(..., description="Type of case: Arbitration or Litigation")
    user_type: str = Field(..., description="User type: Contractor or Client")
    borne_by: str = Field(..., description="Who bears the case: Client or Ircon")
    ircon_claim: float = Field(default=0.0, description="IRCON's claim amount")
    contractor_claim: float = Field(default=0.0, description="Contractor's claim amount")
    client_claim: float = Field(default=0.0, description="Client's claim amount")
    case_status: str = Field(..., description="Status: In Progress, Accreted, Closed, Settled, Awarded")
    
    @field_validator('case_type')
    @classmethod
    def validate_case_type(cls, v: str) -> str:
        """Validate case type."""
        allowed = ['arbitration', 'litigation']
        if v.lower() not in allowed:
            raise ValueError(f"case_type must be one of {allowed}")
        return v.capitalize()
    
    @field_validator('user_type')
    @classmethod
    def validate_user_type(cls, v: str) -> str:
        """Validate user type."""
        allowed = ['contractor', 'client']
        if v.lower() not in allowed:
            raise ValueError(f"user_type must be one of {allowed}")
        return v.capitalize()
    
    @field_validator('borne_by')
    @classmethod
    def validate_borne_by(cls, v: str) -> str:
        """Validate borne_by."""
        allowed = ['client', 'ircon']
        if v.lower() not in allowed:
            raise ValueError(f"borne_by must be one of {allowed}")
        return v.capitalize()
    
    @field_validator('case_status')
    @classmethod
    def validate_case_status(cls, v: str) -> str:
        """Validate case status."""
        allowed = ['in progress', 'accreted', 'closed', 'settled', 'awarded']
        if v.lower() not in allowed:
            raise ValueError(f"case_status must be one of {allowed}")
        return v.title()
    
    class Config:
        """Pydantic config."""
        str_strip_whitespace = True
        json_schema_extra = {
            "example": {
                "id": 1,
                "case_id": 1,
                "case_type": "Arbitration",
                "user_type": "Contractor",
                "borne_by": "Ircon",
                "ircon_claim": 150.5,
                "contractor_claim": 200.0,
                "client_claim": 0.0,
                "case_status": "In Progress"
            }
        }


class ConsolidatedReportRequest(BaseModel):
    """Request model for consolidated report generation."""
    tbl_cases: List[CaseRecord] = Field(..., description="List of current case records")
    tbl_case_past: List[PastCaseRecord] = Field(..., description="List of past case records")
    
    @field_validator('tbl_cases')
    @classmethod
    def validate_tbl_cases(cls, v: List[CaseRecord]) -> List[CaseRecord]:
        """Validate tbl_cases is not empty."""
        if not v:
            raise ValueError("tbl_cases cannot be empty")
        return v
    
    @field_validator('tbl_case_past')
    @classmethod
    def validate_tbl_case_past(cls, v: List[PastCaseRecord]) -> List[PastCaseRecord]:
        """Validate tbl_case_past is not empty."""
        if not v:
            raise ValueError("tbl_case_past cannot be empty")
        return v
    
    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "example": {
                "tbl_cases": [
                    {
                        "id": 1,
                        "case_type": "Arbitration",
                        "user_type": "Contractor",
                        "borne_by": "Ircon",
                        "ircon_claim": 150.5,
                        "contractor_claim": 200.0,
                        "client_claim": 0.0,
                        "case_status": "In Progress",
                        "claimant": "ABC Contractors Ltd",
                        "case_pertain": "Project XYZ",
                        "award_amount_contractor": 0.0,
                        "award_amount_ircon": 0.0
                    }
                ],
                "tbl_case_past": [
                    {
                        "id": 1,
                        "case_id": 1,
                        "case_type": "Arbitration",
                        "user_type": "Contractor",
                        "borne_by": "Ircon",
                        "ircon_claim": 150.5,
                        "contractor_claim": 200.0,
                        "client_claim": 0.0,
                        "case_status": "In Progress"
                    }
                ]
            }
        }
