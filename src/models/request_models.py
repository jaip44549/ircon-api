"""Request models for API validation."""
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator
from datetime import datetime, date
from enum import Enum


class WorkStatus(str, Enum):
    """Work status enum."""
    COMPLETED = "Completed"
    TERMINATED = "Terminated"
    ABANDONED = "Abandoned"
    DISPUTED = "Disputed"
    OTHER = "Other"


class CaseType(str, Enum):
    """Case type enum."""
    ARBITRATION = "Arbitration"
    CONCILIATION = "Conciliation"
    MEDIATION = "Mediation"
    LITIGATION = "Litigation"


class UserType(str, Enum):
    """User type enum."""
    CLIENT = "Client"
    CONTRACTOR = "Contractor"


class BorneBy(str, Enum):
    """Borne by enum."""
    IRCON = "Ircon"
    CLIENT = "Client"


class CaseStatus(str, Enum):
    """Case status enum."""
    IN_PROGRESS = "In Progress"
    AWARDED = "Awarded"
    ACCRETED = "Accreted"
    CLOSED = "Closed"
    CHALLENGED = "Challenged"
    TRANSFERRED = "Transferred"
    SETTLED = "Settled"


# Custom validator to handle empty strings as None
def empty_str_to_none(v):
    """Convert empty strings to None."""
    if v == "" or v is None:
        return None
    return v


class CaseRecord(BaseModel):
    """Single case record from tbl_cases."""
    # Primary fields
    id: Optional[int] = None
    hash_id: Optional[str] = None
    uid: Optional[str] = None
    
    # Case details
    case_pertain: Optional[str] = None
    region: Optional[str] = None
    under_jurisdiction: Optional[str] = None
    loa: Optional[str] = None
    dr_no: Optional[str] = None
    work_name: Optional[str] = None
    work_status: Optional[WorkStatus] = None
    dispute_description: Optional[str] = None
    ocv: float = Field(default=0.0, description="Original Contract Value")
    
    # Case classification
    case_type: CaseType = Field(default=CaseType.ARBITRATION, description="Type of case")
    user_type: Optional[UserType] = Field(None, description="User type: Client or Contractor")
    borne_by: Optional[BorneBy] = Field(None, description="Who bears the case: Client or Ircon")
    
    # Parties involved
    claimant: Optional[str] = None
    respondent: Optional[str] = None
    authorized_representative: Optional[str] = None
    legal_counsel: Optional[str] = None
    
    # Court/Arbitration details
    court_case_no: Optional[str] = None
    neutral_type: Optional[str] = None
    date_appointment: Optional[date] = None
    neutral_name: Optional[str] = None
    client_arbitrator: Optional[str] = None
    ircon_arbitrator: Optional[str] = None
    appointed_by: Optional[date] = None
    
    # Financial claims
    client_claim: float = Field(default=0.0, description="Client's claim amount")
    ircon_claim: float = Field(default=0.0, description="IRCON's claim amount")
    contractor_claim: float = Field(default=0.0, description="Contractor's claim amount")
    
    # Award amounts
    award_amount_client: float = Field(default=0.0, description="Award amount for client")
    award_amount_ircon: float = Field(default=0.0, description="Award amount for IRCON")
    award_amount_contractor: float = Field(default=0.0, description="Award amount for contractor")
    
    # Status and positions
    case_status: Optional[CaseStatus] = Field(None, description="Current status of the case")
    position_end_last_quarter: Optional[str] = None
    past_date_hearing: Optional[date] = None
    position_end_this_quarter: Optional[str] = None
    upcoming_date_hearing: Optional[date] = None
    
    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Validators to handle empty strings
    @field_validator('work_status', 'user_type', 'borne_by', 'case_status', mode='before')
    @classmethod
    def empty_str_to_none_enum(cls, v):
        """Convert empty strings to None for enum fields."""
        if v == "" or v is None:
            return None
        return v
    
    @field_validator(
        'hash_id', 'uid', 'case_pertain', 'region', 'under_jurisdiction', 
        'loa', 'dr_no', 'work_name', 'dispute_description', 'claimant', 
        'respondent', 'authorized_representative', 'legal_counsel', 
        'court_case_no', 'neutral_type', 'neutral_name', 'client_arbitrator', 
        'ircon_arbitrator', 'position_end_last_quarter', 'position_end_this_quarter',
        mode='before'
    )
    @classmethod
    def empty_str_to_none_str(cls, v):
        """Convert empty strings to None for string fields."""
        if v == "":
            return None
        return v
    
    class Config:
        """Pydantic config."""
        use_enum_values = True
        str_strip_whitespace = True
        json_schema_extra = {
            "example": {
                "id": 1,
                "hash_id": "abc123",
                "uid": "CASE001",
                "case_pertain": "Project XYZ",
                "region": "North",
                "under_jurisdiction": "Delhi High Court",
                "loa": "LOA/2023/001",
                "dr_no": "DR/2023/001",
                "work_name": "Construction of Railway Bridge",
                "work_status": "Completed",
                "dispute_description": "Payment dispute regarding final bill",
                "ocv": 5000.50,
                "case_type": "Arbitration",
                "user_type": "Contractor",
                "borne_by": "Ircon",
                "claimant": "ABC Contractors Ltd",
                "respondent": "IRCON International Limited",
                "authorized_representative": "Mr. John Doe",
                "legal_counsel": "XYZ Law Firm",
                "court_case_no": "ARB/2023/001",
                "neutral_type": "Sole Arbitrator",
                "date_appointment": "2023-01-15",
                "neutral_name": "Justice Retired ABC",
                "client_arbitrator": "Mr. Client Rep",
                "ircon_arbitrator": "Mr. IRCON Rep",
                "appointed_by": "2023-01-20",
                "client_claim": 0.0,
                "ircon_claim": 150.5,
                "contractor_claim": 200.0,
                "award_amount_client": 0.0,
                "award_amount_ircon": 0.0,
                "award_amount_contractor": 0.0,
                "case_status": "In Progress",
                "position_end_last_quarter": "Hearing scheduled",
                "past_date_hearing": "2023-06-15",
                "position_end_this_quarter": "Arguments completed",
                "upcoming_date_hearing": "2023-09-20",
                "created_at": "2023-01-01T00:00:00",
                "updated_at": "2023-06-01T00:00:00"
            }
        }


class PastCaseRecord(BaseModel):
    """Single past case record from tbl_case_past."""
    # Primary fields
    past_id: Optional[int] = None
    case_id: int = Field(..., description="Reference to case in tbl_cases")
    
    # Parties and representatives
    authorized_representative: Optional[str] = None
    legal_counsel: Optional[str] = None
    
    # Arbitration details
    neutral_name: Optional[str] = None
    client_arbitrator: Optional[str] = None
    ircon_arbitrator: Optional[str] = None
    appointed_by: Optional[date] = None
    
    # Financial claims
    client_claim: float = Field(default=0.0, description="Client's claim amount")
    ircon_claim: float = Field(default=0.0, description="IRCON's claim amount")
    contractor_claim: float = Field(default=0.0, description="Contractor's claim amount")
    
    # Award amounts
    award_amount_client: float = Field(default=0.0, description="Award amount for client")
    award_amount_ircon: float = Field(default=0.0, description="Award amount for IRCON")
    award_amount_contractor: float = Field(default=0.0, description="Award amount for contractor")
    
    # Position tracking
    position_end_last_quarter: Optional[str] = None
    past_date_hearing: Optional[date] = None
    position_end_this_quarter: Optional[str] = None
    upcoming_date_hearing: Optional[date] = None
    
    # Status and timestamps
    created_at: Optional[datetime] = None
    case_status: Optional[CaseStatus] = Field(None, description="Status: In Progress, Awarded, Accreted, Closed, etc.")
    
    # Validators to handle empty strings
    @field_validator('case_status', mode='before')
    @classmethod
    def empty_str_to_none_enum(cls, v):
        """Convert empty strings to None for enum fields."""
        if v == "" or v is None:
            return None
        return v
    
    @field_validator(
        'authorized_representative', 'legal_counsel', 'neutral_name', 
        'client_arbitrator', 'ircon_arbitrator', 'position_end_last_quarter', 
        'position_end_this_quarter',
        mode='before'
    )
    @classmethod
    def empty_str_to_none_str(cls, v):
        """Convert empty strings to None for string fields."""
        if v == "":
            return None
        return v
    
    class Config:
        """Pydantic config."""
        use_enum_values = True
        str_strip_whitespace = True
        json_schema_extra = {
            "example": {
                "past_id": 1,
                "case_id": 1,
                "authorized_representative": "Mr. John Doe",
                "legal_counsel": "XYZ Law Firm",
                "neutral_name": "Justice Retired ABC",
                "client_arbitrator": "Mr. Client Rep",
                "ircon_arbitrator": "Mr. IRCON Rep",
                "appointed_by": "2023-01-20",
                "client_claim": 0.0,
                "ircon_claim": 150.5,
                "contractor_claim": 200.0,
                "award_amount_client": 0.0,
                "award_amount_ircon": 0.0,
                "award_amount_contractor": 0.0,
                "position_end_last_quarter": "Hearing scheduled",
                "past_date_hearing": "2023-06-15",
                "position_end_this_quarter": "Arguments completed",
                "upcoming_date_hearing": "2023-09-20",
                "created_at": "2023-01-01T00:00:00",
                "case_status": "In Progress"
            }
        }


class ConsolidatedReportRequest(BaseModel):
    """Request model for consolidated report generation."""
    tbl_cases: List[CaseRecord] = Field(..., description="List of current case records (required)")
    tbl_case_past: List[PastCaseRecord] = Field(default=[], description="List of past case records (optional)")
    
    @field_validator('tbl_cases')
    @classmethod
    def validate_tbl_cases(cls, v: List[CaseRecord]) -> List[CaseRecord]:
        """Validate tbl_cases is not empty."""
        if not v or len(v) == 0:
            raise ValueError("tbl_cases cannot be empty - at least one case record is required")
        return v
    
    # tbl_case_past is optional - no validation needed
    
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
