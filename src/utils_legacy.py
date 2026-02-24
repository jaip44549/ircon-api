import numpy as np
import pandas as pd
import numpy as np
from typing import Dict, List, Any

from src.config import config
from src.database import get_db

# Data loading
def load_cases_data(name: str = None) -> pd.DataFrame:
    """
    Load and return the cases dataframe from database.
    """
    db = get_db()
    if name:
        return db.read_table(name)
    return db.read_table(config.CASES_TABLE)

def load_joined_cases_data() -> pd.DataFrame:
    """
    Load cases data joined with past data.
    Returns DataFrame with case_type, user_type, borne_by from current,
    and ircon_claim, contractor_claim from past.
    """
    db = get_db()
    query = """
        SELECT 
            c.case_type, 
            c.user_type, 
            c.borne_by, 
            p.client_claim,
            p.ircon_claim, 
            p.contractor_claim,
            p.case_status
        FROM tbl_case_past p
        JOIN tbl_cases c ON p.case_id = c.id
    """
    return db.read_query(query)

# Helper function for safe aggregation
def safe_aggregate(df: pd.DataFrame, columns: List[str]) -> Dict[str, Any]:
    """
    Safely aggregate data with groupby, handling cases where groups don't exist.
    Returns a dict with 'Client', 'Ircon', and 'total' keys.
    """
    if df.empty:
        return {}
    
    # Replace 0 with NaN for proper counting
    df_processed = df.copy()
    df_processed[columns] = df_processed[columns].replace(0, np.nan)
    
    # Aggregate and transpose
    result = df_processed.groupby("borne_by").agg(["count", "sum"]).T
    
    # Ensure 'Client' and 'Ircon' columns exist
    if 'Client' not in result.columns:
        result['Client'] = 0
    if 'Ircon' not in result.columns:
        result['Ircon'] = 0
    
    # Add total column
    result['total'] = result['Client'] + result['Ircon']
    
    return result.to_dict()

# Data processors
def get_arb_lit_contractor_data(df: pd.DataFrame, past_df: pd.DataFrame) -> Dict[str, Any]:
    """Process data for arbitration and litigation with contractor report."""
    # Current quarter data
    tbl1 = df[["case_type", "user_type", "borne_by", "ircon_claim", "contractor_claim"]]
    tbl1 = tbl1[(tbl1["case_type"].str.lower() == "arbitration") & 
                (tbl1["user_type"].str.lower() == "contractor")]
    
    client_cases = tbl1[tbl1["borne_by"].str.lower() == "client"]
    ircon_cases = tbl1[tbl1["borne_by"].str.lower() == "ircon"]

    contractor_claim_count = tbl1.shape[0]
    total_contractor_claim = client_cases["contractor_claim"].sum()
    total_ircon_claim = ircon_cases["ircon_claim"].sum()
    ircon_share = ircon_cases.shape[0]

    # Past quarter data - using joined data (case_type, user_type, borne_by from current, claims from past)
    p_tbl1 = past_df[["case_type", "user_type", "borne_by", "ircon_claim", "contractor_claim"]]
    p_tbl1 = p_tbl1[(p_tbl1["case_type"].str.lower() == "arbitration") & 
                (p_tbl1["user_type"].str.lower() == "contractor")]
    
    p_client_cases = p_tbl1[p_tbl1["borne_by"].str.lower() == "client"]
    p_ircon_cases = p_tbl1[p_tbl1["borne_by"].str.lower() == "ircon"]

    p_contractor_claim_count = p_tbl1.shape[0]
    p_total_contractor_claim = p_client_cases["contractor_claim"].sum()
    p_total_ircon_claim = p_ircon_cases["ircon_claim"].sum()
    p_ircon_share = p_ircon_cases.shape[0]

    data =  {
        "old_q": {
            "arb_val": [
                (p_contractor_claim_count, p_ircon_share),
                (f"₹{round(p_total_contractor_claim, 2)}", f"₹{round(p_total_ircon_claim, 2)}"),
            ],
            "lit_val": [
                (0, 0),
                (f"₹{round(0, 2)}", f"₹{round(0, 2)}"),
            ]
        },
        "q": {
            "arb_val": [
                (contractor_claim_count, ircon_share),
                (f"₹{round(total_contractor_claim, 2)}", f"₹{round(total_ircon_claim, 2)}"),
            ],
            "lit_val": [
                (0, 0),
                (f"₹{round(0, 2)}", f"₹{round(0, 2)}"),
            ]
        }
    }

    tbl1 = df[["case_type", "user_type", "borne_by", "ircon_claim", "contractor_claim"]]
    tbl1 = tbl1[(tbl1["case_type"].str.lower() == "litigation") & 
                (tbl1["user_type"].str.lower() == "contractor")]
    
    client_cases = tbl1[tbl1["borne_by"].str.lower() == "client"]
    ircon_cases = tbl1[tbl1["borne_by"].str.lower() == "ircon"]

    contractor_claim_count = tbl1.shape[0]
    total_contractor_claim = client_cases["contractor_claim"].sum()
    total_ircon_claim = ircon_cases["ircon_claim"].sum()
    ircon_share = ircon_cases.shape[0]

    # Past quarter data - using joined data (case_type, user_type, borne_by from current, claims from past)
    p_tbl1 = past_df[["case_type", "user_type", "borne_by", "ircon_claim", "contractor_claim"]]
    p_tbl1 = p_tbl1[(p_tbl1["case_type"].str.lower() == "litigation") & 
                (p_tbl1["user_type"].str.lower() == "contractor")]
    
    p_client_cases = p_tbl1[p_tbl1["borne_by"].str.lower() == "client"]
    p_ircon_cases = p_tbl1[p_tbl1["borne_by"].str.lower() == "ircon"]

    p_contractor_claim_count = p_tbl1.shape[0]
    p_total_contractor_claim = p_client_cases["contractor_claim"].sum()
    p_total_ircon_claim = p_ircon_cases["ircon_claim"].sum()
    p_ircon_share = p_ircon_cases.shape[0]

    data["old_q"]["lit_val"] = [
        (p_contractor_claim_count, p_ircon_share),
        (f"₹{round(p_total_contractor_claim, 2)}", f"₹{round(p_total_ircon_claim, 2)}"),
    ]

    data["q"]["lit_val"] = [
        (contractor_claim_count, ircon_share),
        (f"₹{round(total_contractor_claim, 2)}", f"₹{round(total_ircon_claim, 2)}"),
    ]

    return data

def get_arb_contractor_data(df: pd.DataFrame) -> Dict[str, Any]:
    """Process data for position summary report."""
    # Placeholder implementation - adjust based on your actual data structure
    data = {}

    tbl_cases = load_cases_data("tbl_cases")
    tbl_case_past = load_joined_cases_data()

    # 1. Filter the data as before
    tbl1 = tbl_cases[(tbl_cases["case_type"] == "Arbitration") & (tbl_cases["user_type"] == "Contractor")]
    tbl1 = tbl1[["borne_by", "ircon_claim", "contractor_claim"]]

    # 2. Replace 0 with NaN so they are excluded from .count() 
    # but still work correctly with .sum()
    tbl1_processed = tbl1.copy()
    tbl1_processed[["ircon_claim", "contractor_claim"]] = tbl1_processed[["ircon_claim", "contractor_claim"]].replace(0, np.nan)

    # 3. Aggregate and Transpose
    n = tbl1_processed.groupby("borne_by").agg(["count", "sum"]).T

    # 4. Add the total column
    n["total"] = n["Client"] + n["Ircon"]

    data["pendency"] = n.to_dict()

    # 1. Filter the data as before
    tbl1 = tbl_case_past[(tbl_case_past["case_type"] == "Arbitration") & (tbl_case_past["user_type"] == "Contractor") & (tbl_case_past["case_status"] == "In Progress")]
    tbl1 = tbl1[["borne_by", "ircon_claim", "contractor_claim"]]

    # 2. Replace 0 with NaN so they are excluded from .count() 
    # but still work correctly with .sum()
    tbl1_processed = tbl1.copy()
    tbl1_processed[["ircon_claim", "contractor_claim"]] = tbl1_processed[["ircon_claim", "contractor_claim"]].replace(0, np.nan)

    # 3. Aggregate and Transpose
    n = tbl1_processed.groupby("borne_by").agg(["count", "sum"]).T

    # 4. Add the total column
    n["total"] = n["Client"] + n["Ircon"]

    data["opening"] = n.to_dict()

    # 1. Filter the data as before
    tbl1 = tbl_case_past[(tbl_case_past["case_type"] == "Arbitration") & (tbl_case_past["user_type"] == "Contractor") & (tbl_case_past["case_status"] == "Accreted")]
    tbl1 = tbl1[["borne_by", "ircon_claim", "contractor_claim"]]

    # 2. Replace 0 with NaN so they are excluded from .count() 
    # but still work correctly with .sum()
    tbl1_processed = tbl1.copy()
    tbl1_processed[["ircon_claim", "contractor_claim"]] = tbl1_processed[["ircon_claim", "contractor_claim"]].replace(0, np.nan)

    # 3. Aggregate and Transpose
    n = tbl1_processed.groupby("borne_by").agg(["count", "sum"]).T

    data["accretion"] = n.to_dict()

    # 1. Filter the data as before
    # Use .isin() instead of 'in'
    tbl1 = tbl_case_past[
        (tbl_case_past["case_type"] == "Arbitration") & 
        (tbl_case_past["user_type"] == "Contractor") & 
        (tbl_case_past["case_status"].isin(["Closed", "Settled", "Awarded"]))
    ]
    tbl1 = tbl1[["borne_by", "ircon_claim", "contractor_claim"]]

    # 2. Replace 0 with NaN so they are excluded from .count() 
    # but still work correctly with .sum()
    tbl1_processed = tbl1.copy()
    tbl1_processed[["ircon_claim", "contractor_claim"]] = tbl1_processed[["ircon_claim", "contractor_claim"]].replace(0, np.nan)

    # 3. Aggregate and Transpose
    n = tbl1_processed.groupby("borne_by").agg(["count", "sum"]).T

    data["closed"] = n.to_dict()

    return {
        "rows": [
            {
                "category": "Contractor's Claims on IRCON",
                "metric": "Nos.",
                "opening_client": int(data.get("opening")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "opening_ircon": int(data.get("opening")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "opening_total": int(data.get("opening")\
                    .get("total", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "accretion_ircon": int(data.get("accretion")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "accretion_total": int(data.get("accretion")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "closed_ircon": int(data.get("closed")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "closed_total": int(data.get("closed")\
                    .get("total", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "pendency_client": int(data.get("pendency")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "pendency_ircon": int(data.get("pendency")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "pendency_total": int(data.get("pendency")\
                    .get("total", {})\
                        .get(('contractor_claim', 'count'), 0))
            },
            {
                "category": "Contractor's Claims on IRCON",
                "metric": "Amt in Crs.",
                "opening_client": round(data.get("opening")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "opening_ircon": round(data.get("opening")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "opening_total": round(data.get("opening")\
                    .get("total", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "accretion_client": round(data.get("accretion")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "accretion_ircon": round(data.get("accretion")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "closed_client": round(data.get("closed")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "closed_ircon": round(data.get("closed")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "pendency_client": round(data.get("pendency")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "pendency_ircon": round(data.get("pendency")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "pendency_total": round(data.get("pendency")\
                    .get("total", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
            },
            {
                "category": "IRCON's Claims on Contractor",
                "metric": "Nos.",
                "opening_client": int(data.get("opening")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "opening_ircon": int(data.get("opening")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "opening_total": int(data.get("opening")\
                    .get("total", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "accretion_ircon": int(data.get("accretion")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "accretion_total": int(data.get("accretion")\
                    .get("total", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "closed_ircon": int(data.get("closed")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "closed_total": int(data.get("closed")\
                    .get("total", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "pendency_client": int(data.get("pendency")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "pendency_ircon": int(data.get("pendency")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "pendency_total": int(data.get("pendency")\
                    .get("total", {})\
                        .get(('ircon_claim', 'count'), 0))
            },
            {
                "category": "IRCON's Claims on Contractor",
                "metric": "Amt in Crs.",
                "opening_client": round(data.get("opening")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "opening_ircon": round(data.get("opening")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "opening_total": round(data.get("opening")\
                    .get("total", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "accretion_client": round(data.get("accretion")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "accretion_ircon": round(data.get("accretion")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "closed_client": round(data.get("closed")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "closed_ircon": round(data.get("closed")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "pendency_client": round(data.get("pendency")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "pendency_ircon": round(data.get("pendency")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "pendency_total": round(data.get("pendency")\
                    .get("total", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
            }
        ]
    }

def get_court_contractor_data(df: pd.DataFrame) -> Dict[str, Any]:
    """Process data for position summary report."""
    data = {}

    tbl_cases = load_cases_data("tbl_cases")
    tbl_case_past = load_joined_cases_data()

    # Pendency (current cases)
    tbl1 = tbl_cases[(tbl_cases["case_type"] == "Litigation") & (tbl_cases["user_type"] == "Contractor")]
    tbl1 = tbl1[["borne_by", "ircon_claim", "contractor_claim"]]
    data["pendency"] = safe_aggregate(tbl1, ["ircon_claim", "contractor_claim"])

    # Opening (past cases in progress)
    tbl1 = tbl_case_past[(tbl_case_past["case_type"] == "Litigation") & 
                         (tbl_case_past["user_type"] == "Contractor") & 
                         (tbl_case_past["case_status"] == "In Progress")]
    tbl1 = tbl1[["borne_by", "ircon_claim", "contractor_claim"]]
    data["opening"] = safe_aggregate(tbl1, ["ircon_claim", "contractor_claim"])

    # Accretion (past cases accreted)
    tbl1 = tbl_case_past[(tbl_case_past["case_type"] == "Litigation") & 
                         (tbl_case_past["user_type"] == "Contractor") & 
                         (tbl_case_past["case_status"] == "Accreted")]
    tbl1 = tbl1[["borne_by", "ircon_claim", "contractor_claim"]]
    data["accretion"] = safe_aggregate(tbl1, ["ircon_claim", "contractor_claim"])

    # Closed (past cases closed/settled/awarded)
    tbl1 = tbl_case_past[(tbl_case_past["case_type"] == "Litigation") & 
                         (tbl_case_past["user_type"] == "Contractor") & 
                         (tbl_case_past["case_status"].isin(["Closed", "Settled", "Awarded"]))]
    tbl1 = tbl1[["borne_by", "ircon_claim", "contractor_claim"]]
    data["closed"] = safe_aggregate(tbl1, ["ircon_claim", "contractor_claim"])

    return {
        "rows": [
            {
                "category": "Contractor's Claims on IRCON",
                "metric": "Nos.",
                "opening_client": int(data.get("opening")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "opening_ircon": int(data.get("opening")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "opening_total": int(data.get("opening")\
                    .get("total", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "accretion_ircon": int(data.get("accretion")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "accretion_total": int(data.get("accretion")\
                    .get("total", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "closed_ircon": int(data.get("closed")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "closed_total": int(data.get("closed")\
                    .get("total", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "pendency_client": int(data.get("pendency")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "pendency_ircon": int(data.get("pendency")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'count'), 0)),
                "pendency_total": int(data.get("pendency")\
                    .get("total", {})\
                        .get(('contractor_claim', 'count'), 0))
            },
            {
                "category": "Contractor's Claims on IRCON",
                "metric": "Amt in Crs.",
                "opening_client": round(data.get("opening")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "opening_ircon": round(data.get("opening")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "opening_total": round(data.get("opening")\
                    .get("total", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "accretion_client": round(data.get("accretion")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "accretion_ircon": round(data.get("accretion")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "closed_client": round(data.get("closed")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "closed_ircon": round(data.get("closed")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "pendency_client": round(data.get("pendency")\
                    .get("Client", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "pendency_ircon": round(data.get("pendency")\
                    .get("Ircon", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
                "pendency_total": round(data.get("pendency")\
                    .get("total", {})\
                        .get(('contractor_claim', 'sum'), 0), 2),
            },
            {
                "category": "IRCON's Claims on Contractor",
                "metric": "Nos.",
                "opening_client": int(data.get("opening")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "opening_ircon": int(data.get("opening")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "opening_total": int(data.get("opening")\
                    .get("total", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "accretion_ircon": int(data.get("accretion")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "accretion_total": int(data.get("accretion")\
                    .get("total", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "closed_ircon": int(data.get("closed")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "closed_total": int(data.get("closed")\
                    .get("total", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "pendency_client": int(data.get("pendency")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "pendency_ircon": int(data.get("pendency")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'count'), 0)),
                "pendency_total": int(data.get("pendency")\
                    .get("total", {})\
                        .get(('ircon_claim', 'count'), 0))
            },
            {
                "category": "IRCON's Claims on Contractor",
                "metric": "Amt in Crs.",
                "opening_client": round(data.get("opening")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "opening_ircon": round(data.get("opening")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "opening_total": round(data.get("opening")\
                    .get("total", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "accretion_client": round(data.get("accretion")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "accretion_ircon": round(data.get("accretion")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "closed_client": round(data.get("closed")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "closed_ircon": round(data.get("closed")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "pendency_client": round(data.get("pendency")\
                    .get("Client", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "pendency_ircon": round(data.get("pendency")\
                    .get("Ircon", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
                "pendency_total": round(data.get("pendency")\
                    .get("total", {})\
                        .get(('ircon_claim', 'sum'), 0), 2),
            }
        ]
    }

def get_close_arb_contractor_data(df: pd.DataFrame) -> Dict[str, Any]:
    tbl_cases = load_cases_data()

    tbl1 = tbl_cases[(tbl_cases["case_type"] == "Arbitration") & (tbl_cases["user_type"] == "Contractor") & (tbl_cases["case_status"] == "Closed")]
    tbl1 = tbl1[["claimant", "case_pertain", "borne_by", "contractor_claim", "award_amount_contractor", "ircon_claim", "award_amount_ircon"]]
    tbl1["remarks"] = ""

    return {
        "rows": tbl1.values
    }

def get_close_court_contractor_data(df: pd.DataFrame) -> Dict[str, Any]:

    tbl_cases = load_cases_data()

    tbl1 = tbl_cases[(tbl_cases["case_type"] == "Litigation") & (tbl_cases["user_type"] == "Contractor") & (tbl_cases["case_status"] == "Closed")]
    tbl1["contractor_claim_or_award"] = tbl1["contractor_claim"].astype(str) + " " + tbl1["award_amount_contractor"].astype(str)
    tbl1["ircon_claim_or_award"] = tbl1["ircon_claim"].astype(str) + " " + tbl1["award_amount_ircon"].astype(str)
    tbl1 = tbl1[["claimant", "case_pertain", "borne_by", "contractor_claim_or_award", "ircon_claim_or_award"]]
    tbl1["remarks"] = ""


    return {
        "rows": tbl1.values
    }

def get_rev_arb_contractor_data(df: pd.DataFrame) -> Dict[str, Any]:
    tbl_cases = load_cases_data()

    tbl1 = tbl_cases[(tbl_cases["case_type"] == "Arbitration") & (tbl_cases["user_type"] == "Contractor") & (tbl_cases["case_status"] == "Accreted")]
    tbl1 = tbl1[["claimant", "case_pertain", "borne_by", "contractor_claim", "ircon_claim"]]
    tbl1["remarks"] = ""
    return {
        "rows": tbl1.values
    }

def get_rev_court_contractor_data(df: pd.DataFrame) -> Dict[str, Any]:
    tbl_cases = load_cases_data()

    tbl1 = tbl_cases[(tbl_cases["case_type"] == "Litigation") & (tbl_cases["user_type"] == "Contractor") & (tbl_cases["case_status"] == "Accreted")]
    tbl1["contractor_claim_or_award"] = tbl1["contractor_claim"].astype(str) + " " + tbl1["award_amount_contractor"].astype(str)
    tbl1["ircon_claim_or_award"] = tbl1["ircon_claim"].astype(str) + " " + tbl1["award_amount_ircon"].astype(str)
    tbl1 = tbl1[["claimant", "case_pertain", "borne_by", "contractor_claim_or_award", "ircon_claim_or_award"]]
    tbl1["remarks"] = ""
    return {
        "rows": tbl1.values
    }




def get_close_court_client_data(df: pd.DataFrame) -> Dict[str, Any]:
    tbl_cases = load_cases_data()

    tbl1 = tbl_cases[(tbl_cases["case_type"] == "Litigation") & (tbl_cases["user_type"] == "Client") & (tbl_cases["case_status"] == "Closed")]
    tbl1["contractor_claim_or_award"] = tbl1["contractor_claim"].astype(str) + " " + tbl1["award_amount_contractor"].astype(str)
    tbl1["ircon_claim_or_award"] = tbl1["ircon_claim"].astype(str) + " " + tbl1["award_amount_ircon"].astype(str)
    tbl1 = tbl1[["claimant", "case_pertain", "borne_by", "contractor_claim_or_award", "ircon_claim_or_award"]]
    tbl1["remarks"] = ""

    return {
        "rows": tbl1.values
    }

def get_arb_lit_client_data(df: pd.DataFrame) -> Dict[str, Any]:
    data = {
        "arbitration": {},
        "litigation": {}
    }

    tbl_cases = load_cases_data()
    tbl_case_past = load_joined_cases_data()

    tmp1 = tbl_cases[(tbl_cases["case_type"] == "Arbitration") & (tbl_cases["user_type"] == "Client")][["ircon_claim", "client_claim"]]
    data["arbitration"]["pendency"] = (tmp1.count().loc['ircon_claim'],
    tmp1.sum().loc['ircon_claim'],
    tmp1.replace(0, np.nan).count().loc['client_claim'],
    tmp1.sum().loc['client_claim'])

    tmp1 = tbl_case_past[(tbl_case_past["case_type"] == "Arbitration") & (tbl_case_past["user_type"] == "Client")][["ircon_claim", "client_claim"]]
    data["arbitration"]["opening"] = (tmp1.count().loc['ircon_claim'],
    tmp1.sum().loc['ircon_claim'],
    tmp1.replace(0, np.nan).count().loc['client_claim'],
    tmp1.sum().loc['client_claim'])

    tmp1 = tbl_case_past[(tbl_case_past["case_type"] == "Arbitration") & (tbl_case_past["user_type"] == "Client") & (tbl_case_past["case_status"] == "Accreted")][["ircon_claim", "client_claim"]]
    data["arbitration"]["accreted"] = (tmp1.count().loc['ircon_claim'],
    tmp1.sum().loc['ircon_claim'],
    tmp1.replace(0, np.nan).count().loc['client_claim'],
    tmp1.sum().loc['client_claim'])

    tmp1 = tbl_case_past[(tbl_case_past["case_type"] == "Arbitration") & (tbl_case_past["user_type"] == "Client") & (tbl_case_past["case_status"] == "Closed")][["ircon_claim", "client_claim"]]
    data["arbitration"]["closed"] = (tmp1.count().loc['ircon_claim'],
    tmp1.sum().loc['ircon_claim'],
    tmp1.replace(0, np.nan).count().loc['client_claim'],
    tmp1.sum().loc['client_claim'])

    tmp1 = tbl_cases[(tbl_cases["case_type"] == "Litigation") & (tbl_cases["user_type"] == "Client")][["ircon_claim", "client_claim"]]
    data["litigation"]["pendency"] = (tmp1.count().loc['ircon_claim'],
    tmp1.sum().loc['ircon_claim'],
    tmp1.replace(0, np.nan).count().loc['client_claim'],
    tmp1.sum().loc['client_claim'])

    tmp1 = tbl_case_past[(tbl_case_past["case_type"] == "Litigation") & (tbl_case_past["user_type"] == "Client")][["ircon_claim", "client_claim"]]
    data["litigation"]["opening"] = (tmp1.count().loc['ircon_claim'],
    tmp1.sum().loc['ircon_claim'],
    tmp1.replace(0, np.nan).count().loc['client_claim'],
    tmp1.sum().loc['client_claim'])

    tmp1 = tbl_case_past[(tbl_case_past["case_type"] == "Litigation") & (tbl_case_past["user_type"] == "Client") & (tbl_case_past["case_status"] == "Accreted")][["ircon_claim", "client_claim"]]
    data["litigation"]["accreted"] = (tmp1.count().loc['ircon_claim'],
    tmp1.sum().loc['ircon_claim'],
    tmp1.replace(0, np.nan).count().loc['client_claim'],
    tmp1.sum().loc['client_claim'])

    tmp1 = tbl_case_past[(tbl_case_past["case_type"] == "Litigation") & (tbl_case_past["user_type"] == "Client") & (tbl_case_past["case_status"] == "Closed")][["ircon_claim", "client_claim"]]
    data["litigation"]["closed"] = (tmp1.count().loc['ircon_claim'],
    tmp1.sum().loc['ircon_claim'],
    tmp1.replace(0, np.nan).count().loc['client_claim'],
    tmp1.sum().loc['client_claim'])

    return {
        "rows": [
            (
                "Arbitration", 
                "Claims by IRCON in Nos.", 
                data["arbitration"]["opening"][0], 
                data["arbitration"]["accreted"][0], 
                data["arbitration"]["closed"][0], 
                data["arbitration"]["pendency"][0], 
            ),
            (
                "Arbitration", 
                "Claims Amt. by IRCON in Cr.", 
                round(data["arbitration"]["opening"][1], 2), 
                round(data["arbitration"]["accreted"][1], 2), 
                round(data["arbitration"]["closed"][1], 2), 
                round(data["arbitration"]["pendency"][1], 2), 
            ),
            (
                "Arbitration", 
                "C/Claims by Clients in Nos.", 
                data["arbitration"]["opening"][2], 
                data["arbitration"]["accreted"][2], 
                data["arbitration"]["closed"][2], 
                data["arbitration"]["pendency"][2], 
            ),
            (
                "Arbitration", "C/Claims by Clients Amt. in Cr.", 
                round(data["arbitration"]["opening"][3], 2), 
                round(data["arbitration"]["accreted"][3], 2), 
                round(data["arbitration"]["closed"][3], 2), 
                round(data["arbitration"]["pendency"][3], 2), 
            ),
            ("Court", "Award in favour of IRCON in Nos.",
                data["litigation"]["opening"][0], 
                data["litigation"]["accreted"][0], 
                data["litigation"]["closed"][0], 
                data["litigation"]["pendency"][0], 
            ),
            ("Court", "Award Amt. in favour of IRCON in Cr.", 
            round(data["litigation"]["opening"][1], 2), 
                round(data["litigation"]["accreted"][1], 2), 
                round(data["litigation"]["closed"][1], 2), 
                round(data["litigation"]["pendency"][1], 2), ),
            ("Court", "Award in favour of Clients in Nos.", 
            data["litigation"]["opening"][2], 
                data["litigation"]["accreted"][2], 
                data["litigation"]["closed"][2], 
                data["litigation"]["pendency"][2], ),
            ("Court", "Award/Claim Amt. in favour of Client in Cr.", 
                round(data["litigation"]["opening"][3], 2), 
                round(data["litigation"]["accreted"][3], 2), 
                round(data["litigation"]["closed"][3], 2), 
                round(data["litigation"]["pendency"][3], 2), 
            ),
        ]
    }

# Report builders
def build_report_tables(table_configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build multiple report tables based on configurations."""
    df = load_cases_data()
    past_df = load_joined_cases_data()  # Use joined data for past quarter
    tables = []
    
    for config in table_configs:
        table_type = config["type"]
        
        if table_type == "arb_lit_contractor":
            data = get_arb_lit_contractor_data(df, past_df)
        elif table_type == "arb_contractor":
            data = get_arb_contractor_data(df)
        elif table_type == "rev_arb_contractor":
            data = get_rev_arb_contractor_data(df)
        elif table_type == "close_arb_contractor":
            data = get_close_arb_contractor_data(df)
        elif table_type == "court_contractor":
            table_type = "arb_contractor"
            data = get_court_contractor_data(df)
        elif table_type == "rev_court_contractor":
            table_type = "rev_arb_contractor"
            data = get_rev_court_contractor_data(df)
        elif table_type == "close_court_contractor":
            data = get_close_court_contractor_data(df)
        elif table_type == "close_court_client":
            table_type == "close_court_contractor"
            data = get_close_court_client_data(df)
        elif table_type == "arb_lit_client":
            data = get_arb_lit_client_data(df)
        else:
            continue
            
        tables.append({
            "type": table_type,
            "title": config.get("title"),
            "data": data
        })
    
    return tables