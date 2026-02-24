"""Client-related data processing."""
from typing import Dict, Any, Tuple
import pandas as pd
import numpy as np

from src.processors.base import BaseProcessor
from src.data_loader import DataLoader
from src.exceptions import DataProcessingError


class ClientProcessor(BaseProcessor):
    """Processes client-related case data."""
    
    def __init__(self):
        """Initialize client processor."""
        super().__init__()
        self.data_loader = DataLoader()
    
    def process(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Generic process method (required by base class).
        Use specific methods like process_arb_lit_client() instead.
        """
        raise NotImplementedError("Use specific processing methods")
    
    def process_arb_lit_client(self, df: pd.DataFrame, past_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Process arbitration and litigation with client data.
        
        Returns:
            Processed data dictionary
        """
        try:
            self.logger.info("Processing arbitration and litigation client data")
            
            data = {
                "arbitration": {},
                "litigation": {}
            }
            
            # Process arbitration
            data["arbitration"] = self._process_client_case_type(
                df, past_df, "Arbitration"
            )
            
            # Process litigation
            data["litigation"] = self._process_client_case_type(
                df, past_df, "Litigation"
            )
            
            rows = self._build_client_rows(data)
            
            self.logger.info("Arbitration and litigation client data processed successfully")
            return {"rows": rows}
            
        except Exception as e:
            self.logger.error(f"Error processing arb_lit_client data: {e}")
            raise DataProcessingError(f"Failed to process arb_lit_client: {e}")
    
    def _process_client_case_type(
        self,
        tbl_cases: pd.DataFrame,
        tbl_case_past: pd.DataFrame,
        case_type: str
    ) -> Dict[str, Tuple]:
        """Process client data for a specific case type."""
        data = {}
        
        # Pendency (current cases)
        filtered = self.safe_filter(tbl_cases, {
            "case_type": case_type,
            "user_type": "Client"
        })
        data["pendency"] = self._extract_claim_stats(
            filtered[["ircon_claim", "client_claim"]]
        )
        
        # Opening (past cases)
        filtered = self.safe_filter(tbl_case_past, {
            "case_type": case_type,
            "user_type": "Client"
        })
        data["opening"] = self._extract_claim_stats(
            filtered[["ircon_claim", "client_claim"]]
        )
        
        # Accreted
        filtered = self.safe_filter(tbl_case_past, {
            "case_type": case_type,
            "user_type": "Client",
            "case_status": "Accreted"
        })
        data["accreted"] = self._extract_claim_stats(
            filtered[["ircon_claim", "client_claim"]]
        )
        
        # Closed
        filtered = self.safe_filter(tbl_case_past, {
            "case_type": case_type,
            "user_type": "Client",
            "case_status": "Closed"
        })
        data["closed"] = self._extract_claim_stats(
            filtered[["ircon_claim", "client_claim"]]
        )
        
        return data
    
    def _extract_claim_stats(self, df: pd.DataFrame) -> Tuple:
        """
        Extract claim statistics from DataFrame.
        
        Args:
            df: DataFrame with ircon_claim and client_claim columns
            
        Returns:
            Tuple of (ircon_count, ircon_sum, client_count, client_sum)
        """
        if df.empty:
            return (0, 0.0, 0, 0.0)
        
        try:
            ircon_count = df["ircon_claim"].count()
            ircon_sum = df["ircon_claim"].sum()
            
            # Replace 0 with NaN for client claims to exclude from count
            client_claims = df["client_claim"].replace(0, np.nan)
            client_count = client_claims.count()
            client_sum = df["client_claim"].sum()
            
            return (ircon_count, ircon_sum, client_count, client_sum)
            
        except Exception as e:
            self.logger.error(f"Error extracting claim stats: {e}")
            return (0, 0.0, 0, 0.0)
    
    def _build_client_rows(self, data: Dict[str, Any]) -> list:
        """Build rows for client report."""
        arb = data["arbitration"]
        lit = data["litigation"]
        
        return [
            # Arbitration - IRCON Claims
            (
                "Arbitration",
                "Claims by IRCON in Nos.",
                arb["opening"][0],
                arb["accreted"][0],
                arb["closed"][0],
                arb["pendency"][0],
            ),
            (
                "Arbitration",
                "Claims Amt. by IRCON in Cr.",
                round(arb["opening"][1], 2),
                round(arb["accreted"][1], 2),
                round(arb["closed"][1], 2),
                round(arb["pendency"][1], 2),
            ),
            # Arbitration - Client Claims
            (
                "Arbitration",
                "C/Claims by Clients in Nos.",
                arb["opening"][2],
                arb["accreted"][2],
                arb["closed"][2],
                arb["pendency"][2],
            ),
            (
                "Arbitration",
                "C/Claims by Clients Amt. in Cr.",
                round(arb["opening"][3], 2),
                round(arb["accreted"][3], 2),
                round(arb["closed"][3], 2),
                round(arb["pendency"][3], 2),
            ),
            # Court - IRCON Awards
            (
                "Court",
                "Award in favour of IRCON in Nos.",
                lit["opening"][0],
                lit["accreted"][0],
                lit["closed"][0],
                lit["pendency"][0],
            ),
            (
                "Court",
                "Award Amt. in favour of IRCON in Cr.",
                round(lit["opening"][1], 2),
                round(lit["accreted"][1], 2),
                round(lit["closed"][1], 2),
                round(lit["pendency"][1], 2),
            ),
            # Court - Client Awards
            (
                "Court",
                "Award in favour of Clients in Nos.",
                lit["opening"][2],
                lit["accreted"][2],
                lit["closed"][2],
                lit["pendency"][2],
            ),
            (
                "Court",
                "Award/Claim Amt. in favour of Client in Cr.",
                round(lit["opening"][3], 2),
                round(lit["accreted"][3], 2),
                round(lit["closed"][3], 2),
                round(lit["pendency"][3], 2),
            ),
        ]
    
    def process_closed_client_cases(self, df: pd.DataFrame, case_type: str) -> Dict[str, Any]:
        """Process closed client cases."""
        try:
            self.logger.info(f"Processing closed {case_type} client cases")
            
            filtered = self.safe_filter(df, {
                "case_type": case_type,
                "user_type": "Client",
                "case_status": "Closed"
            })
            
            filtered["contractor_claim_or_award"] = (
                filtered["contractor_claim"].astype(str) + " " + 
                filtered["award_amount_contractor"].astype(str)
            )
            filtered["ircon_claim_or_award"] = (
                filtered["ircon_claim"].astype(str) + " " + 
                filtered["award_amount_ircon"].astype(str)
            )
            
            result = filtered[[
                "claimant", "case_pertain", "borne_by",
                "contractor_claim_or_award", "ircon_claim_or_award"
            ]].copy()
            result["remarks"] = ""
            
            self.logger.info(f"Processed {len(result)} closed client cases")
            return {"rows": result.values.tolist()}
            
        except Exception as e:
            self.logger.error(f"Error processing closed client cases: {e}")
            raise DataProcessingError(f"Failed to process closed client cases: {e}")
