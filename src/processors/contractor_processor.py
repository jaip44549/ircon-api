"""Contractor-related data processing."""
from typing import Dict, Any
import pandas as pd
import numpy as np

from src.processors.base import BaseProcessor
from src.data_loader import DataLoader
from src.exceptions import DataProcessingError


class ContractorProcessor(BaseProcessor):
    """Processes contractor-related case data."""
    
    def __init__(self):
        """Initialize contractor processor."""
        super().__init__()
        self.data_loader = DataLoader()
    
    def process(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Generic process method (required by base class).
        Use specific methods like process_arb_contractor() instead.
        """
        raise NotImplementedError("Use specific processing methods")
    
    def process_arb_lit_contractor(
        self, 
        df: pd.DataFrame, 
        past_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Process arbitration and litigation with contractor data.
        
        Args:
            df: Current cases DataFrame
            past_df: Past cases DataFrame
            
        Returns:
            Processed data dictionary
        """
        try:
            self.logger.info("Processing arbitration and litigation contractor data")
            
            data = {
                "old_q": {"arb_val": [], "lit_val": []},
                "q": {"arb_val": [], "lit_val": []}
            }
            
            # Process arbitration data
            arb_data = self._process_case_type(df, past_df, "arbitration")
            data["old_q"]["arb_val"] = arb_data["old_q"]
            data["q"]["arb_val"] = arb_data["q"]
            
            # Process litigation data
            lit_data = self._process_case_type(df, past_df, "litigation")
            data["old_q"]["lit_val"] = lit_data["old_q"]
            data["q"]["lit_val"] = lit_data["q"]
            
            self.logger.info("Arbitration and litigation contractor data processed successfully")
            return data
            
        except Exception as e:
            self.logger.error(f"Error processing arb_lit_contractor data: {e}")
            raise DataProcessingError(f"Failed to process arb_lit_contractor: {e}")
    
    def _process_case_type(
        self, 
        df: pd.DataFrame, 
        past_df: pd.DataFrame, 
        case_type: str
    ) -> Dict[str, Any]:
        """Process data for a specific case type."""
        # Current quarter
        current = self.safe_filter(df, {
            "case_type": case_type,
            "user_type": "contractor"
        })
        
        client_cases = self.safe_filter(current, {"borne_by": "client"})
        ircon_cases = self.safe_filter(current, {"borne_by": "ircon"})
        
        # Past quarter
        past = self.safe_filter(past_df, {
            "case_type": case_type,
            "user_type": "contractor"
        })
        
        p_client_cases = self.safe_filter(past, {"borne_by": "client"})
        p_ircon_cases = self.safe_filter(past, {"borne_by": "ircon"})
        
        return {
            "old_q": [
                (len(past), len(p_ircon_cases)),
                (
                    self.format_currency(p_client_cases["contractor_claim"].sum()),
                    self.format_currency(p_ircon_cases["ircon_claim"].sum())
                )
            ],
            "q": [
                (len(current), len(ircon_cases)),
                (
                    self.format_currency(client_cases["contractor_claim"].sum()),
                    self.format_currency(ircon_cases["ircon_claim"].sum())
                )
            ]
        }
    
    def process_arb_contractor(self, df: pd.DataFrame, past_df: pd.DataFrame) -> Dict[str, Any]:
        """Process arbitration contractor position summary."""
        try:
            self.logger.info("Processing arbitration contractor data")
            
            data = {}
            
            # Pendency (current cases with In Progress status)
            data["pendency"] = self._aggregate_contractor_data(
                df, "Arbitration", "Contractor", "In Progress"
            )
            
            # Opening (past cases with In Progress status)
            data["opening"] = self._aggregate_contractor_data(
                past_df, "Arbitration", "Contractor", "In Progress"
            )
            
            # Accretion (current cases with Accreted status)
            data["accretion"] = self._aggregate_contractor_data(
                df, "Arbitration", "Contractor", "Accreted"
            )
            
            # Closed (current cases with Closed/Settled/Awarded status)
            data["closed"] = self._aggregate_contractor_data(
                df, "Arbitration", "Contractor", 
                ["Closed", "Settled", "Awarded"]
            )
            
            rows = self._build_contractor_rows(data)
            
            self.logger.info("Arbitration contractor data processed successfully")
            return {"rows": rows}
            
        except Exception as e:
            self.logger.error(f"Error processing arb_contractor data: {e}")
            raise DataProcessingError(f"Failed to process arb_contractor: {e}")
    
    def process_court_contractor(self, df: pd.DataFrame, past_df: pd.DataFrame) -> Dict[str, Any]:
        """Process court contractor position summary."""
        try:
            self.logger.info("Processing court contractor data")
            
            data = {}
            
            # Pendency (current cases with In Progress status)
            data["pendency"] = self._aggregate_contractor_data(
                df, "Litigation", "Contractor", "In Progress"
            )
            
            # Opening (past cases with In Progress status)
            data["opening"] = self._aggregate_contractor_data(
                past_df, "Litigation", "Contractor", "In Progress"
            )
            
            # Accretion (current cases with Accreted status)
            data["accretion"] = self._aggregate_contractor_data(
                df, "Litigation", "Contractor", "Accreted"
            )
            
            # Closed (current cases with Closed/Settled/Awarded status)
            data["closed"] = self._aggregate_contractor_data(
                df, "Litigation", "Contractor", 
                ["Closed", "Settled", "Awarded"]
            )
            
            rows = self._build_contractor_rows(data)
            
            self.logger.info("Court contractor data processed successfully")
            return {"rows": rows}
            
        except Exception as e:
            self.logger.error(f"Error processing court_contractor data: {e}")
            raise DataProcessingError(f"Failed to process court_contractor: {e}")
    
    def _aggregate_contractor_data(
        self,
        df: pd.DataFrame,
        case_type: str,
        user_type: str,
        case_status: Any = None
    ) -> Dict[str, Any]:
        """Aggregate contractor data with filters."""
        filters = {"case_type": case_type, "user_type": user_type}
        
        if case_status:
            filters["case_status"] = case_status
        
        filtered = self.safe_filter(df, filters)
        selected = filtered[["borne_by", "ircon_claim", "contractor_claim"]]
        
        return self.safe_aggregate(selected, ["ircon_claim", "contractor_claim"])
    
    def _build_contractor_rows(self, data: Dict[str, Any]) -> list:
        """Build rows for contractor report."""
        return [
            self._build_row(data, "Contractor's Claims on IRCON", "Nos.", "contractor_claim", "count", int),
            self._build_row(data, "Contractor's Claims on IRCON", "Amt in Crs.", "contractor_claim", "sum", lambda x: round(x, 2)),
            self._build_row(data, "IRCON's Claims on Contractor", "Nos.", "ircon_claim", "count", int),
            self._build_row(data, "IRCON's Claims on Contractor", "Amt in Crs.", "ircon_claim", "sum", lambda x: round(x, 2)),
        ]
    
    def _build_row(
        self, 
        data: Dict[str, Any], 
        category: str, 
        metric: str, 
        claim_type: str, 
        agg_type: str,
        formatter
    ) -> Dict[str, Any]:
        """Build a single row for the report."""
        return {
            "category": category,
            "metric": metric,
            "opening_client": formatter(self.safe_get_value(data, ["opening", "Client", (claim_type, agg_type)])),
            "opening_ircon": formatter(self.safe_get_value(data, ["opening", "Ircon", (claim_type, agg_type)])),
            "opening_total": formatter(self.safe_get_value(data, ["opening", "total", (claim_type, agg_type)])),
            "accretion_client": formatter(self.safe_get_value(data, ["accretion", "Client", (claim_type, agg_type)])),
            "accretion_ircon": formatter(self.safe_get_value(data, ["accretion", "Ircon", (claim_type, agg_type)])),
            "closed_client": formatter(self.safe_get_value(data, ["closed", "Client", (claim_type, agg_type)])),
            "closed_ircon": formatter(self.safe_get_value(data, ["closed", "Ircon", (claim_type, agg_type)])),
            "closed_total": formatter(self.safe_get_value(data, ["closed", "total", (claim_type, agg_type)])),
            "pendency_client": formatter(self.safe_get_value(data, ["pendency", "Client", (claim_type, agg_type)])),
            "pendency_ircon": formatter(self.safe_get_value(data, ["pendency", "Ircon", (claim_type, agg_type)])),
            "pendency_total": formatter(self.safe_get_value(data, ["pendency", "total", (claim_type, agg_type)])),
        }
    
    def process_closed_cases(
        self, 
        df: pd.DataFrame,
        case_type: str, 
        user_type: str = "Contractor"
    ) -> Dict[str, Any]:
        """Process closed cases data."""
        try:
            self.logger.info(f"Processing closed {case_type} {user_type} cases")
            
            filtered = self.safe_filter(df, {
                "case_type": case_type,
                "user_type": user_type,
                "case_status": "Closed"
            })
            
            if case_type == "Litigation":
                filtered["contractor_claim_or_award"] = (
                    filtered["contractor_claim"].astype(str) + " " + 
                    filtered["award_amount_contractor"].astype(str)
                )
                filtered["ircon_claim_or_award"] = (
                    filtered["ircon_claim"].astype(str) + " " + 
                    filtered["award_amount_ircon"].astype(str)
                )
                columns = ["claimant", "case_pertain", "borne_by", 
                          "contractor_claim_or_award", "ircon_claim_or_award"]
            else:
                columns = ["claimant", "case_pertain", "borne_by", 
                          "contractor_claim", "award_amount_contractor", 
                          "ircon_claim", "award_amount_ircon"]
            
            result = filtered[columns].copy()
            result["remarks"] = ""
            
            self.logger.info(f"Processed {len(result)} closed cases")
            return {"rows": result.values.tolist()}
            
        except Exception as e:
            self.logger.error(f"Error processing closed cases: {e}")
            raise DataProcessingError(f"Failed to process closed cases: {e}")
    
    def process_revised_cases(
        self, 
        df: pd.DataFrame,
        case_type: str, 
        user_type: str = "Contractor"
    ) -> Dict[str, Any]:
        """Process revised/accreted cases data."""
        try:
            self.logger.info(f"Processing revised {case_type} {user_type} cases")
            
            filtered = self.safe_filter(df, {
                "case_type": case_type,
                "user_type": user_type,
                "case_status": "Accreted"
            })
            
            if case_type == "Litigation":
                filtered["contractor_claim_or_award"] = (
                    filtered["contractor_claim"].astype(str) + " " + 
                    filtered["award_amount_contractor"].astype(str)
                )
                filtered["ircon_claim_or_award"] = (
                    filtered["ircon_claim"].astype(str) + " " + 
                    filtered["award_amount_ircon"].astype(str)
                )
                columns = ["claimant", "case_pertain", "borne_by", 
                          "contractor_claim_or_award", "ircon_claim_or_award"]
            else:
                columns = ["claimant", "case_pertain", "borne_by", 
                          "contractor_claim", "ircon_claim"]
            
            result = filtered[columns].copy()
            result["remarks"] = ""
            
            self.logger.info(f"Processed {len(result)} revised cases")
            return {"rows": result.values.tolist()}
            
        except Exception as e:
            self.logger.error(f"Error processing revised cases: {e}")
            raise DataProcessingError(f"Failed to process revised cases: {e}")
