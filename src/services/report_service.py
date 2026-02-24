"""Report generation service."""
from typing import List, Dict, Any
import pandas as pd

from src.logger import get_logger
from src.data_loader import DataLoader
from src.processors.contractor_processor import ContractorProcessor
from src.processors.client_processor import ClientProcessor
from src.exceptions import DataProcessingError
from src.models.request_models import ConsolidatedReportRequest

logger = get_logger(__name__)


class ReportService:
    """Service for generating various reports."""
    
    def __init__(self):
        """Initialize report service."""
        self.data_loader = DataLoader()
        self.contractor_processor = ContractorProcessor()
        self.client_processor = ClientProcessor()
        logger.info("ReportService initialized")
    
    def build_report_tables(
        self, 
        request_data: ConsolidatedReportRequest,
        table_configs: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Build multiple report tables based on configurations.
        
        Args:
            request_data: Validated request data with tbl_cases and tbl_case_past
            table_configs: List of table configuration dictionaries
            
        Returns:
            List of processed table data
        """
        try:
            logger.info(f"Building {len(table_configs)} report tables")
            
            # Convert request data to DataFrames
            df = self.data_loader.load_cases_data(request_data.tbl_cases)
            past_df = self.data_loader.load_past_cases_data(request_data.tbl_case_past)
            
            tables = []
            
            for config in table_configs:
                try:
                    table = self._process_single_table(config, df, past_df)
                    if table:
                        tables.append(table)
                except Exception as e:
                    logger.error(f"Error processing table '{config.get('type')}': {e}")
                    # Continue processing other tables
                    continue
            
            logger.info(f"Successfully built {len(tables)} report tables")
            return tables
            
        except Exception as e:
            logger.error(f"Error building report tables: {e}")
            raise DataProcessingError(f"Failed to build reports: {e}")
    
    def _process_single_table(
        self,
        config: Dict[str, Any],
        df: pd.DataFrame,
        past_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """Process a single table configuration."""
        table_type = config["type"]
        logger.debug(f"Processing table type: {table_type}")
        
        # Map table types to processor methods
        processor_map = {
            "arb_lit_contractor": lambda: self.contractor_processor.process_arb_lit_contractor(df, past_df),
            "arb_contractor": lambda: self.contractor_processor.process_arb_contractor(df, past_df),
            "court_contractor": lambda: self.contractor_processor.process_court_contractor(df, past_df),
            "rev_arb_contractor": lambda: self.contractor_processor.process_revised_cases(df, "Arbitration"),
            "rev_court_contractor": lambda: self.contractor_processor.process_revised_cases(df, "Litigation"),
            "close_arb_contractor": lambda: self.contractor_processor.process_closed_cases(df, "Arbitration"),
            "close_court_contractor": lambda: self.contractor_processor.process_closed_cases(df, "Litigation"),
            "close_court_client": lambda: self.client_processor.process_closed_client_cases(df, "Litigation"),
            "arb_lit_client": lambda: self.client_processor.process_arb_lit_client(df, past_df),
        }
        
        processor = processor_map.get(table_type)
        
        if not processor:
            logger.warning(f"Unknown table type: {table_type}")
            return None
        
        data = processor()
        
        # Map display table type for templates
        display_type_map = {
            "court_contractor": "arb_contractor",
            "rev_court_contractor": "rev_arb_contractor",
            "close_court_client": "close_court_contractor",
        }
        
        display_type = display_type_map.get(table_type, table_type)
        
        return {
            "type": display_type,
            "title": config.get("title"),
            "data": data
        }
