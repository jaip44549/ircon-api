"""Data loading operations with caching and fault tolerance."""
from typing import Optional
import pandas as pd
from functools import lru_cache

from src.config import get_config
from src.database import get_db
from src.logger import get_logger
from src.exceptions import DataProcessingError

logger = get_logger(__name__)


class DataLoader:
    """Handles data loading operations with caching."""
    
    def __init__(self):
        """Initialize data loader."""
        self.config = get_config()
        self.db = get_db()
        logger.info("DataLoader initialized")
    
    def load_cases_data(self, table_name: Optional[str] = None) -> pd.DataFrame:
        """
        Load cases data from database with fault tolerance.
        
        Args:
            table_name: Optional table name, defaults to config value
            
        Returns:
            DataFrame with cases data
        """
        table = table_name or self.config.CASES_TABLE
        
        try:
            df = self.db.read_table(table)
            
            if df.empty:
                logger.warning(f"Table '{table}' is empty")
            
            logger.info(f"Loaded {len(df)} records from '{table}'")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load cases data from '{table}': {e}")
            raise DataProcessingError(f"Failed to load cases data: {e}")
    
    def load_joined_cases_data(self) -> pd.DataFrame:
        """
        Load cases data joined with past data.
        
        Returns:
            DataFrame with joined case data
        """
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
        
        try:
            df = self.db.read_query(query)
            
            if df.empty:
                logger.warning("Joined cases query returned empty result")
            
            logger.info(f"Loaded {len(df)} joined case records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load joined cases data: {e}")
            raise DataProcessingError(f"Failed to load joined cases: {e}")
    
    def validate_dataframe(self, df: pd.DataFrame, required_columns: list) -> bool:
        """
        Validate DataFrame has required columns.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            True if valid
        """
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            raise DataProcessingError(f"Missing columns: {missing_columns}")
        
        return True
