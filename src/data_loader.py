"""Data loading operations from request data."""
from typing import List, Dict, Any
import pandas as pd

from src.logger import get_logger
from src.exceptions import DataProcessingError
from src.models.request_models import CaseRecord, PastCaseRecord

logger = get_logger(__name__)


class DataLoader:
    """Handles data loading operations from request data."""
    
    def __init__(self):
        """Initialize data loader."""
        logger.info("DataLoader initialized")
    
    def load_cases_data(self, cases: List[CaseRecord]) -> pd.DataFrame:
        """
        Convert case records to DataFrame.
        
        Args:
            cases: List of CaseRecord objects
            
        Returns:
            DataFrame with cases data
        """
        try:
            if not cases:
                logger.warning("Empty cases list provided")
                return pd.DataFrame()
            
            # Convert Pydantic models to dictionaries
            data = [case.model_dump() for case in cases]
            df = pd.DataFrame(data)
            
            logger.info(f"Loaded {len(df)} case records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load cases data: {e}")
            raise DataProcessingError(f"Failed to load cases data: {e}")
    
    def load_past_cases_data(self, past_cases: List[PastCaseRecord]) -> pd.DataFrame:
        """
        Convert past case records to DataFrame.
        
        Args:
            past_cases: List of PastCaseRecord objects
            
        Returns:
            DataFrame with past cases data
        """
        try:
            if not past_cases:
                logger.warning("Empty past cases list provided")
                return pd.DataFrame()
            
            # Convert Pydantic models to dictionaries
            data = [case.model_dump() for case in past_cases]
            df = pd.DataFrame(data)
            
            logger.info(f"Loaded {len(df)} past case records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load past cases data: {e}")
            raise DataProcessingError(f"Failed to load past cases data: {e}")
    
    def load_joined_cases_data(
        self, 
        cases: List[CaseRecord], 
        past_cases: List[PastCaseRecord]
    ) -> pd.DataFrame:
        """
        Load past cases data joined with current cases data.
        
        This simulates the SQL JOIN to get case_type, user_type, and borne_by
        from tbl_cases for each past case record.
        
        Args:
            cases: List of CaseRecord objects
            past_cases: List of PastCaseRecord objects
            
        Returns:
            DataFrame with joined data
        """
        try:
            if not past_cases or not cases:
                logger.warning("Empty cases or past_cases list provided for join")
                return pd.DataFrame()
            
            # Convert to DataFrames
            cases_df = self.load_cases_data(cases)
            past_df = self.load_past_cases_data(past_cases)
            
            # Select only needed columns from cases
            cases_subset = cases_df[['id', 'case_type', 'user_type', 'borne_by']].copy()
            
            # Rename id to case_id for joining
            cases_subset = cases_subset.rename(columns={'id': 'case_id'})
            
            # Join past cases with current cases
            joined_df = past_df.merge(
                cases_subset,
                on='case_id',
                how='left'
            )
            
            logger.info(f"Loaded {len(joined_df)} joined case records")
            return joined_df
            
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
