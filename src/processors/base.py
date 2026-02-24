"""Base processor class for data processing operations."""
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import pandas as pd
import numpy as np

from src.logger import get_logger
from src.exceptions import DataProcessingError

logger = get_logger(__name__)


class BaseProcessor(ABC):
    """Abstract base class for data processors."""
    
    def __init__(self):
        """Initialize base processor."""
        self.logger = get_logger(self.__class__.__name__)
    
    @abstractmethod
    def process(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Process data and return results.
        
        Returns:
            Dictionary with processed data
        """
        pass
    
    def safe_filter(
        self, 
        df: pd.DataFrame, 
        filters: Dict[str, Any],
        case_sensitive: bool = False
    ) -> pd.DataFrame:
        """
        Safely filter DataFrame with error handling.
        
        Args:
            df: DataFrame to filter
            filters: Dictionary of column: value filters
            case_sensitive: Whether to use case-sensitive filtering
            
        Returns:
            Filtered DataFrame
        """
        try:
            result = df.copy()
            
            for column, value in filters.items():
                if column not in result.columns:
                    self.logger.warning(f"Column '{column}' not found in DataFrame")
                    continue
                
                if isinstance(value, str) and not case_sensitive:
                    result = result[result[column].str.lower() == value.lower()]
                elif isinstance(value, list):
                    result = result[result[column].isin(value)]
                else:
                    result = result[result[column] == value]
            
            self.logger.debug(f"Filtered {len(df)} rows to {len(result)} rows")
            return result
            
        except Exception as e:
            self.logger.error(f"Error filtering DataFrame: {e}")
            raise DataProcessingError(f"Filtering failed: {e}")
    
    def safe_aggregate(
        self, 
        df: pd.DataFrame, 
        columns: List[str],
        group_by: str = "borne_by"
    ) -> Dict[str, Any]:
        """
        Safely aggregate data with groupby and handle missing groups.
        
        Args:
            df: DataFrame to aggregate
            columns: Columns to aggregate
            group_by: Column to group by
            
        Returns:
            Dictionary with aggregated data
        """
        try:
            if df.empty:
                self.logger.warning("Empty DataFrame provided for aggregation")
                return {}
            
            # Validate columns exist
            missing_cols = set(columns) - set(df.columns)
            if missing_cols:
                self.logger.error(f"Missing columns for aggregation: {missing_cols}")
                return {}
            
            # Replace 0 with NaN for proper counting
            df_processed = df.copy()
            df_processed[columns] = df_processed[columns].replace(0, np.nan)
            
            # Aggregate and transpose
            result = df_processed.groupby(group_by).agg(["count", "sum"]).T
            
            # Ensure expected columns exist
            for expected_col in ['Client', 'Ircon']:
                if expected_col not in result.columns:
                    result[expected_col] = 0
                    self.logger.debug(f"Added missing column '{expected_col}' with default value 0")
            
            # Add total column
            result['total'] = result.get('Client', 0) + result.get('Ircon', 0)
            
            self.logger.debug(f"Aggregation completed successfully")
            return result.to_dict()
            
        except Exception as e:
            self.logger.error(f"Error in aggregation: {e}")
            raise DataProcessingError(f"Aggregation failed: {e}")
    
    def safe_get_value(
        self, 
        data: Dict, 
        keys: List[str], 
        default: Any = 0
    ) -> Any:
        """
        Safely get nested dictionary value with default.
        
        Args:
            data: Dictionary to query
            keys: List of keys for nested access
            default: Default value if key not found
            
        Returns:
            Value or default
        """
        try:
            result = data
            for key in keys:
                result = result.get(key, {})
            return result if result != {} else default
        except Exception:
            return default
    
    def format_currency(self, value: float, decimals: int = 2) -> str:
        """
        Format value as currency.
        
        Args:
            value: Numeric value
            decimals: Number of decimal places
            
        Returns:
            Formatted currency string
        """
        try:
            return f"₹{round(float(value), decimals)}"
        except (ValueError, TypeError):
            self.logger.warning(f"Invalid currency value: {value}")
            return f"₹0.00"
