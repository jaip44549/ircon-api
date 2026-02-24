import os
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd

class DatabaseConnection:
    """Database connection manager."""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize database connection.
        
        Args:
            connection_string: Database connection string. If None, reads from environment.
        """
        self.connection_string = connection_string or self._get_connection_string()
        self._engine: Optional[Engine] = None
    
    def _get_connection_string(self) -> str:
        """Get connection string from environment variables."""
        db_type = os.getenv("DB_TYPE", "postgresql")
        db_user = os.getenv("DB_USER", "postgres")
        db_password = os.getenv("DB_PASSWORD", "")
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "ircon")
        
        # Use pymysql for MySQL connections
        if db_type.lower() in ["mysql", "mariadb"]:
            return f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        return f"{db_type}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    @property
    def engine(self) -> Engine:
        """Get or create database engine."""
        if self._engine is None:
            self._engine = create_engine(self.connection_string)
        return self._engine
    
    def read_query(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as pandas DataFrame.
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame with query results
        """
        return pd.read_sql(query, self.engine)
    
    def read_table(self, table_name: str) -> pd.DataFrame:
        """
        Read entire table into pandas DataFrame.
        
        Args:
            table_name: Name of the table
            
        Returns:
            DataFrame with table data
        """
        return pd.read_sql_table(table_name, self.engine)
    
    def close(self):
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None


# Global database instance
_db_instance: Optional[DatabaseConnection] = None


def get_db() -> DatabaseConnection:
    """Get or create global database instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = DatabaseConnection()
    return _db_instance


def close_db():
    """Close global database connection."""
    global _db_instance
    if _db_instance:
        _db_instance.close()
        _db_instance = None
