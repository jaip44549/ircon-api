"""Database connection and operations with fault tolerance."""
import time
from typing import Optional
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import pandas as pd

from src.config import get_config
from src.logger import get_logger
from src.exceptions import DatabaseConnectionError, DataProcessingError

logger = get_logger(__name__)


class DatabaseConnection:
    """Database connection manager with retry logic and fault tolerance."""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize database connection.
        
        Args:
            connection_string: Database connection string. If None, uses config.
        """
        self.config = get_config()
        self.connection_string = connection_string or self.config.connection_string
        self._engine: Optional[Engine] = None
        logger.info("DatabaseConnection initialized")
    
    @property
    def engine(self) -> Engine:
        """Get or create database engine with connection pooling."""
        if self._engine is None:
            try:
                self._engine = create_engine(
                    self.connection_string,
                    pool_pre_ping=True,  # Verify connections before using
                    pool_recycle=3600,   # Recycle connections after 1 hour
                    pool_size=5,
                    max_overflow=10,
                    connect_args={"connect_timeout": self.config.QUERY_TIMEOUT}
                )
                logger.info("Database engine created successfully")
            except Exception as e:
                logger.error(f"Failed to create database engine: {e}")
                raise DatabaseConnectionError(f"Database connection failed: {e}")
        return self._engine
    
    def _retry_operation(self, operation, *args, **kwargs):
        """
        Retry database operation with exponential backoff.
        
        Args:
            operation: Function to execute
            *args, **kwargs: Arguments for the operation
            
        Returns:
            Result of the operation
        """
        max_retries = self.config.MAX_RETRIES
        retry_delay = self.config.RETRY_DELAY
        
        for attempt in range(max_retries):
            try:
                return operation(*args, **kwargs)
            except OperationalError as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(
                        f"Database operation failed (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Database operation failed after {max_retries} attempts: {e}")
                    raise DatabaseConnectionError(f"Operation failed after {max_retries} retries: {e}")
            except Exception as e:
                logger.error(f"Unexpected error in database operation: {e}")
                raise DataProcessingError(f"Database operation error: {e}")
    
    def read_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query with fault tolerance.
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame with query results
        """
        logger.debug(f"Executing query: {query[:100]}...")
        
        def _execute_query():
            try:
                df = pd.read_sql(query, self.engine)
                logger.info(f"Query executed successfully, returned {len(df)} rows")
                return df
            except pd.errors.DatabaseError as e:
                logger.error(f"Pandas database error: {e}")
                raise DataProcessingError(f"Query execution failed: {e}")
        
        return self._retry_operation(_execute_query)
    
    def read_table(self, table_name: str) -> pd.DataFrame:
        """
        Read entire table with fault tolerance.
        
        Args:
            table_name: Name of the table
            
        Returns:
            DataFrame with table data
        """
        logger.debug(f"Reading table: {table_name}")
        
        def _read_table():
            try:
                df = pd.read_sql_table(table_name, self.engine)
                logger.info(f"Table '{table_name}' read successfully, {len(df)} rows")
                return df
            except ValueError as e:
                logger.error(f"Table '{table_name}' not found: {e}")
                raise DataProcessingError(f"Table not found: {table_name}")
            except pd.errors.DatabaseError as e:
                logger.error(f"Error reading table '{table_name}': {e}")
                raise DataProcessingError(f"Failed to read table: {e}")
        
        return self._retry_operation(_read_table)
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection is successful
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def close(self):
        """Close database connection and cleanup resources."""
        if self._engine:
            try:
                self._engine.dispose()
                self._engine = None
                logger.info("Database connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")


# Singleton instance
_db_instance: Optional[DatabaseConnection] = None


def get_db() -> DatabaseConnection:
    """Get or create global database instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = DatabaseConnection()
        logger.info("Global database instance created")
    return _db_instance


def close_db():
    """Close global database connection."""
    global _db_instance
    if _db_instance:
        _db_instance.close()
        _db_instance = None
        logger.info("Global database instance closed")


@contextmanager
def get_db_session():
    """Context manager for database sessions."""
    db = get_db()
    try:
        yield db
    finally:
        pass  # Connection pooling handles cleanup
