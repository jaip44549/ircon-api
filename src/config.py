"""Application configuration management."""
import os
from typing import Optional
from src.logger import get_logger
from src.exceptions import ConfigurationError

logger = get_logger(__name__)


class Config:
    """Application configuration with validation."""
    
    def __init__(self):
        """Initialize configuration from environment variables."""
        self._load_config()
        self._validate_config()
    
    def _load_config(self):
        """Load configuration from environment variables."""
        # Database configuration
        self.DB_TYPE: str = os.getenv("DB_TYPE", "postgresql")
        self.DB_USER: str = os.getenv("DB_USER", "postgres")
        self.DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
        self.DB_HOST: str = os.getenv("DB_HOST", "localhost")
        self.DB_PORT: str = os.getenv("DB_PORT", "5432")
        self.DB_NAME: str = os.getenv("DB_NAME", "ircon")
        
        # Table names
        self.CASES_TABLE: str = os.getenv("CASES_TABLE", "tbl_cases")
        self.PAST_CASES_TABLE: str = os.getenv("PAST_CASES_TABLE", "tbl_case_past")
        
        # Application settings
        self.MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
        self.RETRY_DELAY: float = float(os.getenv("RETRY_DELAY", "1.0"))
        self.QUERY_TIMEOUT: int = int(os.getenv("QUERY_TIMEOUT", "30"))
        
        logger.info("Configuration loaded successfully")
    
    def _validate_config(self):
        """Validate configuration values."""
        if not self.DB_NAME:
            raise ConfigurationError("DB_NAME is required")
        
        if not self.DB_USER:
            raise ConfigurationError("DB_USER is required")
        
        if self.DB_TYPE not in ["postgresql", "mysql", "mariadb", "sqlite"]:
            logger.warning(f"Unsupported DB_TYPE: {self.DB_TYPE}, proceeding anyway")
        
        logger.info("Configuration validated successfully")
    
    @property
    def connection_string(self) -> str:
        """Get database connection string."""
        if self.DB_TYPE.lower() in ["mysql", "mariadb"]:
            return f"mysql+pymysql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        elif self.DB_TYPE.lower() == "sqlite":
            return f"sqlite:///{self.DB_NAME}.db"
        return f"{self.DB_TYPE}://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


# Singleton instance
_config_instance: Optional[Config] = None


def get_config() -> Config:
    """Get or create configuration instance."""
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance


# For backward compatibility
config = get_config()
