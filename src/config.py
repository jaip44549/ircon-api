import os

class Config:
    """Application configuration."""
    
    # Database configuration
    DB_TYPE: str = os.getenv("DB_TYPE", "postgresql")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: str = os.getenv("DB_PORT", "5432")
    DB_NAME: str = os.getenv("DB_NAME", "ircon")
    
    # Table names in database
    CASES_TABLE: str = os.getenv("CASES_TABLE", "tbl_cases")
    PAST_CASES_TABLE: str = os.getenv("PAST_CASES_TABLE", "tbl_case_past")


config = Config()
