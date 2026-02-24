"""Custom exceptions for the application."""


class BaseAppException(Exception):
    """Base exception for all application exceptions."""
    pass


class DatabaseConnectionError(BaseAppException):
    """Raised when database connection fails."""
    pass


class DataProcessingError(BaseAppException):
    """Raised when data processing fails."""
    pass


class ConfigurationError(BaseAppException):
    """Raised when configuration is invalid."""
    pass


class DataValidationError(BaseAppException):
    """Raised when data validation fails."""
    pass
