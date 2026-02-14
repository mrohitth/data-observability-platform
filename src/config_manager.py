"""
Production Configuration Manager

Handles environment variable loading and validation for the Data Observability Platform.
Provides centralized configuration management with type safety and validation.
"""

import os
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    name: str
    user: str
    password: str
    timeout: int = 30
    pool_size: int = 5
    max_overflow: int = 10
    ssl_enabled: bool = False
    ssl_cert_path: Optional[str] = None
    ssl_key_path: Optional[str] = None
    
    def get_connection_string(self) -> str:
        """Generate PostgreSQL connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


@dataclass
class RetryConfig:
    """Retry configuration with exponential backoff"""
    max_attempts: int = 3
    backoff_factor: float = 2.0
    initial_delay: float = 1.0
    max_delay: float = 60.0


@dataclass
class LoggingConfig:
    """Logging configuration with rotation"""
    level: str = "INFO"
    format_type: str = "structured"
    log_dir: str = "logs"
    max_bytes: int = 10485760  # 10MB
    backup_count: int = 5
    enable_console: bool = True
    enable_file: bool = True


@dataclass
class MonitoringConfig:
    """Monitoring and alerting configuration"""
    volume_anomaly_threshold: float = 3.0
    freshness_threshold_minutes: int = 30
    contract_validation_mode: str = "strict"
    sampling_size: int = 100
    alert_severity: str = "CRITICAL"
    webhook_enabled: bool = False
    email_enabled: bool = False


@dataclass
class PerformanceConfig:
    """Performance and concurrency configuration"""
    concurrent_workers: int = 4
    batch_size: int = 1000
    connection_timeout: int = 30


class ConfigManager:
    """
    Production configuration manager with environment variable support
    
    Provides centralized configuration loading, validation, and type safety
    for all Data Observability Platform components.
    """
    
    def __init__(self, env_file: str = ".env"):
        """Initialize configuration manager"""
        self.logger = self._setup_logger()
        
        # Load environment variables
        load_dotenv(env_file)
        self.logger.info(f"Environment configuration loaded from {env_file}")
        
        # Initialize configuration objects
        self.batch_db = self._load_database_config("BATCH_DB")
        self.cdc_db = self._load_database_config("CDC_DB")
        self.retry = self._load_retry_config()
        self.logging = self._load_logging_config()
        self.monitoring = self._load_monitoring_config()
        self.performance = self._load_performance_config()
        
        # Validate configuration
        self._validate_configuration()
        
        self.logger.info("Configuration manager initialized successfully")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for configuration manager"""
        logger = logging.getLogger("config_manager")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _load_database_config(self, prefix: str) -> DatabaseConfig:
        """Load database configuration from environment variables"""
        try:
            config = DatabaseConfig(
                host=os.getenv(f"{prefix}_HOST", "localhost"),
                port=int(os.getenv(f"{prefix}_PORT", "5432")),
                name=os.getenv(f"{prefix}_NAME", ""),
                user=os.getenv(f"{prefix}_USER", ""),
                password=os.getenv(f"{prefix}_PASSWORD", ""),
                timeout=int(os.getenv("DB_TIMEOUT", "30")),
                pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
                max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
                ssl_enabled=os.getenv("ENABLE_SSL", "false").lower() == "true",
                ssl_cert_path=os.getenv("SSL_CERT_PATH") or None,
                ssl_key_path=os.getenv("SSL_KEY_PATH") or None
            )
            
            self.logger.info(f"Database configuration loaded for {prefix}")
            return config
            
        except Exception as e:
            self.logger.error(f"Failed to load database configuration for {prefix}: {e}")
            raise
    
    def _load_retry_config(self) -> RetryConfig:
        """Load retry configuration from environment variables"""
        try:
            config = RetryConfig(
                max_attempts=int(os.getenv("MAX_RETRY_ATTEMPTS", "3")),
                backoff_factor=float(os.getenv("RETRY_BACKOFF_FACTOR", "2.0")),
                initial_delay=float(os.getenv("INITIAL_RETRY_DELAY", "1.0")),
                max_delay=float(os.getenv("MAX_RETRY_DELAY", "60.0"))
            )
            
            self.logger.info("Retry configuration loaded")
            return config
            
        except Exception as e:
            self.logger.error(f"Failed to load retry configuration: {e}")
            raise
    
    def _load_logging_config(self) -> LoggingConfig:
        """Load logging configuration from environment variables"""
        try:
            config = LoggingConfig(
                level=os.getenv("LOG_LEVEL", "INFO"),
                format_type=os.getenv("LOG_FORMAT", "structured"),
                log_dir=os.getenv("LOG_DIR", "logs"),
                max_bytes=int(os.getenv("LOG_MAX_BYTES", "10485760")),
                backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5")),
                enable_console=os.getenv("ENABLE_CONSOLE_LOG", "true").lower() == "true",
                enable_file=os.getenv("ENABLE_FILE_LOG", "true").lower() == "true"
            )
            
            self.logger.info("Logging configuration loaded")
            return config
            
        except Exception as e:
            self.logger.error(f"Failed to load logging configuration: {e}")
            raise
    
    def _load_monitoring_config(self) -> MonitoringConfig:
        """Load monitoring configuration from environment variables"""
        try:
            config = MonitoringConfig(
                volume_anomaly_threshold=float(os.getenv("VOLUME_ANOMALY_THRESHOLD", "3.0")),
                freshness_threshold_minutes=int(os.getenv("FRESHNESS_THRESHOLD_MINUTES", "30")),
                contract_validation_mode=os.getenv("CONTRACT_VALIDATION_MODE", "strict"),
                sampling_size=int(os.getenv("SAMPLING_SIZE", "100")),
                alert_severity=os.getenv("ALERT_SEVERITY", "CRITICAL"),
                webhook_enabled=os.getenv("ALERT_WEBHOOK_ENABLED", "false").lower() == "true",
                email_enabled=os.getenv("ALERT_EMAIL_ENABLED", "false").lower() == "true"
            )
            
            self.logger.info("Monitoring configuration loaded")
            return config
            
        except Exception as e:
            self.logger.error(f"Failed to load monitoring configuration: {e}")
            raise
    
    def _load_performance_config(self) -> PerformanceConfig:
        """Load performance configuration from environment variables"""
        try:
            config = PerformanceConfig(
                concurrent_workers=int(os.getenv("CONCURRENT_WORKERS", "4")),
                batch_size=int(os.getenv("BATCH_SIZE", "1000")),
                connection_timeout=int(os.getenv("CONNECTION_TIMEOUT", "30"))
            )
            
            self.logger.info("Performance configuration loaded")
            return config
            
        except Exception as e:
            self.logger.error(f"Failed to load performance configuration: {e}")
            raise
    
    def _validate_configuration(self) -> None:
        """Validate all configuration values"""
        validation_errors = []
        
        # Validate database configurations
        if not self.batch_db.name:
            validation_errors.append("BATCH_DB_NAME is required")
        if not self.batch_db.user:
            validation_errors.append("BATCH_DB_USER is required")
        if not self.batch_db.password:
            validation_errors.append("BATCH_DB_PASSWORD is required")
            
        if not self.cdc_db.name:
            validation_errors.append("CDC_DB_NAME is required")
        if not self.cdc_db.user:
            validation_errors.append("CDC_DB_USER is required")
        if not self.cdc_db.password:
            validation_errors.append("CDC_DB_PASSWORD is required")
        
        # Validate retry configuration
        if self.retry.max_attempts < 1:
            validation_errors.append("MAX_RETRY_ATTEMPTS must be >= 1")
        if self.retry.backoff_factor <= 1:
            validation_errors.append("RETRY_BACKOFF_FACTOR must be > 1")
        
        # Validate monitoring configuration
        if self.monitoring.volume_anomaly_threshold <= 0:
            validation_errors.append("VOLUME_ANOMALY_THRESHOLD must be > 0")
        if self.monitoring.freshness_threshold_minutes <= 0:
            validation_errors.append("FRESHNESS_THRESHOLD_MINUTES must be > 0")
        
        # Validate performance configuration
        if self.performance.concurrent_workers < 1:
            validation_errors.append("CONCURRENT_WORKERS must be >= 1")
        
        if validation_errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in validation_errors)
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.logger.info("Configuration validation passed")
    
    def get_database_config_dict(self) -> Dict[str, Any]:
        """Get database configuration in dictionary format for legacy compatibility"""
        return {
            "databases": {
                "batch_analytics_db": {
                    "name": self.batch_db.name,
                    "connection_string": self.batch_db.get_connection_string(),
                    "type": "postgresql",
                    "timeout": self.batch_db.timeout
                },
                "cdc_history_db": {
                    "name": self.cdc_db.name,
                    "connection_string": self.cdc_db.get_connection_string(),
                    "type": "postgresql",
                    "timeout": self.cdc_db.timeout
                }
            },
            "monitoring": {
                "baselines_table": "monitoring.baselines",
                "retention_days": 90
            },
            "logging": {
                "level": self.logging.level,
                "format": self.logging.format_type
            }
        }


# Global configuration instance
_config_manager = None


def get_config() -> ConfigManager:
    """Get global configuration manager instance"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager


def reload_config(env_file: str = ".env") -> ConfigManager:
    """Reload configuration from environment file"""
    global _config_manager
    _config_manager = ConfigManager(env_file)
    return _config_manager
