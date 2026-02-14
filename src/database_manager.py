"""
Thread-Safe Database Connection Manager with Exponential Backoff

Production-ready database connection management with:
- Thread safety using threading.Lock
- Exponential backoff retry logic
- Connection pooling
- Comprehensive error handling
"""

import time
import threading
import logging
import psycopg2
import psycopg2.pool
from psycopg2 import sql, OperationalError
from typing import Dict, List, Tuple, Optional, Any
from contextlib import contextmanager
from queue import Queue, Empty
import random

from config_manager import get_config


class DatabaseConnectionManager:
    """
    Thread-safe database connection manager with exponential backoff retry logic
    
    Features:
    - Thread-safe connection pooling
    - Exponential backoff retry with jitter
    - Connection health monitoring
    - Automatic connection recovery
    """
    
    def __init__(self, db_config_name: str = "batch_db"):
        """
        Initialize database connection manager
        
        Args:
            db_config_name: Name of database config ('batch_db' or 'cdc_db')
        """
        self.config = get_config()
        self.db_config_name = db_config_name
        self.db_config = getattr(self.config, db_config_name)
        self.retry_config = self.config.retry
        
        # Thread safety
        self._lock = threading.RLock()
        self._connection_pool = None
        self._pool_lock = threading.Lock()
        
        # Connection health tracking
        self._last_health_check = 0
        self._health_check_interval = 60  # seconds
        self._failed_connections = 0
        self._max_failed_connections = 5
        
        # Setup logger
        self.logger = self._setup_logger()
        
        # Initialize connection pool
        self._initialize_connection_pool()
        
        self.logger.info(f"Database connection manager initialized for {db_config_name}")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logger with rotation"""
        logger = logging.getLogger(f"db_manager_{self.db_config_name}")
        logger.setLevel(getattr(self.config.logging, 'level', 'INFO'))
        
        if not logger.handlers:
            # Console handler
            if self.config.logging.enable_console:
                console_handler = logging.StreamHandler()
                console_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
                )
                console_handler.setFormatter(console_formatter)
                logger.addHandler(console_handler)
            
            # File handler with rotation
            if self.config.logging.enable_file:
                from logging.handlers import RotatingFileHandler
                
                log_dir = self.config.logging.log_dir
                import os
                os.makedirs(log_dir, exist_ok=True)
                
                file_handler = RotatingFileHandler(
                    filename=os.path.join(log_dir, f"{self.db_config_name}.log"),
                    maxBytes=self.config.logging.max_bytes,
                    backupCount=self.config.logging.backup_count
                )
                file_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
                )
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)
        
        return logger
    
    def _initialize_connection_pool(self) -> None:
        """Initialize thread-safe connection pool"""
        with self._pool_lock:
            try:
                connection_string = self.db_config.get_connection_string()
                
                # Create connection pool
                self._connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=self.db_config.pool_size,
                    host=self.db_config.host,
                    port=self.db_config.port,
                    database=self.db_config.name,
                    user=self.db_config.user,
                    password=self.db_config.password,
                    connect_timeout=self.db_config.timeout,
                    sslmode='require' if self.db_config.ssl_enabled else 'disable'
                )
                
                self.logger.info(f"Connection pool initialized for {self.db_config_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to initialize connection pool: {e}")
                raise
    
    def _exponential_backoff_retry(self, operation, *args, **kwargs):
        """
        Execute operation with exponential backoff retry logic
        
        Args:
            operation: Function to execute
            *args, **kwargs: Arguments to pass to operation
            
        Returns:
            Result of operation
            
        Raises:
            Last exception if all retries fail
        """
        last_exception = None
        
        for attempt in range(self.retry_config.max_attempts):
            try:
                return operation(*args, **kwargs)
                
            except (OperationalError, psycopg2.Error) as e:
                last_exception = e
                
                if attempt < self.retry_config.max_attempts - 1:
                    # Calculate delay with exponential backoff and jitter
                    delay = min(
                        self.retry_config.initial_delay * (self.retry_config.backoff_factor ** attempt),
                        self.retry_config.max_delay
                    )
                    
                    # Add jitter to prevent thundering herd
                    jitter = random.uniform(0, delay * 0.1)
                    total_delay = delay + jitter
                    
                    self.logger.warning(
                        f"Database operation failed (attempt {attempt + 1}/{self.retry_config.max_attempts}), "
                        f"retrying in {total_delay:.2f}s: {str(e)}"
                    )
                    
                    time.sleep(total_delay)
                else:
                    self.logger.error(
                        f"Database operation failed after {self.retry_config.max_attempts} attempts: {str(e)}"
                    )
                    
            except Exception as e:
                # Non-database exceptions don't trigger retry
                self.logger.error(f"Unexpected error during database operation: {str(e)}")
                raise
        
        if last_exception:
            raise last_exception
    
    @contextmanager
    def get_connection(self):
        """
        Get a database connection from the pool (thread-safe)
        
        Yields:
            Database connection object
        """
        connection = None
        try:
            # Get connection with retry logic
            def _get_connection():
                return self._connection_pool.getconn()
            
            connection = self._exponential_backoff_retry(_get_connection)
            
            # Set connection parameters
            connection.autocommit = False
            
            yield connection
            
        except Exception as e:
            self.logger.error(f"Error getting database connection: {str(e)}")
            raise
        finally:
            if connection:
                try:
                    # Return connection to pool
                    self._connection_pool.putconn(connection)
                except Exception as e:
                    self.logger.error(f"Error returning connection to pool: {str(e)}")
    
    def execute_query(self, query: str, params: Optional[Tuple] = None, 
                     fetch: bool = True) -> List[Tuple]:
        """
        Execute SQL query with thread safety and retry logic
        
        Args:
            query: SQL query to execute
            params: Query parameters
            fetch: Whether to fetch results
            
        Returns:
            Query results if fetch=True, otherwise None
        """
        def _execute_query():
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(query, params)
                    
                    if fetch:
                        results = cursor.fetchall()
                        self.logger.debug(f"Query executed successfully, returned {len(results)} rows")
                        return results
                    else:
                        conn.commit()
                        self.logger.debug("Query executed successfully (no fetch)")
                        return []
                        
                finally:
                    cursor.close()
        
        return self._exponential_backoff_retry(_execute_query)
    
    def execute_batch(self, query: str, params_list: List[Tuple]) -> None:
        """
        Execute batch query with thread safety and retry logic
        
        Args:
            query: SQL query to execute
            params_list: List of parameter tuples
        """
        def _execute_batch():
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.executemany(query, params_list)
                    conn.commit()
                    self.logger.info(f"Batch query executed successfully, {len(params_list)} rows affected")
                    
                finally:
                    cursor.close()
        
        self._exponential_backoff_retry(_execute_batch)
    
    def check_connection_health(self) -> bool:
        """
        Check database connection health
        
        Returns:
            True if connection is healthy, False otherwise
        """
        current_time = time.time()
        
        # Skip health check if recently checked
        if current_time - self._last_health_check < self._health_check_interval:
            return True
        
        try:
            # Simple health check query
            self.execute_query("SELECT 1")
            self._failed_connections = 0
            self._last_health_check = current_time
            return True
            
        except Exception as e:
            self._failed_connections += 1
            self._last_health_check = current_time
            
            self.logger.warning(f"Connection health check failed: {str(e)}")
            
            # Reinitialize pool if too many failures
            if self._failed_connections >= self._max_failed_connections:
                self.logger.error("Too many connection failures, reinitializing connection pool")
                self._reinitialize_connection_pool()
            
            return False
    
    def _reinitialize_connection_pool(self) -> None:
        """Reinitialize connection pool after failures"""
        with self._pool_lock:
            try:
                if self._connection_pool:
                    self._connection_pool.closeall()
                
                self._initialize_connection_pool()
                self._failed_connections = 0
                
                self.logger.info("Connection pool reinitialized successfully")
                
            except Exception as e:
                self.logger.error(f"Failed to reinitialize connection pool: {str(e)}")
                raise
    
    def get_pool_status(self) -> Dict[str, Any]:
        """Get connection pool status information"""
        with self._pool_lock:
            if self._connection_pool:
                return {
                    "min_connections": self._connection_pool.minconn,
                    "max_connections": self._connection_pool.maxconn,
                    "used_connections": self._connection_pool.usedconn,
                    "failed_connections": self._failed_connections,
                    "last_health_check": self._last_health_check
                }
            else:
                return {"status": "No connection pool available"}
    
    def close(self) -> None:
        """Close connection pool and cleanup resources"""
        with self._pool_lock:
            if self._connection_pool:
                try:
                    self._connection_pool.closeall()
                    self.logger.info(f"Connection pool closed for {self.db_config_name}")
                except Exception as e:
                    self.logger.error(f"Error closing connection pool: {str(e)}")
                finally:
                    self._connection_pool = None


class ThreadSafeDatabaseManager:
    """
    Thread-safe database manager for multiple database connections
    
    Manages multiple database connection managers with thread safety
    and provides a unified interface for database operations.
    """
    
    def __init__(self):
        """Initialize thread-safe database manager"""
        self.logger = self._setup_logger()
        
        # Initialize connection managers
        self.batch_db = DatabaseConnectionManager("batch_db")
        self.cdc_db = DatabaseConnectionManager("cdc_db")
        
        self.logger.info("Thread-safe database manager initialized")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for database manager"""
        logger = logging.getLogger("thread_safe_db_manager")
        logger.setLevel(getattr(get_config().logging, 'level', 'INFO'))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def get_connection_manager(self, db_name: str) -> DatabaseConnectionManager:
        """
        Get connection manager for specific database
        
        Args:
            db_name: Database name ('batch' or 'cdc')
            
        Returns:
            DatabaseConnectionManager instance
        """
        if db_name.lower() == 'batch':
            return self.batch_db
        elif db_name.lower() == 'cdc':
            return self.cdc_db
        else:
            raise ValueError(f"Unknown database name: {db_name}")
    
    def health_check(self) -> Dict[str, bool]:
        """
        Perform health check on all database connections
        
        Returns:
            Dictionary with health status for each database
        """
        return {
            "batch_db": self.batch_db.check_connection_health(),
            "cdc_db": self.cdc_db.check_connection_health()
        }
    
    def get_status(self) -> Dict[str, Any]:
        """Get status of all database connections"""
        return {
            "batch_db": self.batch_db.get_pool_status(),
            "cdc_db": self.cdc_db.get_pool_status(),
            "health": self.health_check()
        }
    
    def close_all(self) -> None:
        """Close all database connections"""
        try:
            self.batch_db.close()
            self.cdc_db.close()
            self.logger.info("All database connections closed")
        except Exception as e:
            self.logger.error(f"Error closing database connections: {str(e)}")


# Global database manager instance
_db_manager = None


def get_database_manager() -> ThreadSafeDatabaseManager:
    """Get global thread-safe database manager instance"""
    global _db_manager
    if _db_manager is None:
        _db_manager = ThreadSafeDatabaseManager()
    return _db_manager
