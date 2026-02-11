"""
Utility functions for pipeline.

This module contains shared utilities:
- Logging
- Database helpers
- Common functions
"""

import logging
import os
from datetime import datetime
from pathlib import Path


def setup_logging(
    log_dir: str = "data/logs",
    log_level: str = "INFO"
) -> logging.Logger:
    """
    Setup structured logging.
    
    Args:
        log_dir: Directory for log files
        log_level: Logging level
        
    Returns:
        Configured logger
    """
    # Create log directory if not exists
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Get log level
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Create logger
    logger = logging.getLogger("fraga_pipeline")
    logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler
    log_file = log_path / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


def get_db_connection_from_env():
    """
    Get database connection from environment variables.
    
    Returns:
        psycopg2 connection
        
    Raises:
        ValueError: If required environment variables are missing
        psycopg2.OperationalError: If connection fails
    """
    import psycopg2
    
    # Read environment variables
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    
    # Validate required variables
    if not all([db_host, db_name, db_user, db_password]):
        missing = [var for var, val in [
            ("DB_HOST", db_host),
            ("DB_NAME", db_name),
            ("DB_USER", db_user),
            ("DB_PASSWORD", db_password)
        ] if not val]
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    # Create connection
    conn = psycopg2.connect(
        host=db_host,
        port=int(db_port),
        database=db_name,
        user=db_user,
        password=db_password
    )
    
    return conn


def execute_sql_file(
    sql_file_path: str,
    conn
) -> None:
    """
    Execute SQL file.
    
    Args:
        sql_file_path: Path to SQL file
        conn: Database connection
        
    Raises:
        FileNotFoundError: If SQL file doesn't exist
        psycopg2.Error: If SQL execution fails
    """
    sql_path = Path(sql_file_path)
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
    
    # Read SQL file
    with open(sql_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Execute SQL (split by semicolon for multiple statements)
    cursor = conn.cursor()
    try:
        # Execute all statements
        cursor.execute(sql_content)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
