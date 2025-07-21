"""
Azure SQL Database configuration and connection management for Transportation Analytics
"""
import os
from typing import Optional
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import redis
from urllib.parse import quote_plus
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Azure SQL Database configuration
class AzureDatabaseConfig:
    """Azure SQL Database configuration settings"""
    
    def __init__(self):
        # Azure SQL Database credentials - use environment variables only
        self.server = os.getenv('AZURE_SQL_SERVER')
        self.database = os.getenv('AZURE_SQL_DATABASE')
        self.username = os.getenv('AZURE_SQL_USERNAME')
        self.password = os.getenv('AZURE_SQL_PASSWORD')
        self.port = os.getenv('AZURE_SQL_PORT', '1433')
        
        # Connection settings
        self.driver = os.getenv('AZURE_SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
        self.encrypt = os.getenv('AZURE_SQL_ENCRYPT', 'yes')
        self.trust_server_certificate = os.getenv('AZURE_SQL_TRUST_SERVER_CERTIFICATE', 'yes')
        
        # Redis configuration (keeping for caching)
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_db = int(os.getenv('REDIS_DB', '0'))
    
    @property
    def database_url(self) -> str:
        """Construct Azure SQL Database URL for SQLAlchemy"""
        password = quote_plus(self.password)
        return (f"mssql+pyodbc://{self.username}:{password}@{self.server}:{self.port}/"
                f"{self.database}?driver={quote_plus(self.driver)}&encrypt={self.encrypt}"
                f"&trustServerCertificate={self.trust_server_certificate}")
    
    @property
    def connection_string(self) -> str:
        """Construct ODBC connection string"""
        password = quote_plus(self.password)
        return (f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"Encrypt={self.encrypt};"
                f"TrustServerCertificate={self.trust_server_certificate};")

# Global configuration instance
azure_config = AzureDatabaseConfig()

# SQLAlchemy setup for Azure SQL
azure_engine = create_engine(
    azure_config.database_url,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=os.getenv('SQL_ECHO', 'False').lower() == 'true'
)

# Session factory for Azure SQL
AzureSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=azure_engine)

# Base class for ORM models
AzureBase = declarative_base()

# Metadata for table reflection
azure_metadata = MetaData()

# Redis connection (for caching)
redis_client = redis.Redis(
    host=azure_config.redis_host,
    port=azure_config.redis_port,
    db=azure_config.redis_db,
    decode_responses=True
)

def get_azure_db_connection():
    """Get Azure SQL Database connection"""
    return azure_engine.connect()

def get_azure_db_session():
    """Get Azure SQL Database session"""
    session = AzureSessionLocal()
    try:
        yield session
    finally:
        session.close()

def test_azure_connection() -> bool:
    """Test Azure SQL Database connection"""
    try:
        with azure_engine.connect() as conn:
            result = conn.execute(text("SELECT @@VERSION as version"))
            version = result.fetchone()[0]
            logger.info(f"✅ Azure SQL Database connection successful")
            logger.info(f"Server version: {version}")
        return True
    except Exception as e:
        logger.error(f"❌ Azure SQL Database connection failed: {e}")
        return False

def test_redis_connection() -> bool:
    """Test Redis connection"""
    try:
        redis_client.ping()
        logger.info("✅ Redis connection successful")
        return True
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        return False

def get_azure_tables():
    """Get list of tables in Azure SQL Database"""
    try:
        with azure_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """))
            tables = result.fetchall()
            return [(row[0], row[1]) for row in tables]
    except Exception as e:
        logger.error(f"❌ Error getting tables: {e}")
        return []

def get_table_info(schema: str, table_name: str):
    """Get table structure information"""
    try:
        with azure_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = :schema 
                AND TABLE_NAME = :table_name
                ORDER BY ORDINAL_POSITION
            """), {"schema": schema, "table_name": table_name})
            columns = result.fetchall()
            return columns
    except Exception as e:
        logger.error(f"❌ Error getting table info for {schema}.{table_name}: {e}")
        return []

def get_table_row_count(schema: str, table_name: str) -> int:
    """Get row count for a table"""
    try:
        with azure_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM [{schema}].[{table_name}]"))
            count = result.fetchone()[0]
            return count
    except Exception as e:
        logger.error(f"❌ Error getting row count for {schema}.{table_name}: {e}")
        return 0 