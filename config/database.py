"""
Database configuration and connection management for Transportation Analytics
"""
import os
from typing import Optional
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import redis
from urllib.parse import quote_plus

# Database configuration
class DatabaseConfig:
    """Database configuration settings"""
    
    def __init__(self):
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = os.getenv('POSTGRES_PORT', '5432')
        self.database = os.getenv('POSTGRES_DB', 'transportation_analytics')
        self.username = os.getenv('POSTGRES_USER', 'transport_user')
        self.password = os.getenv('POSTGRES_PASSWORD', 'transport_pass')
        
        # Redis configuration
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_db = int(os.getenv('REDIS_DB', '0'))
    
    @property
    def database_url(self) -> str:
        """Construct database URL for SQLAlchemy"""
        password = quote_plus(self.password)
        return f"postgresql://{self.username}:{password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def async_database_url(self) -> str:
        """Construct async database URL"""
        password = quote_plus(self.password)
        return f"postgresql+asyncpg://{self.username}:{password}@{self.host}:{self.port}/{self.database}"

# Global configuration instance
config = DatabaseConfig()

# SQLAlchemy setup
engine = create_engine(
    config.database_url,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=os.getenv('SQL_ECHO', 'False').lower() == 'true'
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for ORM models
Base = declarative_base()

# Metadata for table reflection
metadata = MetaData()

# Redis connection
redis_client = redis.Redis(
    host=config.redis_host,
    port=config.redis_port,
    db=config.redis_db,
    decode_responses=True
)

def get_db_connection():
    """Get database connection"""
    return engine.connect()

def get_db_session():
    """Get database session"""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

def test_connection() -> bool:
    """Test database connection"""
    try:
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False

def test_redis_connection() -> bool:
    """Test Redis connection"""
    try:
        redis_client.ping()
        return True
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return False 