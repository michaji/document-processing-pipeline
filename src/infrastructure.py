"""
infrastructure.py
Provides Redis and PostgreSQL connections and repository patterns for the document processing pipeline.
"""
import os
import json
import logging
from typing import Dict, Any, Optional

import redis
from sqlalchemy import create_engine, Column, String, JSON, DateTime, Integer
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Redis Setup ───────────────────────────────────────────────────────────────

def get_redis_client() -> Optional[redis.Redis]:
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))
    password = os.getenv("REDIS_PASSWORD", None)

    try:
        client = redis.Redis(
            host=host, port=port, db=db, password=password, decode_responses=True
        )
        client.ping()
        logger.info(f"Connected to Redis at {host}:{port}/{db}")
        return client
    except redis.ConnectionError as e:
        logger.warning(f"Failed to connect to Redis at {host}:{port}: {e}. Falling back to in-memory.")
        return None

redis_client = get_redis_client()

# ── PostgreSQL Setup ──────────────────────────────────────────────────────────

Base = declarative_base()

class DocumentMetadata(Base):
    __tablename__ = "document_metadata"
    
    document_id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False)
    document_type = Column(String, nullable=False)
    status = Column(String, nullable=False)
    workflow_id = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    extracted_data = Column(JSON, nullable=True)
    audit_trail = Column(JSON, nullable=True)
    needs_review = Column(Integer, default=0) # boolean via integer

class ReviewItem(Base):
    __tablename__ = "review_queue"
    
    id = Column(String, primary_key=True)
    document_id = Column(String, nullable=False)
    extracted_data = Column(JSON, nullable=False)
    status = Column(String, nullable=False) # PENDING, CLAIMED, COMPLETED, REJECTED
    assigned_to = Column(String, nullable=True)
    priority = Column(Integer, nullable=False, default=50)
    sla_deadline = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

def get_pg_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5431")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    db = os.getenv("POSTGRES_DB", "postgres")
    
    if not password:
        logger.warning("No Postgres password provided; connection might fail if required.")
        
    dsn = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    try:
        engine = create_engine(dsn, pool_size=int(os.getenv("POSTGRES_POOL_SIZE", "10")))
        # Quick test connection using a block
        with engine.connect() as conn:
            pass
        logger.info(f"Connected to PostgreSQL at {host}:{port}/{db}")
        return engine
    except Exception as e:
        logger.warning(f"Failed to connect to PostgreSQL: {e}. DB persistence will be disabled.")
        return None

pg_engine = get_pg_engine()
SessionLocal = sessionmaker(bind=pg_engine) if pg_engine else None

def init_db():
    if pg_engine:
        Base.metadata.create_all(bind=pg_engine)
        logger.info("PostgreSQL tables initialized.")
