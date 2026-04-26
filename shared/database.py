import os
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Generator

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from shared.models import Base

DATABASE_URL       = os.getenv("DATABASE_URL",       "postgresql://distrq:distrq@postgres:5432/distrq")
ASYNC_DATABASE_URL = os.getenv("ASYNC_DATABASE_URL", "postgresql+asyncpg://distrq:distrq@postgres:5432/distrq")

sync_engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20, pool_pre_ping=True, pool_recycle=300, echo=False)
SyncSession = sessionmaker(bind=sync_engine, autocommit=False, autoflush=False)

async_engine = create_async_engine(ASYNC_DATABASE_URL, pool_size=20, max_overflow=40, pool_pre_ping=True, pool_recycle=300, echo=False)
AsyncSessionLocal = async_sessionmaker(bind=async_engine, class_=AsyncSession, autocommit=False, autoflush=False, expire_on_commit=False)

def create_tables():
    Base.metadata.create_all(bind=sync_engine, checkfirst=True)

@contextmanager
def get_sync_session():
    session = SyncSession()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

@asynccontextmanager
async def get_async_session():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise

async def ping_db():
    try:
        async with async_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
