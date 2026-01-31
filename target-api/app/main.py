"""
Target API - A simple HTTP API that increments and returns a global hit counter.

This service exposes a POST /hit endpoint that atomically increments a Redis key
and returns the current total hit count.
"""

import os
import logging
from contextlib import asynccontextmanager

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Redis connection
redis_client: redis.Redis = None

# Redis configuration from environment
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
REDIS_SSL = os.getenv("REDIS_SSL", "false").lower() == "true"
REDIS_KEY = os.getenv("REDIS_KEY", "total_hits")


class HitResponse(BaseModel):
    """Response model for the hit endpoint."""
    total_hits: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - setup and teardown Redis connection."""
    global redis_client
    
    logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
    
    try:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD if REDIS_PASSWORD else None,
            ssl=REDIS_SSL,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5
        )
        # Test connection
        redis_client.ping()
        logger.info("Successfully connected to Redis")
        
        # Initialize counter if it doesn't exist
        if not redis_client.exists(REDIS_KEY):
            redis_client.set(REDIS_KEY, 0)
            logger.info(f"Initialized {REDIS_KEY} to 0")
        
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise
    
    yield
    
    # Cleanup
    if redis_client:
        redis_client.close()
        logger.info("Redis connection closed")


app = FastAPI(
    title="Target API",
    description="Simple HTTP API for distributed load testing - increments hit counter",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Health check endpoint for Kubernetes probes."""
    try:
        redis_client.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Unhealthy: {str(e)}")


@app.post("/hit", response_model=HitResponse)
async def hit():
    """
    Increment the global hit counter atomically and return the new value.
    
    Returns:
        HitResponse: JSON object with the current total hit count
    """
    try:
        # Atomically increment the counter
        total_hits = redis_client.incr(REDIS_KEY)
        logger.debug(f"Hit recorded, total: {total_hits}")
        return HitResponse(total_hits=total_hits)
    except redis.ConnectionError as e:
        logger.error(f"Redis connection error: {e}")
        raise HTTPException(status_code=503, detail="Redis connection error")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """Get current hit statistics without incrementing."""
    try:
        total_hits = redis_client.get(REDIS_KEY)
        return {"total_hits": int(total_hits) if total_hits else 0}
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reset")
async def reset_counter():
    """Reset the hit counter to zero (for testing purposes)."""
    try:
        redis_client.set(REDIS_KEY, 0)
        logger.info("Hit counter reset to 0")
        return {"message": "Counter reset", "total_hits": 0}
    except Exception as e:
        logger.error(f"Error resetting counter: {e}")
        raise HTTPException(status_code=500, detail=str(e))
