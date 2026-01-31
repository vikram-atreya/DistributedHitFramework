"""
Gateway API - Accepts load test requests and publishes to Azure Service Bus.

This service exposes a POST /tests endpoint that validates test parameters,
generates a test ID, and publishes a message to Azure Service Bus Queue.
"""

import os
import json
import uuid
import logging
from datetime import datetime
from contextlib import asynccontextmanager

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Azure Service Bus configuration
SERVICE_BUS_CONNECTION_STRING = os.getenv("SERVICE_BUS_CONNECTION_STRING", "")
SERVICE_BUS_QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME", "load-test-requests")

# Service Bus client
servicebus_client: ServiceBusClient = None


class TestRequest(BaseModel):
    """Request model for creating a load test."""
    hits_per_sec: int = Field(..., gt=0, le=10000, description="Target hits per second")
    duration_sec: int = Field(..., gt=0, le=3600, description="Test duration in seconds")
    workers: int = Field(..., gt=0, le=100, description="Number of worker pods")
    
    @field_validator('hits_per_sec')
    @classmethod
    def validate_hits_per_sec(cls, v):
        if v <= 0:
            raise ValueError('hits_per_sec must be positive')
        if v > 10000:
            raise ValueError('hits_per_sec cannot exceed 10000')
        return v
    
    @field_validator('duration_sec')
    @classmethod
    def validate_duration(cls, v):
        if v <= 0:
            raise ValueError('duration_sec must be positive')
        if v > 3600:
            raise ValueError('duration_sec cannot exceed 3600 (1 hour)')
        return v
    
    @field_validator('workers')
    @classmethod
    def validate_workers(cls, v):
        if v <= 0:
            raise ValueError('workers must be positive')
        if v > 100:
            raise ValueError('workers cannot exceed 100')
        return v


class TestResponse(BaseModel):
    """Response model for test creation."""
    test_id: str
    message: str = "Test request submitted"
    parameters: dict


class TestMessage(BaseModel):
    """Message model for Service Bus queue."""
    test_id: str
    hits_per_sec: int
    duration_sec: int
    workers: int
    submitted_at: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - setup and teardown Service Bus connection."""
    global servicebus_client
    
    if not SERVICE_BUS_CONNECTION_STRING:
        logger.warning("SERVICE_BUS_CONNECTION_STRING not set - running in mock mode")
    else:
        logger.info("Connecting to Azure Service Bus...")
        try:
            servicebus_client = ServiceBusClient.from_connection_string(
                SERVICE_BUS_CONNECTION_STRING
            )
            logger.info("Successfully connected to Azure Service Bus")
        except Exception as e:
            logger.error(f"Failed to connect to Service Bus: {e}")
            raise
    
    yield
    
    # Cleanup
    if servicebus_client:
        servicebus_client.close()
        logger.info("Service Bus connection closed")


app = FastAPI(
    title="Gateway API",
    description="Gateway for submitting distributed load test requests",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Health check endpoint for Kubernetes probes."""
    return {
        "status": "healthy",
        "service_bus_configured": bool(SERVICE_BUS_CONNECTION_STRING)
    }


@app.post("/tests", response_model=TestResponse)
async def create_test(request: TestRequest):
    """
    Create a new load test.
    
    Validates the request, generates a test ID, and publishes a message
    to Azure Service Bus for the orchestrator to process.
    
    Args:
        request: Load test parameters
        
    Returns:
        TestResponse with the generated test_id
    """
    # Generate unique test ID
    test_id = str(uuid.uuid4())
    
    # Create message
    message = TestMessage(
        test_id=test_id,
        hits_per_sec=request.hits_per_sec,
        duration_sec=request.duration_sec,
        workers=request.workers,
        submitted_at=datetime.utcnow().isoformat()
    )
    
    logger.info(f"Creating test {test_id}: {request.workers} workers, "
                f"{request.hits_per_sec} hits/sec for {request.duration_sec}s")
    
    # Publish to Service Bus
    if servicebus_client:
        try:
            with servicebus_client.get_queue_sender(SERVICE_BUS_QUEUE_NAME) as sender:
                sb_message = ServiceBusMessage(
                    body=json.dumps(message.model_dump()),
                    message_id=test_id,
                    subject="load-test-request"
                )
                sender.send_messages(sb_message)
                logger.info(f"Test {test_id} published to Service Bus queue")
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            raise HTTPException(
                status_code=503,
                detail=f"Failed to submit test request: {str(e)}"
            )
    else:
        # Mock mode for local development
        logger.warning(f"Mock mode: Test {test_id} would be published to queue")
    
    return TestResponse(
        test_id=test_id,
        message="Test request submitted successfully",
        parameters={
            "hits_per_sec": request.hits_per_sec,
            "duration_sec": request.duration_sec,
            "workers": request.workers
        }
    )


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "Gateway API",
        "version": "1.0.0",
        "endpoints": {
            "POST /tests": "Submit a new load test request",
            "GET /health": "Health check endpoint"
        }
    }
