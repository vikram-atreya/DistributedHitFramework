"""
Orchestrator Agent - Manages worker deployments based on Service Bus messages.

This long-running service listens to Azure Service Bus Queue and creates/scales
Kubernetes Deployments for workers based on received test requests.
"""

import os
import sys
import json
import time
import signal
import logging
from datetime import datetime
from typing import Optional

from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
SERVICE_BUS_CONNECTION_STRING = os.getenv("SERVICE_BUS_CONNECTION_STRING", "")
SERVICE_BUS_QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME", "load-test-requests")
NAMESPACE = os.getenv("K8S_NAMESPACE", "load-testing")
TARGET_URL = os.getenv("TARGET_URL", "http://target-api:8000/hit")
WORKER_IMAGE = os.getenv("WORKER_IMAGE", "worker:latest")
WORKER_CPU_REQUEST = os.getenv("WORKER_CPU_REQUEST", "100m")
WORKER_CPU_LIMIT = os.getenv("WORKER_CPU_LIMIT", "500m")
WORKER_MEMORY_REQUEST = os.getenv("WORKER_MEMORY_REQUEST", "128Mi")
WORKER_MEMORY_LIMIT = os.getenv("WORKER_MEMORY_LIMIT", "256Mi")

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True


class KubernetesOrchestrator:
    """Manages Kubernetes worker jobs."""
    
    def __init__(self):
        """Initialize Kubernetes client."""
        try:
            # Try in-cluster config first (when running in K8s)
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration")
        except config.ConfigException:
            # Fall back to kubeconfig (for local development)
            try:
                config.load_kube_config()
                logger.info("Loaded kubeconfig from default location")
            except config.ConfigException as e:
                logger.error(f"Failed to load Kubernetes configuration: {e}")
                raise
        
        self.apps_v1 = client.AppsV1Api()
        self.batch_v1 = client.BatchV1Api()
        self.core_v1 = client.CoreV1Api()
    
    def create_worker_job(
        self,
        test_id: str,
        workers: int,
        hits_per_sec: int,
        duration_sec: int
    ) -> bool:
        """
        Create a Kubernetes Job for workers.
        
        Args:
            test_id: Unique test identifier
            workers: Number of worker pods (parallelism)
            hits_per_sec: Total hits per second (divided among workers)
            duration_sec: Test duration in seconds
            
        Returns:
            True if job created successfully, False otherwise
        """
        job_name = f"worker-{test_id[:8]}"
        
        # Calculate per-worker hit rate
        hits_per_worker = max(1, hits_per_sec // workers)
        
        logger.info(f"Creating job {job_name} with {workers} workers, "
                    f"{hits_per_worker} hits/sec per worker")
        
        # Define the job
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace=NAMESPACE,
                labels={
                    "app": "load-worker",
                    "test-id": test_id,
                    "managed-by": "orchestrator"
                }
            ),
            spec=client.V1JobSpec(
                parallelism=workers,
                completions=workers,
                backoff_limit=0,  # Don't retry failed pods
                ttl_seconds_after_finished=300,  # Clean up after 5 minutes
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={
                            "app": "load-worker",
                            "test-id": test_id
                        }
                    ),
                    spec=client.V1PodSpec(
                        restart_policy="Never",
                        containers=[
                            client.V1Container(
                                name="worker",
                                image=WORKER_IMAGE,
                                image_pull_policy="IfNotPresent",
                                env=[
                                    client.V1EnvVar(
                                        name="TARGET_URL",
                                        value=TARGET_URL
                                    ),
                                    client.V1EnvVar(
                                        name="HITS_PER_SEC",
                                        value=str(hits_per_worker)
                                    ),
                                    client.V1EnvVar(
                                        name="DURATION_SEC",
                                        value=str(duration_sec)
                                    ),
                                    client.V1EnvVar(
                                        name="TEST_ID",
                                        value=test_id
                                    )
                                ],
                                resources=client.V1ResourceRequirements(
                                    requests={
                                        "cpu": WORKER_CPU_REQUEST,
                                        "memory": WORKER_MEMORY_REQUEST
                                    },
                                    limits={
                                        "cpu": WORKER_CPU_LIMIT,
                                        "memory": WORKER_MEMORY_LIMIT
                                    }
                                )
                            )
                        ]
                    )
                )
            )
        )
        
        try:
            self.batch_v1.create_namespaced_job(
                namespace=NAMESPACE,
                body=job
            )
            logger.info(f"Successfully created job {job_name}")
            return True
        except ApiException as e:
            if e.status == 409:
                logger.warning(f"Job {job_name} already exists")
                return True
            logger.error(f"Failed to create job: {e}")
            return False
    
    def delete_worker_job(self, test_id: str) -> bool:
        """Delete a worker job by test ID."""
        job_name = f"worker-{test_id[:8]}"
        
        try:
            self.batch_v1.delete_namespaced_job(
                name=job_name,
                namespace=NAMESPACE,
                propagation_policy="Background"
            )
            logger.info(f"Successfully deleted job {job_name}")
            return True
        except ApiException as e:
            if e.status == 404:
                logger.warning(f"Job {job_name} not found")
                return True
            logger.error(f"Failed to delete job: {e}")
            return False
    
    def list_worker_jobs(self) -> list:
        """List all worker jobs managed by orchestrator."""
        try:
            jobs = self.batch_v1.list_namespaced_job(
                namespace=NAMESPACE,
                label_selector="managed-by=orchestrator,app=load-worker"
            )
            return [j.metadata.name for j in jobs.items]
        except ApiException as e:
            logger.error(f"Failed to list jobs: {e}")
            return []


class ServiceBusConsumer:
    """Consumes messages from Azure Service Bus."""
    
    def __init__(self, orchestrator: KubernetesOrchestrator):
        """Initialize Service Bus client."""
        self.orchestrator = orchestrator
        
        if not SERVICE_BUS_CONNECTION_STRING:
            logger.warning("SERVICE_BUS_CONNECTION_STRING not set")
            self.client = None
        else:
            self.client = ServiceBusClient.from_connection_string(
                SERVICE_BUS_CONNECTION_STRING
            )
            logger.info("Connected to Azure Service Bus")
    
    def process_message(self, message_body: str) -> bool:
        """
        Process a test request message.
        
        Args:
            message_body: JSON string containing test parameters
            
        Returns:
            True if message processed successfully
        """
        try:
            data = json.loads(message_body)
            
            test_id = data.get("test_id")
            hits_per_sec = data.get("hits_per_sec")
            duration_sec = data.get("duration_sec")
            workers = data.get("workers")
            
            if not all([test_id, hits_per_sec, duration_sec, workers]):
                logger.error(f"Invalid message format: {data}")
                return False
            
            logger.info(f"Processing test {test_id}: {workers} workers, "
                        f"{hits_per_sec} hits/sec for {duration_sec}s")
            
            # Create worker job
            success = self.orchestrator.create_worker_job(
                test_id=test_id,
                workers=workers,
                hits_per_sec=hits_per_sec,
                duration_sec=duration_sec
            )
            
            if success:
                logger.info(f"Test {test_id} orchestration complete")
            else:
                logger.error(f"Test {test_id} orchestration failed")
            
            return success
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
            return False
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def run(self):
        """Main loop to receive and process messages."""
        global shutdown_requested
        
        if not self.client:
            logger.error("Service Bus client not initialized")
            return
        
        logger.info(f"Starting message receiver for queue: {SERVICE_BUS_QUEUE_NAME}")
        
        with self.client.get_queue_receiver(
            queue_name=SERVICE_BUS_QUEUE_NAME,
            receive_mode=ServiceBusReceiveMode.PEEK_LOCK
        ) as receiver:
            while not shutdown_requested:
                try:
                    # Receive messages with a timeout
                    messages = receiver.receive_messages(
                        max_message_count=1,
                        max_wait_time=5
                    )
                    
                    for message in messages:
                        message_body = str(message)
                        logger.info(f"Received message: {message.message_id}")
                        
                        success = self.process_message(message_body)
                        
                        if success:
                            receiver.complete_message(message)
                            logger.info(f"Message {message.message_id} completed")
                        else:
                            # Dead letter the message for investigation
                            receiver.dead_letter_message(
                                message,
                                reason="ProcessingFailed",
                                error_description="Failed to process test request"
                            )
                            logger.warning(f"Message {message.message_id} dead-lettered")
                            
                except Exception as e:
                    logger.error(f"Error receiving messages: {e}")
                    if not shutdown_requested:
                        time.sleep(5)  # Wait before retrying
        
        logger.info("Message receiver stopped")
    
    def close(self):
        """Close the Service Bus client."""
        if self.client:
            self.client.close()
            logger.info("Service Bus connection closed")


def main():
    """Main entry point for the orchestrator."""
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("Orchestrator Agent starting...")
    logger.info(f"Namespace: {NAMESPACE}")
    logger.info(f"Queue: {SERVICE_BUS_QUEUE_NAME}")
    logger.info(f"Worker image: {WORKER_IMAGE}")
    
    try:
        # Initialize Kubernetes orchestrator
        orchestrator = KubernetesOrchestrator()
        
        # List existing worker jobs
        existing = orchestrator.list_worker_jobs()
        if existing:
            logger.info(f"Found existing worker jobs: {existing}")
        
        # Initialize and run Service Bus consumer
        consumer = ServiceBusConsumer(orchestrator)
        consumer.run()
        
        # Cleanup
        consumer.close()
        
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")
        sys.exit(1)
    
    logger.info("Orchestrator Agent stopped")
    sys.exit(0)


if __name__ == "__main__":
    main()
