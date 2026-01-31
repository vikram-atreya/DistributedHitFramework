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
    """Manages Kubernetes worker deployments."""
    
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
        self.core_v1 = client.CoreV1Api()
    
    def create_worker_deployment(
        self,
        test_id: str,
        workers: int,
        hits_per_sec: int,
        duration_sec: int
    ) -> bool:
        """
        Create a Kubernetes Deployment for workers.
        
        Args:
            test_id: Unique test identifier
            workers: Number of worker replicas
            hits_per_sec: Total hits per second (divided among workers)
            duration_sec: Test duration in seconds
            
        Returns:
            True if deployment created successfully, False otherwise
        """
        deployment_name = f"worker-{test_id[:8]}"
        
        # Calculate per-worker hit rate
        hits_per_worker = max(1, hits_per_sec // workers)
        
        logger.info(f"Creating deployment {deployment_name} with {workers} workers, "
                    f"{hits_per_worker} hits/sec per worker")
        
        # Define the deployment
        deployment = client.V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(
                name=deployment_name,
                namespace=NAMESPACE,
                labels={
                    "app": "load-worker",
                    "test-id": test_id,
                    "managed-by": "orchestrator"
                }
            ),
            spec=client.V1DeploymentSpec(
                replicas=workers,
                selector=client.V1LabelSelector(
                    match_labels={
                        "app": "load-worker",
                        "test-id": test_id
                    }
                ),
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
                                image_pull_policy="Always",
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
            self.apps_v1.create_namespaced_deployment(
                namespace=NAMESPACE,
                body=deployment
            )
            logger.info(f"Successfully created deployment {deployment_name}")
            return True
        except ApiException as e:
            if e.status == 409:
                logger.warning(f"Deployment {deployment_name} already exists, updating...")
                return self.update_worker_deployment(
                    deployment_name, workers, hits_per_worker, duration_sec, test_id
                )
            logger.error(f"Failed to create deployment: {e}")
            return False
    
    def update_worker_deployment(
        self,
        deployment_name: str,
        workers: int,
        hits_per_worker: int,
        duration_sec: int,
        test_id: str
    ) -> bool:
        """Update an existing worker deployment."""
        try:
            # Get existing deployment
            deployment = self.apps_v1.read_namespaced_deployment(
                name=deployment_name,
                namespace=NAMESPACE
            )
            
            # Update replicas and environment
            deployment.spec.replicas = workers
            for container in deployment.spec.template.spec.containers:
                if container.name == "worker":
                    for env in container.env:
                        if env.name == "HITS_PER_SEC":
                            env.value = str(hits_per_worker)
                        elif env.name == "DURATION_SEC":
                            env.value = str(duration_sec)
            
            self.apps_v1.patch_namespaced_deployment(
                name=deployment_name,
                namespace=NAMESPACE,
                body=deployment
            )
            logger.info(f"Successfully updated deployment {deployment_name}")
            return True
        except ApiException as e:
            logger.error(f"Failed to update deployment: {e}")
            return False
    
    def delete_worker_deployment(self, test_id: str) -> bool:
        """Delete a worker deployment by test ID."""
        deployment_name = f"worker-{test_id[:8]}"
        
        try:
            self.apps_v1.delete_namespaced_deployment(
                name=deployment_name,
                namespace=NAMESPACE
            )
            logger.info(f"Successfully deleted deployment {deployment_name}")
            return True
        except ApiException as e:
            if e.status == 404:
                logger.warning(f"Deployment {deployment_name} not found")
                return True
            logger.error(f"Failed to delete deployment: {e}")
            return False
    
    def list_worker_deployments(self) -> list:
        """List all worker deployments managed by orchestrator."""
        try:
            deployments = self.apps_v1.list_namespaced_deployment(
                namespace=NAMESPACE,
                label_selector="managed-by=orchestrator,app=load-worker"
            )
            return [d.metadata.name for d in deployments.items]
        except ApiException as e:
            logger.error(f"Failed to list deployments: {e}")
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
            
            # Create worker deployment
            success = self.orchestrator.create_worker_deployment(
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
        
        # List existing worker deployments
        existing = orchestrator.list_worker_deployments()
        if existing:
            logger.info(f"Found existing worker deployments: {existing}")
        
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
