"""
Worker - Stateless load generator that hits the Target API.

This worker is designed to run as a Kubernetes pod and generate HTTP requests
at a configured rate for a configured duration.
"""

import os
import sys
import time
import logging
import signal
from datetime import datetime

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
TARGET_URL = os.getenv("TARGET_URL", "http://target-api:8000/hit")
HITS_PER_SEC = int(os.getenv("HITS_PER_SEC", "10"))
DURATION_SEC = int(os.getenv("DURATION_SEC", "60"))
LOG_INTERVAL_SEC = int(os.getenv("LOG_INTERVAL_SEC", "10"))

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True


def validate_config():
    """Validate configuration parameters."""
    if HITS_PER_SEC <= 0:
        logger.error(f"Invalid HITS_PER_SEC: {HITS_PER_SEC}, must be positive")
        sys.exit(1)
    
    if DURATION_SEC <= 0:
        logger.error(f"Invalid DURATION_SEC: {DURATION_SEC}, must be positive")
        sys.exit(1)
    
    if not TARGET_URL:
        logger.error("TARGET_URL is not set")
        sys.exit(1)
    
    logger.info(f"Configuration validated:")
    logger.info(f"  TARGET_URL: {TARGET_URL}")
    logger.info(f"  HITS_PER_SEC: {HITS_PER_SEC}")
    logger.info(f"  DURATION_SEC: {DURATION_SEC}")


def send_hit(session: requests.Session) -> tuple[bool, int]:
    """
    Send a single hit request to the target API.
    
    Returns:
        Tuple of (success: bool, status_code: int)
    """
    try:
        response = session.post(TARGET_URL, timeout=5)
        return response.status_code == 200, response.status_code
    except requests.exceptions.Timeout:
        return False, 408
    except requests.exceptions.ConnectionError:
        return False, 503
    except Exception as e:
        logger.debug(f"Request error: {e}")
        return False, 500


def run_load_test():
    """
    Execute the load test at the configured rate for the configured duration.
    
    Uses simple timing logic to maintain the target request rate.
    """
    validate_config()
    
    # Calculate timing
    interval = 1.0 / HITS_PER_SEC  # Time between requests
    total_requests = HITS_PER_SEC * DURATION_SEC
    
    logger.info(f"Starting load test:")
    logger.info(f"  Total planned requests: {total_requests}")
    logger.info(f"  Request interval: {interval:.4f}s")
    
    # Statistics
    successful_hits = 0
    failed_hits = 0
    start_time = time.time()
    last_log_time = start_time
    
    # Create session for connection pooling
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    
    logger.info(f"Load test started at {datetime.now().isoformat()}")
    
    request_count = 0
    next_request_time = start_time
    
    while not shutdown_requested:
        current_time = time.time()
        elapsed = current_time - start_time
        
        # Check if duration is complete
        if elapsed >= DURATION_SEC:
            logger.info("Duration complete, finishing up...")
            break
        
        # Wait until it's time for the next request
        if current_time < next_request_time:
            sleep_time = next_request_time - current_time
            if sleep_time > 0:
                time.sleep(sleep_time)
            continue
        
        # Send the request
        success, status_code = send_hit(session)
        request_count += 1
        
        if success:
            successful_hits += 1
        else:
            failed_hits += 1
            logger.debug(f"Request failed with status: {status_code}")
        
        # Schedule next request
        next_request_time += interval
        
        # Catch up if we're falling behind
        if next_request_time < current_time:
            next_request_time = current_time + interval
        
        # Log progress periodically
        if current_time - last_log_time >= LOG_INTERVAL_SEC:
            actual_rate = request_count / elapsed if elapsed > 0 else 0
            logger.info(
                f"Progress: {elapsed:.1f}s elapsed, "
                f"{request_count} requests sent, "
                f"{successful_hits} successful, "
                f"{failed_hits} failed, "
                f"actual rate: {actual_rate:.1f} req/s"
            )
            last_log_time = current_time
    
    # Final statistics
    session.close()
    end_time = time.time()
    total_duration = end_time - start_time
    actual_rate = request_count / total_duration if total_duration > 0 else 0
    success_rate = (successful_hits / request_count * 100) if request_count > 0 else 0
    
    logger.info("=" * 60)
    logger.info("Load test completed!")
    logger.info(f"  Duration: {total_duration:.2f}s")
    logger.info(f"  Total requests: {request_count}")
    logger.info(f"  Successful: {successful_hits}")
    logger.info(f"  Failed: {failed_hits}")
    logger.info(f"  Success rate: {success_rate:.2f}%")
    logger.info(f"  Actual rate: {actual_rate:.2f} req/s")
    logger.info(f"  Target rate: {HITS_PER_SEC} req/s")
    logger.info("=" * 60)
    
    return successful_hits, failed_hits


def main():
    """Main entry point for the worker."""
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("Worker starting...")
    logger.info(f"Pod hostname: {os.getenv('HOSTNAME', 'unknown')}")
    
    try:
        successful, failed = run_load_test()
        
        if shutdown_requested:
            logger.info("Worker terminated by signal")
            sys.exit(0)
        
        logger.info("Worker completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Worker failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
