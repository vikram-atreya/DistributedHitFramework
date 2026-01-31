# Distributed Load Testing Framework

A Python-based, containerized, Kubernetes-managed distributed load testing system designed for Azure Kubernetes Service (AKS).

## Architecture

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Client    │────▶│   Gateway API   │────▶│ Azure Service   │
│             │     │   (FastAPI)     │     │   Bus Queue     │
└─────────────┘     └─────────────────┘     └────────┬────────┘
                                                     │
                                                     ▼
┌─────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Azure Redis │◀────│   Target API    │◀────│    Workers      │
│   Cache     │     │   (FastAPI)     │     │   (N pods)      │
└─────────────┘     └─────────────────┘     └────────┬────────┘
                                                     │
                                            ┌────────┴────────┐
                                            │  Orchestrator   │
                                            │    Agent        │
                                            └─────────────────┘
```

## Components

| Component | Description |
|-----------|-------------|
| **Target API** | FastAPI service that increments a Redis counter on each hit |
| **Gateway API** | FastAPI service that accepts load test requests and publishes to Service Bus |
| **Orchestrator** | Long-running service that creates/scales worker deployments |
| **Worker** | Stateless load generator that hits the Target API at configured rate |

## Prerequisites

- Docker
- Kubernetes cluster (AKS recommended)
- Helm 3.x
- Azure Redis Cache
- Azure Service Bus

## Project Structure

```
.
├── target-api/
│   ├── app/
│   │   └── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── gateway-api/
│   ├── app/
│   │   └── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── orchestrator/
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── worker/
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── charts/
│   └── load-tester/
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
└── README.md
```

## Quick Start

### 1. Build Docker Images

```bash
# Build all images
docker build -t target-api:latest ./target-api
docker build -t gateway-api:latest ./gateway-api
docker build -t orchestrator:latest ./orchestrator
docker build -t worker:latest ./worker

# Tag and push to your container registry (e.g., Azure Container Registry)
ACR_NAME=<your-acr-name>

az acr login --name $ACR_NAME

docker tag target-api:latest $ACR_NAME.azurecr.io/target-api:latest
docker tag gateway-api:latest $ACR_NAME.azurecr.io/gateway-api:latest
docker tag orchestrator:latest $ACR_NAME.azurecr.io/orchestrator:latest
docker tag worker:latest $ACR_NAME.azurecr.io/worker:latest

docker push $ACR_NAME.azurecr.io/target-api:latest
docker push $ACR_NAME.azurecr.io/gateway-api:latest
docker push $ACR_NAME.azurecr.io/orchestrator:latest
docker push $ACR_NAME.azurecr.io/worker:latest
```

### 2. Create Azure Resources

```bash
# Create resource group
az group create --name load-testing-rg --location eastus

# Create Azure Redis Cache
az redis create \
  --name my-redis-cache \
  --resource-group load-testing-rg \
  --location eastus \
  --sku Basic \
  --vm-size c0

# Create Azure Service Bus namespace and queue
az servicebus namespace create \
  --name my-servicebus-ns \
  --resource-group load-testing-rg \
  --location eastus \
  --sku Standard

az servicebus queue create \
  --name load-test-requests \
  --namespace-name my-servicebus-ns \
  --resource-group load-testing-rg

# Get connection strings
REDIS_HOST=$(az redis show --name my-redis-cache --resource-group load-testing-rg --query hostName -o tsv)
REDIS_PASSWORD=$(az redis list-keys --name my-redis-cache --resource-group load-testing-rg --query primaryKey -o tsv)
SERVICE_BUS_CONN=$(az servicebus namespace authorization-rule keys list \
  --namespace-name my-servicebus-ns \
  --resource-group load-testing-rg \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv)
```

### 3. Create AKS Cluster

```bash
# Create AKS cluster
az aks create \
  --resource-group load-testing-rg \
  --name load-testing-aks \
  --node-count 3 \
  --enable-managed-identity \
  --generate-ssh-keys \
  --attach-acr $ACR_NAME

# Get credentials
az aks get-credentials --resource-group load-testing-rg --name load-testing-aks
```

### 4. Deploy with Helm

```bash
# Create values file with your configuration
cat > my-values.yaml << EOF
images:
  registry: $ACR_NAME.azurecr.io
  pullPolicy: Always

redis:
  host: "$REDIS_HOST"
  port: 6380
  password: "$REDIS_PASSWORD"
  ssl: true

serviceBus:
  connectionString: "$SERVICE_BUS_CONN"
  queueName: load-test-requests
EOF

# Install the Helm chart
helm install load-tester ./charts/load-tester -f my-values.yaml
```

### 5. Run a Load Test

```bash
# Get the Gateway API external IP
GATEWAY_IP=$(kubectl get svc gateway-api -n load-testing -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Submit a load test request
curl -X POST "http://$GATEWAY_IP:8000/tests" \
  -H "Content-Type: application/json" \
  -d '{
    "hits_per_sec": 500,
    "duration_sec": 60,
    "workers": 5
  }'

# Check the current hit count
curl "http://$GATEWAY_IP:8000/stats"
```

## API Reference

### Gateway API

#### POST /tests
Submit a new load test request.

**Request Body:**
```json
{
  "hits_per_sec": 500,
  "duration_sec": 300,
  "workers": 5
}
```

**Response:**
```json
{
  "test_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "Test request submitted successfully",
  "parameters": {
    "hits_per_sec": 500,
    "duration_sec": 300,
    "workers": 5
  }
}
```

### Target API

#### POST /hit
Increment the global hit counter.

**Response:**
```json
{
  "total_hits": 12345
}
```

#### GET /stats
Get current hit statistics without incrementing.

**Response:**
```json
{
  "total_hits": 12345
}
```

#### POST /reset
Reset the hit counter to zero.

## Configuration

### Environment Variables

#### Target API
| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_HOST` | Redis server hostname | `localhost` |
| `REDIS_PORT` | Redis server port | `6379` |
| `REDIS_PASSWORD` | Redis authentication password | `""` |
| `REDIS_SSL` | Enable SSL connection | `false` |
| `REDIS_KEY` | Redis key for hit counter | `total_hits` |

#### Gateway API
| Variable | Description | Default |
|----------|-------------|---------|
| `SERVICE_BUS_CONNECTION_STRING` | Azure Service Bus connection string | `""` |
| `SERVICE_BUS_QUEUE_NAME` | Queue name for test requests | `load-test-requests` |

#### Orchestrator
| Variable | Description | Default |
|----------|-------------|---------|
| `SERVICE_BUS_CONNECTION_STRING` | Azure Service Bus connection string | `""` |
| `SERVICE_BUS_QUEUE_NAME` | Queue name for test requests | `load-test-requests` |
| `K8S_NAMESPACE` | Kubernetes namespace for workers | `load-testing` |
| `TARGET_URL` | URL of the Target API | `http://target-api:8000/hit` |
| `WORKER_IMAGE` | Docker image for workers | `worker:latest` |

#### Worker
| Variable | Description | Default |
|----------|-------------|---------|
| `TARGET_URL` | URL to hit | `http://target-api:8000/hit` |
| `HITS_PER_SEC` | Target requests per second | `10` |
| `DURATION_SEC` | Test duration in seconds | `60` |

## Helm Values

Key configuration options in `values.yaml`:

```yaml
# Image registry (e.g., Azure Container Registry)
images:
  registry: myregistry.azurecr.io

# Azure Redis Cache
redis:
  host: myredis.redis.cache.windows.net
  port: 6380
  password: <redis-access-key>
  ssl: true

# Azure Service Bus
serviceBus:
  connectionString: <service-bus-connection-string>
  queueName: load-test-requests

# Resource limits
targetApi:
  replicaCount: 2
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 256Mi
```

## Monitoring

### View Worker Logs
```bash
# List worker pods
kubectl get pods -n load-testing -l app=load-worker

# View logs from a specific worker
kubectl logs -n load-testing <pod-name>
```

### View Orchestrator Logs
```bash
kubectl logs -n load-testing -l app=orchestrator -f
```

### Check Redis Counter
```bash
# Port-forward to Target API
kubectl port-forward svc/target-api -n load-testing 8000:8000

# Get current stats
curl http://localhost:8000/stats
```

## Local Development

### Run Target API Locally
```bash
cd target-api
pip install -r requirements.txt

# Start a local Redis (using Docker)
docker run -d -p 6379:6379 redis:alpine

# Run the API
export REDIS_HOST=localhost
export REDIS_PORT=6379
uvicorn app.main:app --reload --port 8000
```

### Run Worker Locally
```bash
cd worker
pip install -r requirements.txt

export TARGET_URL=http://localhost:8000/hit
export HITS_PER_SEC=10
export DURATION_SEC=30

python main.py
```

## Cleanup

```bash
# Uninstall Helm release
helm uninstall load-tester

# Delete Azure resources
az group delete --name load-testing-rg --yes --no-wait
```

## License

MIT License
