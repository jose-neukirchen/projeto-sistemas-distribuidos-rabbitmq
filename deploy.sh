#!/usr/bin/env bash
set -e

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=================================="
echo "Deploying to Kubernetes..."
echo "=================================="

# Use minikube docker environment
eval "$(minikube docker-env)"

# Build images
echo ""
echo "Building Docker images..."
docker build -t ms:latest -f "${ROOT_DIR}/Dockerfile" "${ROOT_DIR}"
docker build -t frontend:latest -f "${ROOT_DIR}/Dockerfile.frontend" "${ROOT_DIR}"

# Create namespace if it doesn't exist
kubectl get ns leilao >/dev/null 2>&1 || kubectl create ns leilao

echo ""
echo "Deploying to namespace 'leilao'..."

# Apply manifests
echo "  - Deploying microservices..."
kubectl apply -n leilao -f "${ROOT_DIR}/kube/ms.yaml"

echo "  - Deploying frontend..."
kubectl apply -n leilao -f "${ROOT_DIR}/kube/frontend.yaml"

echo ""
echo "=================================="
echo "✓ Deployment completed!"
echo "=================================="
echo ""
echo "Useful commands:"
echo "  - View pods: kubectl get pods -n leilao"
echo "  - View services: kubectl get svc -n leilao"
echo "  - View logs: kubectl logs -n leilao <pod-name>"
echo "  - API Gateway URL: minikube service api-gateway -n leilao --url"
echo "  - Access frontend: kubectl exec -it -n leilao deployment/frontend-client -- node client.js"
echo ""
