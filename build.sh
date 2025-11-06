#!/bin/bash

# Script para build das imagens Docker no Minikube

echo "=================================="
echo "Building Docker images..."
echo "=================================="

# Configura para usar o Docker do Minikube
eval $(minikube docker-env)

# Build da imagem dos microsserviços Python
echo ""
echo "Building ms:latest (Python microservices)..."
docker build -t ms:latest -f Dockerfile .

# Build da imagem do frontend Node.js
echo ""
echo "Building frontend:latest (Node.js client)..."
docker build -t frontend:latest -f Dockerfile.frontend .

echo ""
echo "=================================="
echo "✓ Build completed!"
echo "=================================="
echo ""
echo "Images built:"
docker images | grep -E "(ms|frontend)" | grep latest

echo ""
echo "To deploy, run: ./deploy.sh"
