#!/usr/bin/env bash
set -e

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

eval "$(minikube docker-env)"

# Build das imagens locais
docker build -t client:latest -f "${ROOT_DIR}/Dockerfile" "${ROOT_DIR}"
docker build -t ms:latest -f "${ROOT_DIR}/Dockerfile" "${ROOT_DIR}"

kubectl get ns leilao >/dev/null 2>&1 || kubectl create ns leilao

# Aplicar manifestos 
# kubectl apply -n leilao -f "${ROOT_DIR}/kube/infra.yaml"
# # adiciona sleep de 60s para garantir que o RabbitMQ esteja pronto
# sleep 60
kubectl apply -n leilao -f "${ROOT_DIR}/kube/ms.yaml"
kubectl apply -n leilao -f "${ROOT_DIR}/kube/clients.yaml"

echo "Deploy aplicado no namespace 'leilao'."
