FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

# Para build de cryptography
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libssl-dev libffi-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# libs Python - adicionadas Flask, Flask-CORS e requests
RUN pip install --no-cache-dir \
    pika==1.3.2 \
    flask==3.0.0 \
    flask-cors==4.0.0 \
    requests==2.31.0

COPY src /app/src
