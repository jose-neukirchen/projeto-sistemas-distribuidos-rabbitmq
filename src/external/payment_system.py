# Sistema de Pagamento Externo (Mock)
# 1. Recebe requisições REST do MS Pagamento para iniciar a transação e retorna um link de pagamento.
# 2. Após o processamento do pagamento (simulado), envia uma notificação assíncrona via webhook
#    (HTTP POST) ao endpoint configurado no MS Pagamento.
# 3. A notificação inclui: ID da transação, status do pagamento (aprovado ou recusado), valor e dados do comprador.

import os
import json
import uuid
import time
import threading
import requests
from flask import Flask, request, jsonify

PAYMENT_PORT = int(os.getenv("PAYMENT_PORT", "8080"))
PAYMENT_PROCESS_DELAY = int(os.getenv("PAYMENT_PROCESS_DELAY", "10"))  # Segundos para processar pagamento
APPROVAL_RATE = float(os.getenv("APPROVAL_RATE", "0.8"))  # 80% de aprovação

app = Flask(__name__)


def process_payment_async(transaction_id, webhook_url, amount, customer_id):
    """Processa o pagamento de forma assíncrona e envia webhook"""
    # Simula tempo de processamento
    time.sleep(PAYMENT_PROCESS_DELAY)
    
    # Determina status (80% aprovado, 20% recusado)
    import random
    status = "approved" if random.random() < APPROVAL_RATE else "rejected"
    
    # Envia webhook
    webhook_data = {
        "transaction_id": transaction_id,
        "status": status,
        "amount": amount,
        "customer_id": customer_id,
        "processed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    
    try:
        response = requests.post(webhook_url, json=webhook_data, timeout=5)
        print(f"[Payment External] Webhook enviado: {transaction_id} -> {status} (status code: {response.status_code})")
    except Exception as e:
        print(f"[Payment External] Erro ao enviar webhook: {e}")


@app.route('/health', methods=['GET'])
def health():
    """Endpoint de health check"""
    return jsonify({"status": "healthy"}), 200


@app.route('/payments', methods=['POST'])
def create_payment():
    """Cria um novo pagamento e retorna link de pagamento"""
    try:
        data = request.json
        amount = data.get("amount")
        currency = data.get("currency", "BRL")
        customer = data.get("customer", {})
        description = data.get("description", "")
        webhook_url = data.get("webhook_url")
        
        if not amount or not customer or not webhook_url:
            return jsonify({"error": "Campos obrigatórios: amount, customer, webhook_url"}), 400
        
        # Gera ID de transação único
        transaction_id = str(uuid.uuid4())
        
        # Gera link de pagamento simulado
        payment_link = f"https://payment.example.com/pay/{transaction_id}"
        
        print(f"[Payment External] Pagamento criado: {transaction_id}, valor: {amount} {currency}")
        
        # Inicia processamento assíncrono
        customer_id = customer.get("id", "unknown")
        thread = threading.Thread(
            target=process_payment_async,
            args=(transaction_id, webhook_url, amount, customer_id),
            daemon=True
        )
        thread.start()
        
        return jsonify({
            "transaction_id": transaction_id,
            "payment_link": payment_link,
            "amount": amount,
            "currency": currency,
            "status": "pending"
        }), 201
        
    except Exception as e:
        print(f"[Payment External] Erro ao criar pagamento: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/payments/<transaction_id>', methods=['GET'])
def get_payment_status(transaction_id):
    """Consulta o status de um pagamento"""
    # Mock - retorna sempre pending
    return jsonify({
        "transaction_id": transaction_id,
        "status": "pending"
    }), 200


def main():
    print(f"[Payment External] Iniciando sistema de pagamento externo na porta {PAYMENT_PORT}")
    print(f"[Payment External] Delay de processamento: {PAYMENT_PROCESS_DELAY}s")
    print(f"[Payment External] Taxa de aprovação: {APPROVAL_RATE * 100}%")
    app.run(host='0.0.0.0', port=PAYMENT_PORT, threaded=True)


if __name__ == "__main__":
    main()
