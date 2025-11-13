# MS Pagamento (publisher e subscriber)
# 1. Consome os eventos leilao_vencedor (ID do leilão, ID do vencedor, valor).
# 2. Para cada evento consumido, faz uma requisição REST ao sistema externo de pagamentos
#    enviando os dados do pagamento (valor, moeda, informações do cliente) e recebe um link de pagamento
#    que será publicado em link_pagamento.
# 3. Define um endpoint que recebe notificações assíncronas do sistema externo indicando o status
#    da transação (aprovada ou recusada). Com base nos eventos externos recebidos, publica eventos
#    status_pagamento, para que o API Gateway notifique o cliente via SSE.

import os
import json
import pika
import threading
import requests
from typing import Dict
from flask import Flask, request, jsonify

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "leilao.events")
PAGAMENTO_PORT = int(os.getenv("PAGAMENTO_PORT", "5002"))
EXTERNAL_PAYMENT_URL = os.getenv("EXTERNAL_PAYMENT_URL", "http://payment-external:8080")

# Filas dedicadas para consumo
LEILAO_VENCEDOR_QUEUE = "leilao_vencedor.ms_pagamento"

# Consome leilao_vencedor
# Publica link_pagamento e status_pagamento

# Mapeamento de transaction_id -> (auction_id, user_id)
transactions: Dict[str, tuple] = {}
transactions_lock = threading.Lock()

app = Flask(__name__)


def conn_params():
    """Cria parâmetros de conexão RabbitMQ"""
    creds = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=creds,
        heartbeat=30
    )


def declare_basics(ch):
    """Declara exchange e filas que este serviço CONSOME"""
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)
    
    # Fila consumida por este serviço
    ch.queue_declare(queue=LEILAO_VENCEDOR_QUEUE, durable=True)
    ch.queue_bind(queue=LEILAO_VENCEDOR_QUEUE, exchange=EXCHANGE_NAME, routing_key="leilao.vencedor")
    
    # Nota: Não declaramos filas para eventos que publicamos
    # O consumidor (API Gateway) declara suas próprias filas


def publish(routing_key: str, body: dict):
    """Envia eventos para a exchange (cria nova conexão por segurança)"""
    try:
        conn = pika.BlockingConnection(conn_params())
        ch = conn.channel()
        declare_basics(ch)
        ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=routing_key,
            body=json.dumps(body).encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        conn.close()
    except Exception as e:
        print(f"[MS_Pagamento] Erro ao publicar evento {routing_key}: {e}")


def on_leilao_vencedor(_ch, method, _props, body):
    """Callback para eventos de vencedor do leilão"""
    try:
        msg = json.loads(body.decode())
        auction_id = int(msg.get("auction_id"))
        winner_id = str(msg.get("winner_id"))
        value = float(msg.get("value"))
        
        print(f"[MS_Pagamento] Processando vencedor: leilão {auction_id}, vencedor {winner_id}, valor {value}")
        
        # Faz requisição ao sistema externo de pagamento
        payment_data = {
            "amount": value,
            "currency": "BRL",
            "customer": {
                "id": winner_id,
                "name": f"Cliente {winner_id}"
            },
            "description": f"Pagamento do leilão {auction_id}",
            "webhook_url": f"http://ms-pagamento:{PAGAMENTO_PORT}/webhook"
        }
        
        try:
            response = requests.post(
                f"{EXTERNAL_PAYMENT_URL}/payments",
                json=payment_data,
                timeout=10
            )
            
            if response.status_code == 201:
                payment_response = response.json()
                transaction_id = payment_response.get("transaction_id")
                payment_link = payment_response.get("payment_link")
                
                # Armazena mapeamento de transação
                with transactions_lock:
                    transactions[transaction_id] = (auction_id, winner_id)
                
                # Publica link de pagamento
                event = {
                    "event": "link_pagamento",
                    "auction_id": auction_id,
                    "user_id": winner_id,
                    "transaction_id": transaction_id,
                    "payment_link": payment_link,
                    "amount": value
                }
                publish("link.pagamento", event)
                print(f"[MS_Pagamento] Link de pagamento gerado: {payment_link}")
            else:
                print(f"[MS_Pagamento] Erro ao gerar pagamento: {response.status_code}")
                
        except Exception as e:
            print(f"[MS_Pagamento] Erro ao chamar sistema externo: {e}")
        
    except Exception as e:
        print(f"[MS_Pagamento] Erro ao processar leilao_vencedor: {e}")
    
    _ch.basic_ack(method.delivery_tag)


def consume_events():
    """Thread para consumir eventos do RabbitMQ"""
    conn = pika.BlockingConnection(conn_params())
    ch = conn.channel()
    declare_basics(ch)
    
    ch.basic_qos(prefetch_count=10)
    ch.basic_consume(queue=LEILAO_VENCEDOR_QUEUE, on_message_callback=on_leilao_vencedor, auto_ack=False)
    
    print("[MS_Pagamento] Consumindo eventos do RabbitMQ")
    try:
        ch.start_consuming()
    finally:
        conn.close()


# REST API Endpoints

@app.route('/health', methods=['GET'])
def health():
    """Endpoint de health check"""
    return jsonify({"status": "healthy"}), 200


@app.route('/webhook', methods=['POST'])
def webhook():
    """Recebe notificações do sistema externo de pagamento"""
    try:
        data = request.json
        transaction_id = data.get("transaction_id")
        status = data.get("status")  # "approved" ou "rejected"
        amount = data.get("amount")
        
        print(f"[MS_Pagamento] Webhook recebido: transação {transaction_id}, status {status}")
        
        # Busca informações da transação
        with transactions_lock:
            if transaction_id not in transactions:
                print(f"[MS_Pagamento] Transação {transaction_id} não encontrada")
                return jsonify({"error": "Transaction not found"}), 404
            
            auction_id, user_id = transactions[transaction_id]
        
        # Publica evento de status de pagamento
        event = {
            "event": "status_pagamento",
            "transaction_id": transaction_id,
            "auction_id": auction_id,
            "user_id": user_id,
            "status": status,
            "amount": amount
        }
        publish("status.pagamento", event)
        
        status_pt = "aprovado" if status == "approved" else "recusado"
        print(f"[MS_Pagamento] Pagamento {status_pt} para transação {transaction_id}")
        
        return jsonify({"message": "Webhook processed"}), 200
        
    except Exception as e:
        print(f"[MS_Pagamento] Erro ao processar webhook: {e}")
        return jsonify({"error": str(e)}), 500


def main():
    # Inicia thread para consumir eventos
    consumer_thread = threading.Thread(target=consume_events, daemon=True)
    consumer_thread.start()
    
    print(f"[MS_Pagamento] Iniciando servidor REST na porta {PAGAMENTO_PORT}")
    app.run(host='0.0.0.0', port=PAGAMENTO_PORT, threaded=True)


if __name__ == "__main__":
    main()
