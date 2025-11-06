# API Gateway (subscriber)
# 1. Expõe a API REST para o cliente:
#    - Criar um leilão
#    - Consultar leilões ativos
#    - Efetuar um lance
#    - Registrar interesse em receber notificações
#    - Cancelar interesse em receber notificações
# 2. Mantém conexões SSE com os clientes
# 3. Consome eventos: lance_validado, lance_invalidado, leilao_vencedor, link_pagamento, status_pagamento
# 4. Envia notificações via SSE aos clientes interessados

import os
import json
import time
import threading
import requests
from queue import Queue
from typing import Dict, Set
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "leilao.events")
MS_LEILAO_URL = os.getenv("MS_LEILAO_URL", "http://ms-leilao:5000")
MS_LANCE_URL = os.getenv("MS_LANCE_URL", "http://ms-lance:5000")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "8000"))

app = Flask(__name__)
CORS(app)

# Gerenciamento de interesses: auction_id -> set of client_ids
interests: Dict[int, Set[str]] = {}
interests_lock = threading.Lock()

# Filas de notificação por cliente: client_id -> Queue
client_queues: Dict[str, Queue] = {}
client_queues_lock = threading.Lock()


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
    """Declara exchange e filas para consumo de eventos"""
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)
    
    # Filas dedicadas para o API Gateway
    queues = [
        ("lance_validado.gateway", "lance.validado"),
        ("lance_invalidado.gateway", "lance.invalidado"),
        ("leilao_vencedor.gateway", "leilao.vencedor"),
        ("link_pagamento.gateway", "link.pagamento"),
        ("status_pagamento.gateway", "status.pagamento"),
    ]
    
    for queue_name, routing_key in queues:
        ch.queue_declare(queue=queue_name, durable=True)
        ch.queue_bind(queue=queue_name, exchange=EXCHANGE_NAME, routing_key=routing_key)


def get_client_queue(client_id: str) -> Queue:
    """Obtém ou cria fila de notificação para um cliente"""
    with client_queues_lock:
        if client_id not in client_queues:
            client_queues[client_id] = Queue()
        return client_queues[client_id]


def notify_clients(auction_id: int, event_data: dict):
    """Envia notificação para todos os clientes interessados em um leilão"""
    with interests_lock:
        interested_clients = interests.get(auction_id, set()).copy()
    
    for client_id in interested_clients:
        queue = get_client_queue(client_id)
        queue.put(event_data)


def on_lance_validado(_ch, method, _props, body):
    """Callback para eventos de lance validado"""
    try:
        msg = json.loads(body.decode())
        auction_id = int(msg.get("auction_id"))
        
        event = {
            "event": "lance_validado",
            "data": msg
        }
        
        notify_clients(auction_id, event)
        print(f"[API Gateway] Lance validado notificado para leilão {auction_id}")
        
    except Exception as e:
        print(f"[API Gateway] Erro ao processar lance_validado: {e}")
    
    _ch.basic_ack(method.delivery_tag)


def on_lance_invalidado(_ch, method, _props, body):
    """Callback para eventos de lance invalidado"""
    try:
        msg = json.loads(body.decode())
        user_id = msg.get("user_id")
        
        event = {
            "event": "lance_invalidado",
            "data": msg
        }
        
        # Notifica apenas o usuário que tentou dar o lance
        queue = get_client_queue(user_id)
        queue.put(event)
        print(f"[API Gateway] Lance invalidado notificado para usuário {user_id}")
        
    except Exception as e:
        print(f"[API Gateway] Erro ao processar lance_invalidado: {e}")
    
    _ch.basic_ack(method.delivery_tag)


def on_leilao_vencedor(_ch, method, _props, body):
    """Callback para eventos de vencedor do leilão"""
    try:
        msg = json.loads(body.decode())
        auction_id = int(msg.get("auction_id") or msg.get("id"))
        
        event = {
            "event": "leilao_vencedor",
            "data": msg
        }
        
        notify_clients(auction_id, event)
        print(f"[API Gateway] Vencedor notificado para leilão {auction_id}")
        
    except Exception as e:
        print(f"[API Gateway] Erro ao processar leilao_vencedor: {e}")
    
    _ch.basic_ack(method.delivery_tag)


def on_link_pagamento(_ch, method, _props, body):
    """Callback para eventos de link de pagamento"""
    try:
        msg = json.loads(body.decode())
        user_id = msg.get("user_id")
        
        event = {
            "event": "link_pagamento",
            "data": msg
        }
        
        # Notifica apenas o vencedor
        queue = get_client_queue(user_id)
        queue.put(event)
        print(f"[API Gateway] Link de pagamento enviado para {user_id}")
        
    except Exception as e:
        print(f"[API Gateway] Erro ao processar link_pagamento: {e}")
    
    _ch.basic_ack(method.delivery_tag)


def on_status_pagamento(_ch, method, _props, body):
    """Callback para eventos de status de pagamento"""
    try:
        msg = json.loads(body.decode())
        user_id = msg.get("user_id")
        
        event = {
            "event": "status_pagamento",
            "data": msg
        }
        
        # Notifica apenas o usuário do pagamento
        queue = get_client_queue(user_id)
        queue.put(event)
        print(f"[API Gateway] Status de pagamento enviado para {user_id}")
        
    except Exception as e:
        print(f"[API Gateway] Erro ao processar status_pagamento: {e}")
    
    _ch.basic_ack(method.delivery_tag)


def consume_events():
    """Thread para consumir eventos do RabbitMQ"""
    conn = pika.BlockingConnection(conn_params())
    ch = conn.channel()
    declare_basics(ch)
    
    ch.basic_qos(prefetch_count=10)
    
    ch.basic_consume(queue="lance_validado.gateway", on_message_callback=on_lance_validado, auto_ack=False)
    ch.basic_consume(queue="lance_invalidado.gateway", on_message_callback=on_lance_invalidado, auto_ack=False)
    ch.basic_consume(queue="leilao_vencedor.gateway", on_message_callback=on_leilao_vencedor, auto_ack=False)
    ch.basic_consume(queue="link_pagamento.gateway", on_message_callback=on_link_pagamento, auto_ack=False)
    ch.basic_consume(queue="status_pagamento.gateway", on_message_callback=on_status_pagamento, auto_ack=False)
    
    print("[API Gateway] Consumindo eventos do RabbitMQ")
    try:
        ch.start_consuming()
    finally:
        conn.close()


# REST API Endpoints

@app.route('/health', methods=['GET'])
def health():
    """Endpoint de health check"""
    return jsonify({"status": "healthy"}), 200


@app.route('/api/leiloes', methods=['POST'])
def criar_leilao():
    """Criar um novo leilão"""
    try:
        data = request.json
        response = requests.post(f"{MS_LEILAO_URL}/leiloes", json=data, timeout=5)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/leiloes', methods=['GET'])
def consultar_leiloes():
    """Consultar leilões ativos"""
    try:
        response = requests.get(f"{MS_LEILAO_URL}/leiloes", timeout=5)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/lances', methods=['POST'])
def efetuar_lance():
    """Efetuar um lance"""
    try:
        data = request.json
        response = requests.post(f"{MS_LANCE_URL}/lances", json=data, timeout=5)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/interesses', methods=['POST'])
def registrar_interesse():
    """Registrar interesse em notificações de um leilão"""
    try:
        data = request.json
        client_id = data.get("client_id")
        auction_id = int(data.get("auction_id"))
        
        if not client_id or not auction_id:
            return jsonify({"error": "client_id e auction_id são obrigatórios"}), 400
        
        with interests_lock:
            if auction_id not in interests:
                interests[auction_id] = set()
            interests[auction_id].add(client_id)
        
        print(f"[API Gateway] Cliente {client_id} registrou interesse no leilão {auction_id}")
        return jsonify({"message": "Interesse registrado com sucesso"}), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/interesses', methods=['DELETE'])
def cancelar_interesse():
    """Cancelar interesse em notificações de um leilão"""
    try:
        data = request.json
        client_id = data.get("client_id")
        auction_id = int(data.get("auction_id"))
        
        if not client_id or not auction_id:
            return jsonify({"error": "client_id e auction_id são obrigatórios"}), 400
        
        with interests_lock:
            if auction_id in interests and client_id in interests[auction_id]:
                interests[auction_id].remove(client_id)
                if not interests[auction_id]:
                    del interests[auction_id]
        
        print(f"[API Gateway] Cliente {client_id} cancelou interesse no leilão {auction_id}")
        return jsonify({"message": "Interesse cancelado com sucesso"}), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/sse/<client_id>')
def sse_stream(client_id):
    """Stream SSE para um cliente específico"""
    
    def event_generator():
        queue = get_client_queue(client_id)
        print(f"[API Gateway] Cliente {client_id} conectado via SSE")
        
        # Envia keepalive a cada 30 segundos
        last_keepalive = time.time()
        
        while True:
            try:
                # Tenta pegar evento da fila (timeout de 1s para permitir keepalive)
                try:
                    event = queue.get(timeout=1)
                    yield f"data: {json.dumps(event)}\n\n"
                    last_keepalive = time.time()
                except:
                    # Timeout - verifica se precisa enviar keepalive
                    if time.time() - last_keepalive > 30:
                        yield f": keepalive\n\n"
                        last_keepalive = time.time()
                        
            except GeneratorExit:
                print(f"[API Gateway] Cliente {client_id} desconectado do SSE")
                break
            except Exception as e:
                print(f"[API Gateway] Erro no SSE para {client_id}: {e}")
                break
    
    return Response(
        event_generator(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive'
        }
    )


def main():
    # Inicia thread para consumir eventos do RabbitMQ
    consumer_thread = threading.Thread(target=consume_events, daemon=True)
    consumer_thread.start()
    
    print(f"[API Gateway] Iniciando servidor na porta {GATEWAY_PORT}")
    app.run(host='0.0.0.0', port=GATEWAY_PORT, threaded=True)


if __name__ == "__main__":
    main()
