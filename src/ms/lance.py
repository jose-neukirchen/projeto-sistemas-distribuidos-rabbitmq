# MS Lance (publisher e subscriber)
# 1. Consome os eventos leilao_iniciado e leilao_finalizado.
# 2. Recebe requisições REST do gateway contendo um lance (ID do leilão; ID do usuário, valor do lance).
#    Somente aceita o lance se o leilão estiver ativo e se o lance for maior que o último lance registrado:
#    - Se o lance for válido, o MS Lance publica lance_validado.
#    - Caso contrário, ele publica o evento lance_invalidado.
# 3. Quando o evento leilao_finalizado é recebido, o MS Lance determina o vencedor e publica leilao_vencedor,
#    informando o ID do leilão, o ID do vencedor do leilão e o valor negociado.

import os
import json
import pika
import requests
import threading
from typing import Dict, Tuple, Set
from flask import Flask, request, jsonify

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "leilao.events")
LANCE_PORT = int(os.getenv("LANCE_PORT", "5001"))

# Filas dedicadas para consumo
LEILAO_INICIADO_QUEUE = "leilao_iniciado.ms_lance"
LEILAO_FINALIZADO_QUEUE = "leilao_finalizado.ms_lance"

# Consome leilao_iniciado e leilao_finalizado
# Publica lance_validado, lance_invalidado e leilao_vencedor

active_auctions: Set[int] = set()
best_bids: Dict[int, Tuple[str, float]] = {}  # auction_id -> (user_id, value)
auctions_lock = threading.Lock()

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
    
    # Filas consumidas por este serviço
    for q, rk in [
        (LEILAO_INICIADO_QUEUE, "leilao.iniciado"),
        (LEILAO_FINALIZADO_QUEUE, "leilao.finalizado"),
    ]:
        ch.queue_declare(queue=q, durable=True)
        ch.queue_bind(queue=q, exchange=EXCHANGE_NAME, routing_key=rk)
    
    # Nota: Não declaramos filas para eventos que publicamos
    # Os consumidores (API Gateway, MS Pagamento) declaram suas próprias filas


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
        print(f"[MS_Lance] Erro ao publicar evento {routing_key}: {e}")


def on_leilao_iniciado(_ch, method, _props, body):
    """Callback para eventos de leilão iniciado"""
    try:
        msg = json.loads(body.decode())
        aid = int(msg.get("id"))
        
        with auctions_lock:
            active_auctions.add(aid)
            if aid not in best_bids:
                best_bids[aid] = (None, 0.0)
        
        print(f"[MS_Lance] Leilão {aid} iniciado e marcado como ativo")
        
    except Exception as e:
        print(f"[MS_Lance] Erro ao processar leilao_iniciado: {e}")
    
    _ch.basic_ack(method.delivery_tag)


def on_leilao_finalizado(_ch, method, _props, body):
    """Callback para eventos de leilão finalizado"""
    try:
        msg = json.loads(body.decode())
        aid = int(msg.get("id"))
        
        with auctions_lock:
            if aid in active_auctions:
                active_auctions.remove(aid)
            
            # Publica vencedor se houver lances
            if aid in best_bids and best_bids[aid][0] is not None:
                winner_id, winner_value = best_bids[aid]
                event = {
                    "event": "leilao_vencedor",
                    "auction_id": aid,
                    "winner_id": winner_id,
                    "value": winner_value
                }
                publish("leilao.vencedor", event)
                print(f"[MS_Lance] Vencedor do leilão {aid}: {winner_id} com valor {winner_value}")
            else:
                print(f"[MS_Lance] Leilão {aid} finalizado sem lances")
        
    except Exception as e:
        print(f"[MS_Lance] Erro ao processar leilao_finalizado: {e}")
    
    _ch.basic_ack(method.delivery_tag)


def consume_events():
    """Thread para consumir eventos do RabbitMQ"""
    conn = pika.BlockingConnection(conn_params())
    ch = conn.channel()
    declare_basics(ch)
    
    ch.basic_qos(prefetch_count=10)
    ch.basic_consume(queue=LEILAO_INICIADO_QUEUE, on_message_callback=on_leilao_iniciado, auto_ack=False)
    ch.basic_consume(queue=LEILAO_FINALIZADO_QUEUE, on_message_callback=on_leilao_finalizado, auto_ack=False)
    
    print("[MS_Lance] Consumindo eventos do RabbitMQ")
    try:
        ch.start_consuming()
    finally:
        conn.close()


# REST API Endpoints

@app.route('/health', methods=['GET'])
def health():
    """Endpoint de health check"""
    return jsonify({"status": "healthy"}), 200


@app.route('/lances', methods=['POST'])
def efetuar_lance():
    """Recebe um lance via REST"""
    try:
        data = request.json
        auction_id = int(data.get("auction_id"))
        user_id = str(data.get("user_id"))
        value = float(data.get("value"))
        
        if not auction_id or not user_id or not value:
            return jsonify({"error": "Campos obrigatórios: auction_id, user_id, value"}), 400
        
        with auctions_lock:
            # Verifica se o leilão está ativo
            if auction_id not in active_auctions:
                event = {
                    "event": "lance_invalidado",
                    "auction_id": auction_id,
                    "user_id": user_id,
                    "value": value,
                    "reason": "Leilão não está ativo"
                }
                publish("lance.invalidado", event)
                print(f"[MS_Lance] Lance rejeitado: leilão {auction_id} não está ativo")
                return jsonify({"error": "Leilão não está ativo"}), 400
            
            # Verifica se o lance é maior que o último
            prev_user, prev_value = best_bids.get(auction_id, (None, 0.0))
            
            if value <= prev_value:
                event = {
                    "event": "lance_invalidado",
                    "auction_id": auction_id,
                    "user_id": user_id,
                    "value": value,
                    "reason": f"Lance deve ser maior que {prev_value}"
                }
                publish("lance.invalidado", event)
                print(f"[MS_Lance] Lance rejeitado: valor {value} não é maior que {prev_value}")
                return jsonify({"error": f"Lance deve ser maior que {prev_value}"}), 400
            
            # Lance válido - atualiza e publica
            best_bids[auction_id] = (user_id, value)
            
            event = {
                "event": "lance_validado",
                "auction_id": auction_id,
                "user_id": user_id,
                "value": value,
                "previous_value": prev_value
            }
            publish("lance.validado", event)
            print(f"[MS_Lance] Lance válido: leilão {auction_id}, usuário {user_id}, valor {value}")
            
            return jsonify({
                "message": "Lance aceito",
                "auction_id": auction_id,
                "user_id": user_id,
                "value": value
            }), 200
        
    except Exception as e:
        print(f"[MS_Lance] Erro ao processar lance: {e}")
        return jsonify({"error": str(e)}), 500


def main():
    # Inicia thread para consumir eventos
    consumer_thread = threading.Thread(target=consume_events, daemon=True)
    consumer_thread.start()
    
    print(f"[MS_Lance] Iniciando servidor REST na porta {LANCE_PORT}")
    app.run(host='0.0.0.0', port=LANCE_PORT, threaded=True)


if __name__ == "__main__":
    main()
