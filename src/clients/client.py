# Cliente (publisher e subscriber), DIRETIVAS:
# 1. Não se comunica diretamente com nenhum serviço, toda a
# comunicação é indireta através de filas de mensagens.
# 2. Logo ao inicializar, atuará como consumidor recebendo
# eventos da fila leilao_iniciado. Os eventos recebidos contêm ID do
# leilão, descrição, data e hora de início e fim.
# 3. Possui um par de chaves pública/privada. Publica lances na
# fila de mensagens lance_realizado. Cada lance contém: ID do
# leilão, ID do usuário, valor do lance. O cliente assina digitalmente
# cada lance com sua chave privada.
# 4. Ao dar um lance em um leilão, o cliente atuará como
# consumidor desse leilão, registrando interesse em receber
# notificações quando um novo lance for efetuado no leilão de seu
# interesse ou quando o leilão for encerrado. Por exemplo, se o
# cliente der um lance no leilão de ID 1, ele escutará a fila leilao_1.
# 5. Possui duas instâncias Cliente_A e Cliente_B.
# 6. Não precisa de interface gráfica, mas no cliente programar pra dar um lance de tempo em tempo (hardcoded)


import os
import time
import pika
import json
import base64
from threading import Thread, Lock
from datetime import datetime, timezone
from random import uniform
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")

EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "leilao.events")
CLIENT_ID = os.getenv("CLIENT_ID")
# Caminho da chave privada (necessário para assinatura)
PRIVATE_KEY_PATH = os.getenv("PRIVATE_KEY_PATH", "/app/private-keys/private.pem")
# Fila dedicada para receber broadcast de leilões iniciados (evita competir em uma única fila)
LEILAO_INICIADO_QUEUE = f"leilao_iniciado.{CLIENT_ID}"
LEILAO_FINALIZADO_QUEUE = f"leilao_finalizado.{CLIENT_ID}"
BID_INTERVAL = float(os.getenv("BID_INTERVAL", "6"))

# Cria parâmetros de conexão para o RabbitMQ, usados em todas as conexões do client.
# Centraliza a configuração de conexão, facilitando manutenção e reutilização.
# return: objeto pika.ConnectionParameters.
def _conn_params():
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=60,
    )


# Carrega a chave privada do client para assinar digitalmente os lances.
# (usa caminho em PRIVATE_KEY_PATH).
# return: RSA private key carregada.
def load_private_key():
    with open(PRIVATE_KEY_PATH, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())


# Realiza a assinatura digital do payload do lance usando a chave privada. Permitindo validação pelo MS Lance.
# params: priv_key (objeto chave privada), payload (dict com dados do lance).
# return: string Base64 da assinatura.
def sign_payload(priv_key, payload: dict) -> str:
    msg = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    signature = priv_key.sign(
        msg,
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode()


# Declara as exchanges e filas necessárias para consumir eventos e publicar lances.
# params: ch (canal pika).
def declare_basics(ch: pika.adapters.blocking_connection.BlockingChannel):
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)
    # Cada cliente possui sua própria fila para eventos de leilões iniciados (broadcast via binding compartilhado)
    ch.queue_declare(queue=LEILAO_INICIADO_QUEUE, durable=True)
    ch.queue_bind(queue=LEILAO_INICIADO_QUEUE, exchange=EXCHANGE_NAME, routing_key="leilao.iniciado")
    # fila dedicada para finalizações
    ch.queue_declare(queue=LEILAO_FINALIZADO_QUEUE, durable=True)
    ch.queue_bind(queue=LEILAO_FINALIZADO_QUEUE, exchange=EXCHANGE_NAME, routing_key="leilao.finalizado")


# Consome e imprime eventos de leilões iniciados, informando o client sobre novos leilões.
# Permite que o client saiba quando um leilão começa ou termina.
def consumer_leiloes():
    conn = pika.BlockingConnection(_conn_params())
    ch = conn.channel()
    declare_basics(ch)

    def on_started(_ch, _method, _props, body):
        try:
            evt = json.loads(body.decode())
            aid = int(evt.get("id"))
            with ACTIVE_LOCK:
                if aid in FINISHED_AUCTIONS:
                    # ignora reuso de id finalizado
                    pass
                else:
                    ACTIVE_AUCTIONS.add(aid)
            print(f"[{CLIENT_ID}] leilao_iniciado: {evt}")
        except Exception:
            print(f"[{CLIENT_ID}] leilao_iniciado: payload inválido")

    def on_finalizado(_ch, _method, _props, body):
        try:
            evt = json.loads(body.decode())
            aid = int(evt.get("id"))
            with ACTIVE_LOCK:
                ACTIVE_AUCTIONS.discard(aid)
                FINISHED_AUCTIONS.add(aid)
            print(f"[{CLIENT_ID}] leilao_finalizado: {evt}")
        except Exception:
            print(f"[{CLIENT_ID}] leilao_finalizado: payload inválido")

    ch.basic_consume(queue=LEILAO_INICIADO_QUEUE, on_message_callback=lambda *a: (on_started(*a), None), auto_ack=True)
    ch.basic_consume(queue=LEILAO_FINALIZADO_QUEUE, on_message_callback=lambda *a: (on_finalizado(*a), None), auto_ack=True)
    print(f"[{CLIENT_ID}] Waiting events on '{LEILAO_INICIADO_QUEUE}' e '{LEILAO_FINALIZADO_QUEUE}'")
    try:
        ch.start_consuming()
    finally:
        conn.close()


# Declara e vincula a fila específica do leilão para receber notificações.
# Permite que o client receba atualizações e resultados dos leilões em que participa.
# params: ch (canal pika), auction_id (str id do leilão).
# return: nome da fila, routing key.
def ensure_leilao_queue(ch, auction_id: str):
    qname = f"leilao_{auction_id}"
    rkey = f"leilao.{auction_id}"
    ch.queue_declare(queue=qname, durable=True)
    ch.queue_bind(queue=qname, exchange=EXCHANGE_NAME, routing_key=rkey)
    return qname, rkey


# Consome mensagens da fila do leilão específico, exibindo notificações ao usuário.
# Mantém o client informado sobre lances e encerramento do leilão de interesse.
# params: auction_id (str id do leilão).
def consume_notifications(auction_id: str):
    conn = pika.BlockingConnection(_conn_params())
    ch = conn.channel()
    declare_basics(ch)
    qname, _ = ensure_leilao_queue(ch, auction_id)

    def on_notify(_ch, _method, _props, body):
        try:
            msg = json.loads(body.decode())
        except Exception:
            msg = {"raw": body.decode(errors="ignore")}
        print(f"[{CLIENT_ID}] notif leilao_{auction_id}: {msg}")
        # Mensagem de resultado
        if isinstance(msg, dict) and msg.get("event") == "leilao.vencedor" and str(msg.get("auction_id")) == auction_id:
            winner = msg.get("user_id")
            value = msg.get("value")
            if winner == CLIENT_ID:
                print(f"Parabéns, você venceu o leilão {auction_id} com o lance de {value}")
            else:
                print(f"Infelizmente você não venceu o leilao {auction_id}, o lance vencedor foi o de valor {value}.")

    ch.basic_consume(queue=qname, on_message_callback=lambda *args: (on_notify(*args), None), auto_ack=True)
    print(f"[{CLIENT_ID}] Esperando atualizações para leilao_{auction_id}")
    try:
        ch.start_consuming()
    finally:
        conn.close()


# Conjunto de leilões ativos recebidos (controle local)
ACTIVE_AUCTIONS = set()
FINISHED_AUCTIONS = set()  # leilões já finalizados
LAST_BIDS = {}  # auction_id -> último valor publicado
ACTIVE_LOCK = Lock()

# publica lances assinados, consome notificações, da os lances hardcoded
# Publica os lances assinados recorrrentement e inicia consumidores de notificações para cada leilão de interesse.
# Automatiza o envio de lances e o recebimento de notificações, simulando o comportamento do cliente 
def publisher_loop():
    priv = load_private_key()
    conn = pika.BlockingConnection(_conn_params())
    ch = conn.channel()
    declare_basics(ch)
    started_consumers = set()

    try:
        ch.queue_declare(queue="lance_realizado", durable=True)
        ch.queue_bind(queue="lance_realizado", exchange=EXCHANGE_NAME, routing_key="lance.realizado")
        while True:
            now = datetime.now(timezone.utc).isoformat()
            with ACTIVE_LOCK:
                active_snapshot = set(ACTIVE_AUCTIONS)
            for aid in active_snapshot:
                if aid in FINISHED_AUCTIONS:
                    continue
                if aid not in LAST_BIDS:
                    LAST_BIDS[aid] = 0.0
                prev = LAST_BIDS[aid]
                raw = uniform(0, 1000)
                if raw < prev:
                    raw = uniform(prev, 1000) if prev < 1000 else prev
                bid_value = round(raw, 2)
                LAST_BIDS[aid] = bid_value
                aid_str = str(aid)
                if aid_str not in started_consumers:
                    Thread(target=consume_notifications, args=(aid_str,), daemon=True).start()
                    started_consumers.add(aid_str)
                payload = {"event": "lance.realizado", "auction_id": aid, "user_id": CLIENT_ID, "value": bid_value, "ts": now}
                signature = sign_payload(priv, payload)
                envelope = {"payload": payload, "signature": signature, "algo": "RSA-PKCS1v15-SHA256", "key_id": CLIENT_ID}
                ch.basic_publish(exchange=EXCHANGE_NAME, routing_key="lance.realizado", body=json.dumps(envelope).encode("utf-8"), properties=pika.BasicProperties(delivery_mode=2))
                print(f"[{CLIENT_ID}] lance.realizado -> {payload}")
            time.sleep(BID_INTERVAL if BID_INTERVAL > 0 else 10)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

def main():
    # Consumidor leilao_iniciado
    Thread(target=consumer_leiloes, daemon=True).start()
    # Publisher de lances
    publisher_loop()

if __name__ == "__main__":
    main()




