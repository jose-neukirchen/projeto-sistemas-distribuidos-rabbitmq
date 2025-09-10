# MS Notificação (publisher e subscriber)
# 1. Escuta os eventos das filas lance_validado e
# leilao_vencedor.
# 2. Publica esses eventos nas filas específicas para cada leilão,
# de acordo com o seu ID (leilao_1, leilao_2, ...), de modo que
# somente os consumidores interessados nesses leilões recebam as
# notificações correspondentes.

import os
import json
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "leilao.events")

# Conexão com o rabbimq
# return: objeto pika.ConnectionParameters.
def conn_params():
    creds = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    return pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, virtual_host=RABBITMQ_VHOST, credentials=creds, heartbeat=30)

# Declara exchange direct e todas as filas+bindings tanto para consumo quanto para publicação.
# Garantir infraestrutura de filas antes de processar mensagens.
# param: ch (canal pika.BlockingChannel).
def declare_basics(ch):
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)
    for q, rk in [
        ("lance_validado", "lance.validado"),
        ("leilao_vencedor", "leilao.vencedor"),
    ]:
        ch.queue_declare(queue=q, durable=True)
        ch.queue_bind(queue=q, exchange=EXCHANGE_NAME, routing_key=rk)

# Cria dinamicamente fila/leilao_{id} e binding para routing key leilao.{id}.
# Isola notificações por leilão assim apenas interessados consomem.
# params: ch (canal), auction_id (int ID do leilão).
# return: qname, routing_key.
def ensure_leilao_queue(ch, auction_id: int):
    qname = f"leilao_{auction_id}"
    rkey = f"leilao.{auction_id}"
    ch.queue_declare(queue=qname, durable=True)
    ch.queue_bind(queue=qname, exchange=EXCHANGE_NAME, routing_key=rkey)
    return qname, rkey

# Realiza o publish: Envia mensagem p/ exchange com a rk correta
# params: ch (canal), routing_key (str), body (dict mensagem de evento).
def publish(ch, routing_key: str, body: dict):
    ch.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=routing_key,
        body=json.dumps(body).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
    )

# Desserializa evento, identifica auction_id, garante fila específica e republica para routing key leilao.{id}
# Faz fan-out especifico por leilão para que os consumidores recebam apenas eventos relevantes
# params: _ch (canal), method (entrega), body (bytes da mensagem).
def on_event(_ch, method, _props, body):
    try:
        msg = json.loads(body.decode())
        aid = int(msg.get("auction_id") or msg.get("id"))
    except Exception:
        print("[MS_Notificacao] mensagem inválida")
        _ch.basic_ack(method.delivery_tag)
        return
    qname, rkey = ensure_leilao_queue(_ch, aid)
    publish(_ch, rkey, msg)
    print(f"[MS_Notificacao] -> {qname}: {msg}")
    _ch.basic_ack(method.delivery_tag)

def main():
    conn = pika.BlockingConnection(conn_params())
    ch = conn.channel()
    declare_basics(ch)

    ch.basic_qos(prefetch_count=10)
    ch.basic_consume(queue="lance_validado", on_message_callback=on_event, auto_ack=False)
    ch.basic_consume(queue="leilao_vencedor", on_message_callback=on_event, auto_ack=False)

    print("[MS_Notificacao] consumindo filas: lance_validado, leilao_vencedor")
    try:
        ch.start_consuming()
    finally:
        conn.close()

if __name__ == "__main__":
    main()