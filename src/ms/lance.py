# MS Lance (publisher e subscriber)
# 1. Possui as chaves públicas de todos os clientes.
# 2. Escuta os eventos das filas lance_realizado, leilao_iniciado
# e leilao_finalizado.
# 3. Recebe lances de usuários (ID do leilão; ID do usuário, valor
# do lance) e checa a assinatura digital da mensagem utilizando a
# chave pública correspondente. Somente aceitará o lance se:
# 	3.1. A assinatura for válida;
# 	3.2. ID do leilão existir e se o leilão estiver ativo;
# 	3.3. Se o lance for maior que o último lance registrado;
# 4. Se o lance for válido, o MS Lance publica o evento na fila
# lance_validado.
# 5. Ao finalizar um leilão, deve publicar na fila leilao_vencedor,
# informando o ID do leilão, o ID do vencedor do leilão e o valor
# negociado. O vencedor é o que efetuou o maior lance válido até o
# encerramento.

import os
import json
import base64
import pika
from typing import Dict, Tuple, Set
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME", os.getenv("RABBITMQ_USER"))
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", os.getenv("RABBITMQ_PASS"))
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "leilao.events")
KEYS_DIR = os.getenv("KEYS_DIR")
LEILAO_INICIADO_QUEUE = os.getenv("MS_LANCE_LEILAO_INICIADO_QUEUE", "leilao_iniciado.ms_lance")
LEILAO_FINALIZADO_QUEUE = os.getenv("MS_LANCE_LEILAO_FINALIZADO_QUEUE", "leilao_finalizado.ms_lance")



# connect com o rabbimq
# return: objeto pika.ConnectionParameters.
def conn_params():
    creds = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    return pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, virtual_host=RABBITMQ_VHOST, credentials=creds, heartbeat=30)

# Declara exchange direct e todas as filas+bindings tanto para consumo quanto para publicação.
# Garantir infraestrutura de filas antes de processar mensagens.
# param: ch (canal pika.BlockingChannel).
def declare_basics(ch):
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)
    # filas consumidas (dedicadas para broadcast de início/fim)
    for q, rk in [
        ("lance_realizado", "lance.realizado"),
        (LEILAO_INICIADO_QUEUE, "leilao.iniciado"),
        (LEILAO_FINALIZADO_QUEUE, "leilao.finalizado"),
    ]:
        ch.queue_declare(queue=q, durable=True)
        ch.queue_bind(queue=q, exchange=EXCHANGE_NAME, routing_key=rk)
    # filas publicadas (únicas)
    for q, rk in [
        ("lance_validado", "lance.validado"),
        ("leilao_vencedor", "leilao.vencedor"),
    ]:
        ch.queue_declare(queue=q, durable=True)
        ch.queue_bind(queue=q, exchange=EXCHANGE_NAME, routing_key=rk)

_pub_conn = pika.BlockingConnection(conn_params())
_pub_ch = _pub_conn.channel()
declare_basics(_pub_ch)

active_auctions: Set[int] = set()
best_bids: Dict[int, Tuple[str, float]] = {}  # auction_id -> (user_id, value)
pubkey_cache: Dict[str, object] = {}

# Carrega e faz cache da chave pública correspondente ao client_id para validação futura.
# Disponibiliza a chave pública para cada lance.
# param: client_id (str identificador do cliente).
# return: objeto de chave pública (RSA public key).
def load_pubkey(client_id: str):
    if client_id in pubkey_cache:
        return pubkey_cache[client_id]
    path = os.path.join(KEYS_DIR, f"{client_id}_public.pem")
    with open(path, "rb") as f:
        key = serialization.load_pem_public_key(f.read(), backend=default_backend())
        pubkey_cache[client_id] = key
        return key

# Decodifica assinatura e usa a chave pública para verificar
# Garantir autenticidade e integridade do lance
# param: envelope (dict contendo payload e assinatura Base64).
# return: bool (True se assinatura válida, False caso contrário).
def verify_signature(envelope: dict) -> bool:
    try:
        payload = envelope["payload"]
        signature_b64 = envelope["signature"]
        client_id = envelope.get("key_id") or payload["user_id"]
        pubkey = load_pubkey(client_id)
        msg = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        signature = base64.b64decode(signature_b64)
        pubkey.verify(signature, msg, padding.PKCS1v15(), hashes.SHA256())
        return True
    except Exception as e:
        print(f"[MS_Lance] assinatura inválida: {e}")
        return False

# Envia eventos para a exchange com a devida rk.
# param: routing_key (str), body (dict do evento a publicar).
# return: None.
def publish(routing_key: str, body: dict):
    _pub_ch.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=routing_key,
        body=json.dumps(body).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
    )

# Desserializa, verifica leilão ativo, assinatura e valor maior. publica evento de validação
# param: _ch (canal), _method (entrega AMQP), _props (propriedades), body (bytes mensagem).
def on_lance_realizado(_ch, _method, _props, body):
    try:
        envelope = json.loads(body.decode())
        payload = envelope["payload"]
    except Exception:
        print("[MS_Lance] payload inválido")
        _ch.basic_ack(_method.delivery_tag)
        return

    aid = int(payload["auction_id"])
    uid = str(payload["user_id"])
    val = float(payload["value"])

    if not verify_signature(envelope):
        print(f"[MS_Lance] assinatura inválida para {uid}")
        _ch.basic_ack(_method.delivery_tag)
        return

    prev = best_bids.get(aid)
    if prev is None or val > prev[1]:
        best_bids[aid] = (uid, val)
        evt = {
            "event": "lance.validado",
            "auction_id": aid,
            "user_id": uid,
            "value": val,
            "ts": payload.get("ts"),
        }
        publish("lance.validado", evt)
        print(f"[MS_Lance] lance válido {evt}")
    else:
        print(f"[MS_Lance] lance {val} <= atual {prev[1]}")

    _ch.basic_ack(_method.delivery_tag)


# Marca leilão como ativo e reseta melhor lance.
# param: _ch (canal), _method, _props, body (bytes evento).
def on_leilao_iniciado(_ch, _method, _props, body):
    try:
        evt = json.loads(body.decode())
        aid = int(evt["id"])
    except Exception:
        _ch.basic_ack(_method.delivery_tag)
        return
    active_auctions.add(aid)
    best_bids.pop(aid, None)
    print(f"[MS_Lance] leilão iniciado {aid}")
    _ch.basic_ack(_method.delivery_tag)

# Remove leilão de ativos e publica vencedor se houver lance registrado.
# param: _ch (canal), _method, _props, body (bytes evento).
def on_leilao_finalizado(_ch, _method, _props, body):
    try:
        evt = json.loads(body.decode())
        aid = int(evt["id"])
    except Exception:
        _ch.basic_ack(_method.delivery_tag)
        return
    was_active = aid in active_auctions
    active_auctions.discard(aid)
    winner = best_bids.get(aid)
    if not was_active:
        print(f"[MS_Lance] aviso: finalização recebida para leilão {aid} não marcado ativo (possível perda de leilao.iniciado)")
    if winner:
        uid, val = winner
        out = {"event": "leilao.vencedor", "auction_id": aid, "user_id": uid, "value": val}
        publish("leilao.vencedor", out)
        print(f"[MS_Lance] vencedor {out}")
    else:
        print(f"[MS_Lance] sem lances válidos no leilão {aid}")
    _ch.basic_ack(_method.delivery_tag)

def main():
    conn = pika.BlockingConnection(conn_params())
    ch = conn.channel()
    declare_basics(ch)

    ch.basic_qos(prefetch_count=10)
    ch.basic_consume(queue="lance_realizado", on_message_callback=on_lance_realizado, auto_ack=False)
    ch.basic_consume(queue=LEILAO_INICIADO_QUEUE, on_message_callback=on_leilao_iniciado, auto_ack=False)
    ch.basic_consume(queue=LEILAO_FINALIZADO_QUEUE, on_message_callback=on_leilao_finalizado, auto_ack=False)

    print(f"[MS_Lance] consumindo filas: lance_realizado, {LEILAO_INICIADO_QUEUE}, {LEILAO_FINALIZADO_QUEUE}")
    try:
        ch.start_consuming()
    finally:
        try:
            _pub_conn.close()
        except Exception:
            pass
        conn.close()

if __name__ == "__main__":
    main()
