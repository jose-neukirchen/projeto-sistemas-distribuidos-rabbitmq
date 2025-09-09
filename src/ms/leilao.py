# MS Leilão (publisher)
# 1. Mantém internamente uma lista pré-configurada (hardcoded)
# de leilões com: ID do leilão, descrição, data e hora de início e fim,
# status (ativo, encerrado).
# 2. O leilão de um determinado produto deve ser iniciado quando
# o tempo definido para esse leilão for atingido. Quando um leilão
# começa, ele publica o evento na fila: leilao_iniciado.
# 3. O leilão de um determinado produto deve ser finalizado
# quando o tempo definido para esse leilão expirar. Quando um leilão
# termina, ele publica o evento na fila: leilao_finalizado.

lista_leiloes = [
    {
        "id": 1,
        "descricao": "Leilão de Produto A",
        "data_inicio": "2023-10-01T10:00:00",
        "data_fim": "2023-10-01T12:00:00",
        "status": "ativo"
    },
    {
        "id": 2,
        "descricao": "Leilão de Produto B",
        "data_inicio": "2023-10-02T14:00:00",
        "data_fim": "2023-10-02T16:00:00",
        "status": "ativo"
    },
    {
        "id": 3,
        "descricao": "Leilão de Produto C",
        "data_inicio": "2023-10-03T14:00:00",
        "data_fim": "2023-10-03T16:00:00",
        "status": "ativo"
    },
    {
        "id": 4,
        "descricao": "Leilão de Produto D",
        "data_inicio": "2023-10-04T14:00:00",
        "data_fim": "2023-10-04T16:00:00",
        "status": "ativo"
    }
]

import os
import time
import json
import pika
from datetime import datetime, timedelta, timezone

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "cluster-leilao")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME", os.getenv("RABBITMQ_USER", "guest"))
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", os.getenv("RABBITMQ_PASS", "guest"))
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "leilao.events")

START_STAGGER_SEC = int(os.getenv("START_STAGGER_SEC", "2"))   # intervalo entre inícios
DURATION_SEC = int(os.getenv("DURATION_SEC", "30"))            # duração de cada leilão
START_DELAY_SEC = int(os.getenv("START_DELAY_SEC", "3"))  # atraso inicial

MAX_ACTIVE = 2
MAX_TOTAL = 10

# Conexão com o rabbimq
# return: objeto pika.ConnectionParameters.
def conn_params():
    creds = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    return pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, virtual_host=RABBITMQ_VHOST, credentials=creds, heartbeat=30)

# Declara exchange e filas leilao_iniciado e leilao_finalizado.
# garante filas antes de publicar eventos.
# params: ch (canal pika.BlockingChannel).
def declare_basics(ch):
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)
    
# realiza o publish enviando as mensagens para a exchange com a routing key devida
# params: ch (canal), routing_key (str), body (dict do evento).
def publish(ch, routing_key: str, body: dict):
    ch.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=routing_key,
        body=json.dumps(body).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
    )

def main():
    conn = pika.BlockingConnection(conn_params())
    ch = conn.channel()
    declare_basics(ch)

    if START_DELAY_SEC > 0:
        print(f"[MS_Leilao] aguardando {START_DELAY_SEC}s antes de iniciar o primeiro leilão (startup delay)")
        time.sleep(START_DELAY_SEC)

    print(f"[MS_Leilao] Iniciando: até {MAX_TOTAL} leilões, no máximo {MAX_ACTIVE} ativos simultaneamente")

    next_id = 1
    active = {}  # leilao_id -> end_time
    created_total = 0

    try:
        while created_total < MAX_TOTAL or active:
            now = datetime.now(timezone.utc)
            # Finaliza leilões que expiraram
            ended = [aid for aid, end_at in active.items() if now >= end_at]
            for aid in ended:
                evt_end = {"event": "leilao.finalizado", "id": aid, "fim": active[aid].isoformat()}
                publish(ch, "leilao.finalizado", evt_end)
                print(f"[MS_Leilao] finalizado: {evt_end}")
                del active[aid]
            # Cria novos se houver espaço e restam a criar
            while len(active) < MAX_ACTIVE and created_total < MAX_TOTAL:
                aid = next_id
                next_id += 1
                created_total += 1
                start_at = now
                end_at = start_at + timedelta(seconds=DURATION_SEC)
                active[aid] = end_at
                desc = f"Leilão {aid}"
                evt_start = {"event": "leilao.iniciado", "id": aid, "descricao": desc, "inicio": start_at.isoformat(), "fim": end_at.isoformat()}
                publish(ch, "leilao.iniciado", evt_start)
                print(f"[MS_Leilao] iniciado: {evt_start}")
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass

    print("[MS_Leilao] ciclo encerrado (todos leilões criados e finalizados). Mantendo processo vivo.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()