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
        "data_inicio": "2025-10-01T10:00:00",
        "data_fim": "2025-10-01T12:00:00",
        "status": "ativo"
    },
    {
        "id": 2,
        "descricao": "Leilão de Produto B",
        "data_inicio": "2025-10-02T14:00:00",
        "data_fim": "2025-10-02T16:00:00",
        "status": "ativo"
    },
    {
        "id": 3,
        "descricao": "Leilão de Produto C",
        "data_inicio": "2025-10-03T14:00:00",
        "data_fim": "2025-10-03T16:00:00",
        "status": "ativo"
    },
    {
        "id": 4,
        "descricao": "Leilão de Produto D",
        "data_inicio": "2025-10-04T14:00:00",
        "data_fim": "2025-10-04T16:00:00",
        "status": "ativo"
    },
    {
        "id": 5,
        "descricao": "Leilão de Produto E",
        "data_inicio": "2025-10-05T14:00:00",
        "data_fim": "2025-10-05T16:00:00",
        "status": "ativo"
    },
    {
        "id": 6,
        "descricao": "Leilão de Produto F",
        "data_inicio": "2025-10-06T14:00:00",
        "data_fim": "2025-10-06T16:00:00",
        "status": "ativo"
    }
]

import os
import time
import json
import pika
from datetime import datetime, timedelta, timezone

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "leilao.events")

START_STAGGER_SEC = 20   # intervalo entre inícios
DURATION_SEC = 30       # duração de cada leilão
START_DELAY_SEC = 15  # atraso inicial

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

    # Prepara lista de leilões a partir da lista hardcoded
    # Ajusta datas para o datetime atual para demonstrar funcionamento
    now = datetime.now(timezone.utc)
    leiloes = []
    for idx, leilao in enumerate(lista_leiloes):
        aid = int(leilao["id"])
        desc = leilao["descricao"]
        start_at = now + timedelta(seconds=idx * START_STAGGER_SEC)
        end_at = start_at + timedelta(seconds=DURATION_SEC)
        leiloes.append({
            "id": aid,
            "descricao": desc,
            "start_at": start_at,
            "end_at": end_at,
            "status": "ativo"
        })

    print("[MS_Leilao] agendados:", [(l["id"], l["start_at"].isoformat(), l["end_at"].isoformat()) for l in leiloes])

    ativos = {}
    finalizados = set()
    total = len(leiloes)
    try:
        while len(finalizados) < total:
            now = datetime.now(timezone.utc)
            # Inicia leilões cujo horário chegou
            for l in leiloes:
                if l["id"] not in ativos and l["id"] not in finalizados and now >= l["start_at"]:
                    evt_start = {"event": "leilao.iniciado", "id": l["id"], "descricao": l["descricao"], "inicio": l["start_at"].isoformat(), "fim": l["end_at"].isoformat()}
                    publish(ch, "leilao.iniciado", evt_start)
                    print(f"[MS_Leilao] iniciado: {evt_start}")
                    ativos[l["id"]] = l["end_at"]
            # Finaliza leilões cujo horário de fim chegou
            ended = [aid for aid, end_at in ativos.items() if now >= end_at]
            for aid in ended:
                evt_end = {"event": "leilao.finalizado", "id": aid, "fim": ativos[aid].isoformat()}
                publish(ch, "leilao.finalizado", evt_end)
                print(f"[MS_Leilao] finalizado: {evt_end}")
                finalizados.add(aid)
                del ativos[aid]
            time.sleep(0.2)
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