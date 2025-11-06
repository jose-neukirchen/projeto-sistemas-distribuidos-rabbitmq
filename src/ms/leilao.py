# MS Leilão (publisher)
# 1. Mantém internamente uma lista de leilões com: ID do leilão, descrição, 
#    valor inicial, data e hora de início e fim, status (ativo, encerrado).
# 2. Expõe API REST para:
#    - Criar leilões
#    - Consultar leilões ativos
# 3. O leilão de um determinado produto deve ser iniciado quando
#    o tempo definido para esse leilão for atingido. Quando um leilão
#    começa, ele publica o evento na fila: leilao_iniciado.
# 4. O leilão de um determinado produto deve ser finalizado
#    quando o tempo definido para esse leilão expirar. Quando um leilão
#    termina, ele publica o evento na fila: leilao_finalizado.

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
import threading
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "leilao.events")
LEILAO_PORT = int(os.getenv("LEILAO_PORT", "5000"))

START_STAGGER_SEC = 20   # intervalo entre inícios
DURATION_SEC = 30       # duração de cada leilão
START_DELAY_SEC = 15  # atraso inicial

MAX_ACTIVE = 2
MAX_TOTAL = 10

# Publica em leilao.iniciado e leilao.finalizado

# Estado global dos leilões
leiloes_db = []  # Lista de todos os leilões
leiloes_ativos = {}  # auction_id -> end_time
leiloes_finalizados = set()
leiloes_lock = threading.Lock()
next_auction_id = 1

app = Flask(__name__)

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


# REST API Endpoints

@app.route('/health', methods=['GET'])
def health():
    """Endpoint de health check"""
    return jsonify({"status": "healthy"}), 200


@app.route('/leiloes', methods=['POST'])
def criar_leilao():
    """Criar um novo leilão"""
    global next_auction_id
    
    try:
        data = request.json
        nome = data.get("nome")
        descricao = data.get("descricao")
        valor_inicial = float(data.get("valor_inicial", 0))
        inicio = data.get("inicio")  # ISO format string
        fim = data.get("fim")  # ISO format string
        
        if not nome or not descricao or not inicio or not fim:
            return jsonify({"error": "Campos obrigatórios: nome, descricao, inicio, fim"}), 400
        
        # Parse dates
        try:
            start_at = datetime.fromisoformat(inicio.replace('Z', '+00:00'))
            end_at = datetime.fromisoformat(fim.replace('Z', '+00:00'))
        except:
            return jsonify({"error": "Formato de data inválido. Use ISO 8601"}), 400
        
        if end_at <= start_at:
            return jsonify({"error": "Data de fim deve ser posterior à data de início"}), 400
        
        with leiloes_lock:
            auction_id = next_auction_id
            next_auction_id += 1
            
            leilao = {
                "id": auction_id,
                "nome": nome,
                "descricao": descricao,
                "valor_inicial": valor_inicial,
                "start_at": start_at,
                "end_at": end_at,
                "status": "agendado"
            }
            
            leiloes_db.append(leilao)
        
        print(f"[MS_Leilao] Leilão criado: {auction_id} - {nome}")
        
        return jsonify({
            "id": auction_id,
            "nome": nome,
            "descricao": descricao,
            "valor_inicial": valor_inicial,
            "inicio": start_at.isoformat(),
            "fim": end_at.isoformat(),
            "status": "agendado"
        }), 201
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/leiloes', methods=['GET'])
def consultar_leiloes():
    """Consultar leilões ativos"""
    try:
        with leiloes_lock:
            ativos = []
            for leilao in leiloes_db:
                if leilao["id"] in leiloes_ativos:
                    # Busca o último lance (valor atual) se disponível
                    # Por enquanto, retorna valor inicial
                    ativos.append({
                        "id": leilao["id"],
                        "nome": leilao["nome"],
                        "descricao": leilao["descricao"],
                        "valor_atual": leilao["valor_inicial"],
                        "inicio": leilao["start_at"].isoformat(),
                        "fim": leilao["end_at"].isoformat(),
                        "status": "ativo"
                    })
        
        return jsonify(ativos), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def manage_auctions():
    """Thread para gerenciar ciclo de vida dos leilões"""
    conn = pika.BlockingConnection(conn_params())
    ch = conn.channel()
    declare_basics(ch)

    if START_DELAY_SEC > 0:
        print(f"[MS_Leilao] aguardando {START_DELAY_SEC}s antes de iniciar (startup delay)")
        time.sleep(START_DELAY_SEC)

    # Carrega leilões hardcoded iniciais
    now = datetime.now(timezone.utc)
    with leiloes_lock:
        global next_auction_id
        for idx, leilao in enumerate(lista_leiloes):
            aid = next_auction_id
            next_auction_id += 1
            desc = leilao["descricao"]
            start_at = now + timedelta(seconds=idx * START_STAGGER_SEC)
            end_at = start_at + timedelta(seconds=DURATION_SEC)
            leiloes_db.append({
                "id": aid,
                "nome": f"Produto {aid}",
                "descricao": desc,
                "valor_inicial": 100.0,
                "start_at": start_at,
                "end_at": end_at,
                "status": "agendado"
            })

    print(f"[MS_Leilao] {len(leiloes_db)} leilões agendados")

    try:
        while True:
            now = datetime.now(timezone.utc)
            
            with leiloes_lock:
                # Inicia leilões cujo horário chegou
                for leilao in leiloes_db:
                    aid = leilao["id"]
                    if aid not in leiloes_ativos and aid not in leiloes_finalizados and now >= leilao["start_at"]:
                        evt_start = {
                            "event": "leilao.iniciado",
                            "id": aid,
                            "nome": leilao["nome"],
                            "descricao": leilao["descricao"],
                            "valor_inicial": leilao["valor_inicial"],
                            "inicio": leilao["start_at"].isoformat(),
                            "fim": leilao["end_at"].isoformat()
                        }
                        publish(ch, "leilao.iniciado", evt_start)
                        print(f"[MS_Leilao] iniciado: leilão {aid}")
                        leiloes_ativos[aid] = leilao["end_at"]
                        leilao["status"] = "ativo"
                
                # Finaliza leilões cujo horário de fim chegou
                ended = [aid for aid, end_at in list(leiloes_ativos.items()) if now >= end_at]
                for aid in ended:
                    evt_end = {"event": "leilao.finalizado", "id": aid, "fim": leiloes_ativos[aid].isoformat()}
                    publish(ch, "leilao.finalizado", evt_end)
                    print(f"[MS_Leilao] finalizado: leilão {aid}")
                    leiloes_finalizados.add(aid)
                    del leiloes_ativos[aid]
                    # Atualiza status no banco
                    for leilao in leiloes_db:
                        if leilao["id"] == aid:
                            leilao["status"] = "finalizado"
                            break
            
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()


def main():
    # Inicia thread para gerenciar leilões
    auction_thread = threading.Thread(target=manage_auctions, daemon=True)
    auction_thread.start()
    
    print(f"[MS_Leilao] Iniciando servidor REST na porta {LEILAO_PORT}")
    app.run(host='0.0.0.0', port=LEILAO_PORT, threaded=True)


if __name__ == "__main__":
    main()