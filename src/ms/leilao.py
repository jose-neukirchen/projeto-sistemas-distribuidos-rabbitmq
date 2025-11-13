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

import os
import time
import json
import pika
import threading
from datetime import datetime, timezone
from flask import Flask, request, jsonify

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "leilao.events")
LEILAO_PORT = int(os.getenv("LEILAO_PORT", "5000"))

START_STAGGER_SEC = 20   # intervalo entre inícios
DURATION_SEC = 120       # duração de cada leilão
START_DELAY_SEC = 15  # atraso inicial

MAX_ACTIVE = 2
MAX_TOTAL = 10

# Publica em leilao.iniciado e leilao.finalizado

# Estado global dos leilões
leiloes_db = []  # Lista de todos os leilões
leiloes_ativos = {}  # auction_id -> end_time
leiloes_finalizados = set()
best_bids = {}  # auction_id -> (user_id, value)
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


@app.route('/leiloes/<int:auction_id>/lance', methods=['POST'])
def atualizar_lance(auction_id):
    """Atualizar o melhor lance de um leilão (chamado pelo MS Lance)"""
    try:
        data = request.json
        user_id = data.get("user_id")
        value = float(data.get("value"))
        
        with leiloes_lock:
            if auction_id not in leiloes_ativos:
                return jsonify({"error": "Leilão não está ativo"}), 404
            
            best_bids[auction_id] = (user_id, value)
        
        return jsonify({"message": "Lance atualizado"}), 200
        
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
                    # Busca o melhor lance se disponível, senão valor inicial
                    aid = leilao["id"]
                    valor_atual = leilao["valor_inicial"]
                    if aid in best_bids:
                        _, valor_atual = best_bids[aid]
                    
                    ativos.append({
                        "id": aid,
                        "nome": leilao["nome"],
                        "descricao": leilao["descricao"],
                        "valor_atual": valor_atual,
                        "inicio": leilao["start_at"].isoformat(),
                        "fim": leilao["end_at"].isoformat(),
                        "status": "ativo"
                    })
        
        return jsonify(ativos), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def manage_auctions():
    """Thread para gerenciar ciclo de vida dos leilões"""
    if START_DELAY_SEC > 0:
        print(f"[MS_Leilao] aguardando {START_DELAY_SEC}s antes de iniciar (startup delay)")
        time.sleep(START_DELAY_SEC)

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
                        try:
                            conn = pika.BlockingConnection(conn_params())
                            ch = conn.channel()
                            declare_basics(ch)
                            publish(ch, "leilao.iniciado", evt_start)
                            conn.close()
                            print(f"[MS_Leilao] iniciado: leilão {aid}")
                            leiloes_ativos[aid] = leilao["end_at"]
                            leilao["status"] = "ativo"
                        except Exception as e:
                            print(f"[MS_Leilao] Erro ao publicar leilao_iniciado para {aid}: {e}")
                
                # Finaliza leilões cujo horário de fim chegou
                ended = [aid for aid, end_at in list(leiloes_ativos.items()) if now >= end_at]
                for aid in ended:
                    evt_end = {"event": "leilao.finalizado", "id": aid, "fim": leiloes_ativos[aid].isoformat()}
                    # Cria nova conexão para cada publicação (thread-safe)
                    try:
                        conn = pika.BlockingConnection(conn_params())
                        ch = conn.channel()
                        declare_basics(ch)
                        publish(ch, "leilao.finalizado", evt_end)
                        conn.close()
                        print(f"[MS_Leilao] finalizado: leilão {aid}")
                        leiloes_finalizados.add(aid)
                        del leiloes_ativos[aid]
                        # Atualiza status no banco
                        for leilao in leiloes_db:
                            if leilao["id"] == aid:
                                leilao["status"] = "finalizado"
                                break
                    except Exception as e:
                        print(f"[MS_Leilao] Erro ao publicar leilao_finalizado para {aid}: {e}")
            
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        pass


def main():
    # Inicia thread para gerenciar leilões
    auction_thread = threading.Thread(target=manage_auctions, daemon=True)
    auction_thread.start()
    
    print(f"[MS_Leilao] Iniciando servidor REST na porta {LEILAO_PORT}")
    app.run(host='0.0.0.0', port=LEILAO_PORT, threaded=True)


if __name__ == "__main__":
    main()