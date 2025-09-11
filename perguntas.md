# Perguntas e Respostas sobre o Projeto

1. Onde os eventos de início e fim de leilão são publicados?
Resposta: Em ms/leilao.py, função publish() chamada dentro do loop principal (main), com routing keys "leilao.iniciado" e "leilao.finalizado".

2. Onde o serviço que valida lances consome os eventos de início de leilão?
Resposta: Em ms/lance.py na função main(), registrando consumidor para a fila leilao_iniciado.ms_lance via on_leilao_iniciado.

3. Como o MS Lance identifica que um leilão está ativo?
Resposta: Ao receber on_leilao_iniciado, adiciona o ID no set active_auctions.

4. Onde é feita a verificação de assinatura digital de um lance?
Resposta: Em ms/lance.py na função verify_signature(), chamada dentro de on_lance_realizado.

5. Qual arquivo publica o evento lance.validado e em que condição?
Resposta: ms/lance.py em on_lance_realizado quando o lance tem valor maior que o anterior (ou é o primeiro) e assinatura válida.

6. Onde o evento leilao.vencedor é publicado?
Resposta: ms/lance.py dentro de on_leilao_finalizado quando existe um winner em best_bids.

7. Como os clientes descobrem novos leilões?
Resposta: Consumindo a fila leilao_iniciado.<CLIENT_ID> em clients/client.py na função consumer_leiloes (callback on_started).

8. Onde os clientes assinam digitalmente os lances?
Resposta: Em clients/client.py na função sign_payload() usando a chave privada carregada por load_private_key().

9. Em que fila os clientes publicam lances?
Resposta: Publicam na exchange leilao.events com routing key "lance.realizado" (fila lance_realizado declarada em ms/lance.py e pelos próprios clientes antes do publish).

10. Como o cliente começa a ouvir notificações específicas de um leilão?
Resposta: Ao preparar para dar lance, inicia uma thread consume_notifications que cria/binda fila leilao_<id> (clients/client.py ensure_leilao_queue()).

11. Onde as notificações por leilão (lance.validado e leilao.vencedor) são redistribuídas?
Resposta: Em ms/notificacao.py na função on_event, republicando para routing key leilao.<auction_id>.

12. Como as filas específicas de notificação por leilão são criadas?
Resposta: Dinamicamente em ms/notificacao.py via ensure_leilao_queue() quando chega um evento de validação ou vencedor.

13. Qual componente decide o vencedor de um leilão?
Resposta: MS Lance em on_leilao_finalizado consultando best_bids.

14. Onde os valores dos lances são gerados nos clientes?
Resposta: Em clients/client.py dentro de publisher_loop, usando uniform(0, 1000) e garantindo não regressão (comparação com LAST_BIDS).

15. Como o cliente sabe se venceu ou perdeu um leilão?
Resposta: Ao receber evento leilao.vencedor em consume_notifications imprime mensagem de vitória ou derrota comparando user_id.

16. Como é garantido que somente lances de leilões ativos sejam aceitos?
Resposta: ms/lance.py verifica se auction_id está em active_auctions antes de registrar o lance (condição em on_lance_realizado).

17. Onde as chaves públicas dos clientes são carregadas?
Resposta: ms/lance.py função load_pubkey(), lê arquivos {client_id}_public.pem no diretório KEYS_DIR.

18. Qual é o papel da exchange leilao.events?
Resposta: Centralizar roteamento de todos os eventos (início, finalização, lances, validações e vencedores) via routing keys específicas.

19. Como o serviço de leilão define os horários de cada leilão?
Resposta: Em ms/leilao.py (versão atual) ajusta dinamicamente datas com base no horário atual e deslocamentos START_STAGGER_SEC e DURATION_SEC antes de iniciar o loop.

20. Por que utilizar filas dedicadas por cliente para leilao_iniciado?
Resposta: Para garantir broadcast (cada cliente recebe todos os eventos) sem competição em uma única fila, implementando padrão fan-out usando exchange direct + múltiplas filas.
