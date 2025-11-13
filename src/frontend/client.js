// Cliente Frontend (Node.js)
// 1. Envia requisições REST para:
//    - Criar leilões
//    - Consultar leilões ativos
//    - Efetuar lances
//    - Registrar interesse em notificações
//    - Cancelar interesse em notificações
// 2. Recebe notificações via SSE, incluindo:
//    - Novos lances válidos em leilões de interesse
//    - Encerramento de leilões e anúncio de vencedor
//    - Geração de link de pagamento
//    - Resultado do pagamento (aprovado ou recusado)
// 3. O cliente vencedor do leilão recebe via SSE o link de pagamento

const axios = require('axios');
const EventSource = require('eventsource');
const readline = require('readline-sync');
const chalk = require('chalk');

const API_GATEWAY_URL = process.env.API_GATEWAY_URL || 'http://localhost:8000';
const CLIENT_ID = process.env.CLIENT_ID || `client_${Date.now()}`;

let eventSource = null;
let registeredAuctions = new Set();
let notificationsBuffer = []; // Buffer para armazenar notificações

// Configura EventSource para receber notificações SSE
function connectSSE() {
    const sseUrl = `${API_GATEWAY_URL}/api/sse/${CLIENT_ID}`;
    console.log(chalk.blue(`\n[SSE] Conectando ao servidor de eventos: ${sseUrl}`));
    
    eventSource = new EventSource(sseUrl);
    
    eventSource.onopen = () => {
        console.log(chalk.green('[SSE] Conexão estabelecida com sucesso\n'));
    };
    
    eventSource.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            console.log(chalk.gray(`[DEBUG SSE] Mensagem recebida: ${JSON.stringify(data).substring(0, 100)}`));
            handleNotification(data);
        } catch (e) {
            console.log(chalk.red(`[DEBUG SSE] Erro ao processar mensagem: ${e.message}`));
            console.log(chalk.red(`[DEBUG SSE] Dados brutos: ${event.data}`));
        }
    };
    
    eventSource.onerror = (error) => {
        console.log(chalk.red('[SSE] Erro na conexão. Tentando reconectar...'));
        console.log(chalk.red(`[DEBUG SSE] Erro detalhes: ${JSON.stringify(error)}`));
    };
}

// Processa notificações recebidas via SSE
function handleNotification(notification) {
    const eventType = notification.event;
    const data = notification.data;
    
    // Adiciona timestamp e armazena no buffer
    const timestampedNotification = {
        timestamp: new Date(),
        event: eventType,
        data: data
    };
    notificationsBuffer.push(timestampedNotification);
}

// Exibe uma notificação formatada
function displayNotification(notification) {
    const eventType = notification.event;
    const data = notification.data;
    const timestamp = notification.timestamp.toLocaleString();
    
    console.log(chalk.yellow('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'));
    console.log(chalk.yellow.bold('  🔔 NOTIFICAÇÃO RECEBIDA'));
    console.log(chalk.gray(`  Horário: ${timestamp}`));
    console.log(chalk.yellow('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'));
    
    switch (eventType) {
        case 'lance_validado':
            console.log(chalk.green('✓ LANCE VÁLIDO'));
            console.log(`  Leilão: ${data.auction_id}`);
            console.log(`  Usuário: ${data.user_id}`);
            console.log(`  Valor: R$ ${data.value.toFixed(2)}`);
            if (data.previous_value) {
                console.log(`  Valor anterior: R$ ${data.previous_value.toFixed(2)}`);
            }
            break;
            
        case 'lance_invalidado':
            console.log(chalk.red('✗ LANCE INVÁLIDO'));
            console.log(`  Leilão: ${data.auction_id}`);
            console.log(`  Usuário: ${data.user_id}`);
            console.log(`  Valor tentado: R$ ${data.value.toFixed(2)}`);
            console.log(`  Motivo: ${data.reason}`);
            break;
            
        case 'leilao_vencedor':
            console.log(chalk.green.bold('★ LEILÃO ENCERRADO - VENCEDOR ANUNCIADO'));
            console.log(`  Leilão: ${data.auction_id}`);
            console.log(`  Vencedor: ${data.winner_id}`);
            console.log(`  Valor final: R$ ${data.value.toFixed(2)}`);
            
            if (data.winner_id === CLIENT_ID) {
                console.log(chalk.green.bold('\n  🎉 PARABÉNS! VOCÊ VENCEU ESTE LEILÃO!'));
                console.log(chalk.green('     Aguarde o link de pagamento...'));
            }
            break;
            
        case 'link_pagamento':
            console.log(chalk.cyan.bold('💳 LINK DE PAGAMENTO GERADO'));
            console.log(`  Leilão: ${data.auction_id}`);
            console.log(`  Valor: R$ ${data.amount.toFixed(2)}`);
            console.log(`  Transaction ID: ${data.transaction_id}`);
            console.log(chalk.cyan.bold(`\n  Link: ${data.payment_link}`));
            console.log(chalk.cyan('\n  Acesse o link acima para concluir o pagamento.'));
            break;
            
        case 'status_pagamento':
            if (data.status === 'approved') {
                console.log(chalk.green.bold('✓ PAGAMENTO APROVADO'));
                console.log(chalk.green('  Sua compra foi confirmada com sucesso!'));
            } else {
                console.log(chalk.red.bold('✗ PAGAMENTO RECUSADO'));
                console.log(chalk.red('  O pagamento não foi aprovado.'));
            }
            console.log(`  Leilão: ${data.auction_id}`);
            console.log(`  Transaction ID: ${data.transaction_id}`);
            console.log(`  Valor: R$ ${data.amount.toFixed(2)}`);
            break;
            
        default:
            console.log(`Evento desconhecido: ${eventType}`);
            console.log(JSON.stringify(data, null, 2));
    }
    
    console.log(chalk.yellow('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'));
}

// API Functions

async function criarLeilao() {
    console.log(chalk.blue('\n=== CRIAR NOVO LEILÃO ===\n'));
    
    const nome = readline.question('Nome do produto: ');
    const descricao = readline.question('Descrição: ');
    const valorInicial = parseFloat(readline.question('Valor inicial (R$): '));
    
    const agora = new Date();
    const inicioMinutos = parseInt(readline.question('Início em quantos minutos? '));
    const duracaoMinutos = parseInt(readline.question('Duração em minutos? '));
    
    const inicio = new Date(agora.getTime() + inicioMinutos * 60000);
    const fim = new Date(inicio.getTime() + duracaoMinutos * 60000);
    
    try {
        const response = await axios.post(`${API_GATEWAY_URL}/api/leiloes`, {
            nome,
            descricao,
            valor_inicial: valorInicial,
            inicio: inicio.toISOString(),
            fim: fim.toISOString()
        });
        
        console.log(chalk.green('\n✓ Leilão criado com sucesso!'));
        console.log(`  ID: ${response.data.id}`);
        console.log(`  Nome: ${response.data.nome}`);
        console.log(`  Valor inicial: R$ ${response.data.valor_inicial.toFixed(2)}`);
        console.log(`  Início: ${new Date(response.data.inicio).toLocaleString()}`);
        console.log(`  Fim: ${new Date(response.data.fim).toLocaleString()}`);
    } catch (error) {
        console.log(chalk.red(`\n✗ Erro ao criar leilão: ${error.response?.data?.error || error.message}`));
    }
}

async function consultarLeiloes() {
    console.log(chalk.blue('\n=== LEILÕES ATIVOS ===\n'));
    
    try {
        const response = await axios.get(`${API_GATEWAY_URL}/api/leiloes`);
        const leiloes = response.data;
        
        if (leiloes.length === 0) {
            console.log(chalk.yellow('Nenhum leilão ativo no momento.'));
            return;
        }
        
        leiloes.forEach((leilao, index) => {
            console.log(chalk.cyan(`\n[${index + 1}] Leilão #${leilao.id}`));
            console.log(`    Nome: ${leilao.nome}`);
            console.log(`    Descrição: ${leilao.descricao}`);
            console.log(`    Valor atual: R$ ${leilao.valor_atual.toFixed(2)}`);
            console.log(`    Término: ${new Date(leilao.fim).toLocaleString()}`);
            
            if (registeredAuctions.has(leilao.id)) {
                console.log(chalk.green('    ✓ Você está recebendo notificações deste leilão'));
            }
        });
    } catch (error) {
        console.log(chalk.red(`\n✗ Erro ao consultar leilões: ${error.response?.data?.error || error.message}`));
    }
}

async function efetuarLance() {
    console.log(chalk.blue('\n=== EFETUAR LANCE ===\n'));
    
    const auctionId = parseInt(readline.question('ID do leilão: '));
    const value = parseFloat(readline.question('Valor do lance (R$): '));
    
    try {
        const response = await axios.post(`${API_GATEWAY_URL}/api/lances`, {
            auction_id: auctionId,
            user_id: CLIENT_ID,
            value
        });
        
        console.log(chalk.green('\n✓ Lance enviado com sucesso!'));
        console.log(`  Leilão: ${response.data.auction_id}`);
        console.log(`  Valor: R$ ${response.data.value.toFixed(2)}`);
        
        // Registra interesse automaticamente ao dar lance
        if (!registeredAuctions.has(auctionId)) {
            await registrarInteresse(auctionId, true);
        }
    } catch (error) {
        console.log(chalk.red(`\n✗ Erro ao efetuar lance: ${error.response?.data?.error || error.message}`));
    }
}

async function registrarInteresse(auctionId = null, auto = false) {
    if (!auto) {
        console.log(chalk.blue('\n=== REGISTRAR INTERESSE ===\n'));
        auctionId = parseInt(readline.question('ID do leilão: '));
    }
    
    try {
        await axios.post(`${API_GATEWAY_URL}/api/interesses`, {
            client_id: CLIENT_ID,
            auction_id: auctionId
        });
        
        registeredAuctions.add(auctionId);
        
        if (!auto) {
            console.log(chalk.green(`\n✓ Interesse registrado no leilão ${auctionId}`));
            console.log('  Você receberá notificações sobre este leilão.');
        }
    } catch (error) {
        if (!auto) {
            console.log(chalk.red(`\n✗ Erro ao registrar interesse: ${error.response?.data?.error || error.message}`));
        }
    }
}

async function cancelarInteresse() {
    console.log(chalk.blue('\n=== CANCELAR INTERESSE ===\n'));
    
    if (registeredAuctions.size === 0) {
        console.log(chalk.yellow('Você não está registrado em nenhum leilão.'));
        return;
    }
    
    console.log('Leilões registrados:', Array.from(registeredAuctions).join(', '));
    const auctionId = parseInt(readline.question('ID do leilão: '));
    
    try {
        await axios.delete(`${API_GATEWAY_URL}/api/interesses`, {
            data: {
                client_id: CLIENT_ID,
                auction_id: auctionId
            }
        });
        
        registeredAuctions.delete(auctionId);
        console.log(chalk.green(`\n✓ Interesse cancelado no leilão ${auctionId}`));
    } catch (error) {
        console.log(chalk.red(`\n✗ Erro ao cancelar interesse: ${error.response?.data?.error || error.message}`));
    }
}

async function consultarNotificacoes() {
    console.log(chalk.blue('\n=== NOTIFICAÇÕES RECEBIDAS ===\n'));
    
    // Aguarda 100ms para permitir que eventos SSE pendentes sejam processados
    // O readline-sync é síncrono e bloqueia o event loop, impedindo que
    // mensagens SSE sejam processadas enquanto aguarda input do usuário.
    // Este await libera o event loop para processar eventos pendentes.
    await new Promise(resolve => setTimeout(resolve, 100));
    
    if (notificationsBuffer.length === 0) {
        console.log(chalk.yellow('Nenhuma notificação recebida ainda.'));
        return;
    }
    
    console.log(chalk.cyan(`Total de notificações: ${notificationsBuffer.length}\n`));
    
    // Exibe todas as notificações em ordem cronológica
    notificationsBuffer.forEach((notification, index) => {
        console.log(chalk.gray(`\n[Notificação ${index + 1}/${notificationsBuffer.length}]`));
        displayNotification(notification);
    });
    
    console.log(chalk.cyan(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`));
    console.log(chalk.cyan(`  Fim das notificações (${notificationsBuffer.length} total)`));
    console.log(chalk.cyan(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`));
}

// Menu principal
function showMenu() {
    const notifCount = notificationsBuffer.length;
    const notifIndicator = notifCount > 0 ? chalk.yellow.bold(` [${notifCount} nova${notifCount > 1 ? 's' : ''}]`) : '';
    
    console.log(chalk.cyan('\n╔════════════════════════════════════════╗'));
    console.log(chalk.cyan('║      SISTEMA DE LEILÃO - CLIENTE       ║'));
    console.log(chalk.cyan('╚════════════════════════════════════════╝'));
    console.log(chalk.white(`\nID do Cliente: ${chalk.bold(CLIENT_ID)}\n`));
    console.log('1. Criar novo leilão');
    console.log('2. Consultar leilões ativos');
    console.log('3. Efetuar lance');
    console.log('4. Registrar interesse em notificações');
    console.log('5. Cancelar interesse em notificações');
    console.log(`6. Consultar notificações recebidas${notifIndicator}`);
    console.log('0. Sair\n');
}

async function main() {
    console.log(chalk.green.bold('\n🎯 Bem-vindo ao Sistema de Leilão!\n'));
    
    // Conecta ao SSE
    connectSSE();
    
    // Aguarda um pouco para estabelecer conexão
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    let running = true;
    
    while (running) {
        showMenu();
        const option = readline.question('Escolha uma opção: ');
        
        switch (option) {
            case '1':
                await criarLeilao();
                break;
            case '2':
                await consultarLeiloes();
                break;
            case '3':
                await efetuarLance();
                break;
            case '4':
                await registrarInteresse();
                break;
            case '5':
                await cancelarInteresse();
                break;
            case '6':
                await consultarNotificacoes();
                break;
            case '0':
                running = false;
                console.log(chalk.green('\nAté logo!'));
                break;
            default:
                console.log(chalk.red('\nOpção inválida!'));
        }
        
        if (running && option !== '0') {
            readline.question(chalk.gray('\nPressione ENTER para continuar...'));
        }
    }
    
    // Fecha conexão SSE
    if (eventSource) {
        eventSource.close();
    }
    
    process.exit(0);
}

// Trata sinais de interrupção
process.on('SIGINT', () => {
    console.log(chalk.yellow('\n\nEncerrando cliente...'));
    if (eventSource) {
        eventSource.close();
    }
    process.exit(0);
});

// Inicia o cliente
main().catch(error => {
    console.error(chalk.red('Erro fatal:'), error);
    process.exit(1);
});
