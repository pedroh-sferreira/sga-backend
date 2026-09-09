const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();

const PORT = process.env.PORT || 3000;

// ============================================================
// CONFIGURAÇÕES
// ============================================================

const MAX_HISTORICO_POR_EXECUCAO = 500;

const CLIENTES_SSE = new Map();
const HISTORICO = new Map();


// ============================================================
// MIDDLEWARE
// ============================================================

app.use(
    cors({
        origin: true,
        credentials: true
    })
);

app.use(
    express.json({
        limit: "1mb"
    })
);


// ============================================================
// FUNÇÕES AUXILIARES
// ============================================================

function gerarIdEvento() {
    return crypto.randomUUID();
}


function normalizarNivel(level) {
    const niveis = [
        "info",
        "success",
        "warn",
        "error"
    ];

    if (niveis.includes(level)) {
        return level;
    }

    return "info";
}


function criarEvento(dados) {
    return {
        id: gerarIdEvento(),

        execucao_id:
            dados.execucao_id || null,

        usuario:
            dados.usuario || null,

        detentora:
            dados.detentora || null,

        message:
            String(dados.message || ""),

        level:
            normalizarNivel(dados.level),

        event:
            dados.event || "log",

        timestamp:
            dados.timestamp ||
            new Date().toISOString()
    };
}


// ============================================================
// HEALTH CHECK
// ============================================================

app.get("/", (req, res) => {

    res.json({
        status: "online",
        service: "SGA Backend",
        timestamp: new Date().toISOString()
    });

});


app.get("/health", (req, res) => {

    res.json({
        status: "ok",
        timestamp: new Date().toISOString(),
        clientes_sse: contarClientes(),
        execucoes_ativas: HISTORICO.size
    });

});


// ============================================================
// POST /api/logs
// ============================================================
//
// Recebe os logs enviados pelo monitor.py
//
// Exemplo:
//
// {
//   "execucao_id": "ATC-20260909-143201-1234",
//   "usuario": "Pedro Ferreira",
//   "detentora": "atc",
//   "message": "[LOGIN] Login realizado",
//   "level": "info"
// }
//
// ============================================================

app.post("/api/logs", (req, res) => {

    try {

        const {
            message,
            level,
            execucao_id,
            usuario,
            detentora,
            event,
            timestamp
        } = req.body;


        // ----------------------------------------------------
        // VALIDAÇÃO
        // ----------------------------------------------------

        if (!message) {

            return res.status(400).json({
                success: false,
                error: "Mensagem não informada."
            });

        }


        if (!execucao_id) {

            return res.status(400).json({
                success: false,
                error: "execucao_id não informado."
            });

        }


        // ----------------------------------------------------
        // CRIA EVENTO
        // ----------------------------------------------------

        const evento = criarEvento({
            message,
            level,
            execucao_id,
            usuario,
            detentora,
            event,
            timestamp
        });


        // ----------------------------------------------------
        // HISTÓRICO
        // ----------------------------------------------------

        if (!HISTORICO.has(execucao_id)) {

            HISTORICO.set(
                execucao_id,
                []
            );

        }


        const historico =
            HISTORICO.get(execucao_id);


        historico.push(evento);


        // Limita o histórico
        if (
            historico.length >
            MAX_HISTORICO_POR_EXECUCAO
        ) {

            historico.splice(
                0,
                historico.length -
                MAX_HISTORICO_POR_EXECUCAO
            );

        }


        // ----------------------------------------------------
        // LOG NO CONSOLE DO RENDER
        // ----------------------------------------------------

        console.log(
            `[${evento.level.toUpperCase()}]`,
            `[${evento.detentora || "?"}]`,
            `[${evento.usuario || "?"}]`,
            evento.message
        );


        // ----------------------------------------------------
        // ENVIA PARA OS CLIENTES SSE
        // ----------------------------------------------------

        enviarParaExecucao(
            execucao_id,
            evento
        );


        // ----------------------------------------------------
        // RESPOSTA
        // ----------------------------------------------------

        return res.json({
            success: true,
            event_id: evento.id
        });

    } catch (erro) {

        console.error(
            "Erro ao processar log:",
            erro
        );

        return res.status(500).json({
            success: false,
            error: "Erro interno ao processar log."
        });

    }

});


// ============================================================
// GET /api/logs/stream
// ============================================================
//
// Conexão SSE.
//
// Exemplo:
//
// /api/logs/stream?execucao_id=ATC-20260909-143201-1234
//
// ============================================================

app.get(
    "/api/logs/stream",
    (req, res) => {

        const execucaoId =
            req.query.execucao_id;


        if (!execucaoId) {

            return res.status(400).json({
                success: false,
                error: "execucao_id não informado."
            });

        }


        // ----------------------------------------------------
        // HEADERS SSE
        // ----------------------------------------------------

        res.setHeader(
            "Content-Type",
            "text/event-stream"
        );

        res.setHeader(
            "Cache-Control",
            "no-cache, no-transform"
        );

        res.setHeader(
            "Connection",
            "keep-alive"
        );

        res.setHeader(
            "X-Accel-Buffering",
            "no"
        );


        if (res.flushHeaders) {
            res.flushHeaders();
        }


        // ----------------------------------------------------
        // REGISTRA CLIENTE
        // ----------------------------------------------------

        if (!CLIENTES_SSE.has(execucaoId)) {

            CLIENTES_SSE.set(
                execucaoId,
                new Set()
            );

        }


        const clientes =
            CLIENTES_SSE.get(execucaoId);


        clientes.add(res);


        console.log(
            `[SSE] Cliente conectado: ${execucaoId}`
        );


        // ----------------------------------------------------
        // ENVIA HISTÓRICO
        // ----------------------------------------------------

        const historico =
            HISTORICO.get(execucaoId) || [];


        for (const evento of historico) {

            enviarEventoSSE(
                res,
                evento
            );

        }


        // ----------------------------------------------------
        // EVENTO DE CONEXÃO
        // ----------------------------------------------------

        enviarEventoSSE(
            res,
            {
                id: gerarIdEvento(),
                event: "connected",
                execucao_id: execucaoId,
                message: "Conectado ao log da execução.",
                level: "info",
                timestamp: new Date().toISOString()
            }
        );


        // ----------------------------------------------------
        // ENCERRAMENTO
        // ----------------------------------------------------

        req.on(
            "close",
            () => {

                clientes.delete(res);

                console.log(
                    `[SSE] Cliente desconectado: ${execucaoId}`
                );


                if (
                    clientes.size === 0
                ) {

                    CLIENTES_SSE.delete(
                        execucaoId
                    );

                }

            }
        );

    }
);


// ============================================================
// ENVIA EVENTO SSE
// ============================================================

function enviarEventoSSE(
    response,
    evento
) {

    try {

        response.write(
            `data: ${JSON.stringify(evento)}\n\n`
        );

    } catch (erro) {

        console.error(
            "Erro ao enviar SSE:",
            erro
        );

    }

}


// ============================================================
// ENVIA PARA UMA EXECUÇÃO
// ============================================================

function enviarParaExecucao(
    execucaoId,
    evento
) {

    const clientes =
        CLIENTES_SSE.get(execucaoId);


    if (!clientes) {
        return;
    }


    for (const cliente of clientes) {

        enviarEventoSSE(
            cliente,
            evento
        );

    }

}


// ============================================================
// CONTADOR DE CLIENTES
// ============================================================

function contarClientes() {

    let total = 0;

    for (
        const clientes
        of CLIENTES_SSE.values()
    ) {

        total += clientes.size;

    }

    return total;

}


// ============================================================
// HEARTBEAT SSE
// ============================================================
//
// Render/proxies podem encerrar conexões ociosas.
// Este heartbeat mantém a conexão viva.
//

setInterval(
    () => {

        for (
            const clientes
            of CLIENTES_SSE.values()
        ) {

            for (
                const cliente
                of clientes
            ) {

                try {

                    cliente.write(
                        `: heartbeat ${Date.now()}\n\n`
                    );

                } catch (erro) {

                    // Cliente será removido pelo close.
                }

            }

        }

    },
    25000
);


// ============================================================
// TRATAMENTO DE ERROS
// ============================================================

process.on(
    "uncaughtException",
    (erro) => {

        console.error(
            "UNCAUGHT EXCEPTION:",
            erro
        );

    }
);


process.on(
    "unhandledRejection",
    (erro) => {

        console.error(
            "UNHANDLED REJECTION:",
            erro
        );

    }
);


// ============================================================
// INICIAR SERVIDOR
// ============================================================

app.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log(
            "============================================"
        );

        console.log(
            "SGA BACKEND INICIADO"
        );

        console.log(
            `Porta: ${PORT}`
        );

        console.log(
            "============================================"
        );

    }
);