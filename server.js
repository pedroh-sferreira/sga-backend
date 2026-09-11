const express = require("express");
const cors = require("cors");
const crypto = require("crypto");

const app = express();

const PORT = process.env.PORT || 3000;

// ============================================================
// CONFIGURAÇÕES
// ============================================================

const MAX_HISTORICO_POR_EXECUCAO = 500;
const MAX_EXECUCOES = 1000;

// ============================================================
// MEMÓRIA
// ============================================================

// execucao_id -> execução
const EXECUCOES = new Map();

// execucao_id -> array de eventos
const HISTORICO = new Map();

// execucao_id -> Set de conexões SSE
const CLIENTES_SSE = new Map();

// ============================================================
// MIDDLEWARE
// ============================================================

app.use(
  cors({
    origin: true,
    credentials: true,
  })
);

app.use(
  express.json({
    limit: "1mb",
  })
);

// ============================================================
// FUNÇÕES AUXILIARES
// ============================================================

function gerarId() {
  return crypto.randomUUID();
}

function normalizarNivel(level) {
  const niveis = [
    "info",
    "success",
    "warn",
    "error",
  ];

  return niveis.includes(level)
    ? level
    : "info";
}

function normalizarStatus(status) {
  const statusValidos = [
    "running",
    "completed",
    "error",
    "cancelled",
  ];

  return statusValidos.includes(status)
    ? status
    : "running";
}

function criarEvento(dados) {
  return {
    id: gerarId(),

    execucao_id:
      dados.execucao_id || null,

    usuario:
      dados.usuario || null,

    detentora:
      dados.detentora || null,

    arquivo:
      dados.arquivo || null,

    protocolo:
      dados.protocolo || null,

    message:
      String(dados.message || ""),

    level:
      normalizarNivel(dados.level),

    event:
      dados.event || "log",

    timestamp:
      dados.timestamp ||
      new Date().toISOString(),
  };
}

// ============================================================
// LIMPEZA DE EXECUÇÕES ANTIGAS
// ============================================================

function limitarExecucoes() {
  if (EXECUCOES.size <= MAX_EXECUCOES) {
    return;
  }

  const execucoes = Array.from(
    EXECUCOES.values()
  );

  execucoes.sort(
    (a, b) =>
      new Date(a.updated_at) -
      new Date(b.updated_at)
  );

  const quantidadeRemover =
    EXECUCOES.size - MAX_EXECUCOES;

  for (
    let i = 0;
    i < quantidadeRemover;
    i++
  ) {
    const execucao =
      execucoes[i];

    EXECUCOES.delete(
      execucao.execucao_id
    );

    HISTORICO.delete(
      execucao.execucao_id
    );

    CLIENTES_SSE.delete(
      execucao.execucao_id
    );
  }
}

// ============================================================
// ROTA PRINCIPAL
// ============================================================

app.get("/", (req, res) => {
  res.json({
    status: "online",
    service: "SGA Backend",
    version: "2.1.0",
    timestamp:
      new Date().toISOString(),
  });
});

// ============================================================
// HEALTH CHECK
// ============================================================

app.get("/health", (req, res) => {
  res.json({
    status: "ok",

    timestamp:
      new Date().toISOString(),

    clientes_sse:
      contarClientes(),

    execucoes:
      EXECUCOES.size,
  });
});

// ============================================================
// CRIAR / REGISTRAR EXECUÇÃO
// ============================================================

app.post(
  "/api/executions",
  (req, res) => {

    try {

      const {
        execucao_id,
        usuario,
        detentora,
        arquivo,
        protocolo,
      } = req.body;

      if (!execucao_id) {
        return res.status(400).json({
          success: false,
          error:
            "execucao_id não informado.",
        });
      }

      if (!usuario) {
        return res.status(400).json({
          success: false,
          error:
            "usuario não informado.",
        });
      }

      if (!detentora) {
        return res.status(400).json({
          success: false,
          error:
            "detentora não informada.",
        });
      }

      const agora =
        new Date().toISOString();

      let execucao =
        EXECUCOES.get(
          execucao_id
        );

      // ------------------------------------------------------
      // SE JÁ EXISTE
      // ------------------------------------------------------

      if (execucao) {

        execucao.usuario =
          usuario;

        execucao.detentora =
          detentora;

        if (arquivo) {
          execucao.arquivo =
            arquivo;
        }

        if (protocolo) {
          execucao.protocolo =
            protocolo;
        }

        execucao.updated_at =
          agora;

      }

      // ------------------------------------------------------
      // NOVA EXECUÇÃO
      // ------------------------------------------------------

      else {

        execucao = {
          execucao_id,

          usuario,

          detentora,

          arquivo:
            arquivo || null,

          protocolo:
            protocolo || null,

          status: "running",

          started_at: agora,

          updated_at: agora,

          finished_at: null,
        };

        EXECUCOES.set(
          execucao_id,
          execucao
        );

        HISTORICO.set(
          execucao_id,
          []
        );

        limitarExecucoes();

        console.log(
          "============================================"
        );

        console.log(
          "[EXECUÇÃO] Nova execução"
        );

        console.log(
          "ID:",
          execucao_id
        );

        console.log(
          "Usuário:",
          usuario
        );

        console.log(
          "Detentora:",
          detentora
        );

        console.log(
          "Arquivo:",
          arquivo || "-"
        );

        console.log(
          "Protocolo:",
          protocolo || "-"
        );

        console.log(
          "============================================"
        );
      }

      // ------------------------------------------------------
      // EVENTO DE INÍCIO
      // ------------------------------------------------------

      const evento = criarEvento({
        execucao_id,
        usuario,
        detentora,
        arquivo,

        protocolo:
          protocolo ||
          execucao.protocolo,

        message:
          "Execução iniciada.",

        level: "info",

        event:
          "execution_started",
      });

      adicionarEvento(
        execucao_id,
        evento
      );

      return res.json({
        success: true,
        execution: execucao,
      });

    } catch (erro) {

      console.error(
        "Erro ao criar execução:",
        erro
      );

      return res.status(500).json({
        success: false,
        error:
          "Erro interno ao criar execução.",
      });
    }
  }
);

// ============================================================
// LISTAR EXECUÇÕES
// ============================================================

app.get(
  "/api/executions",
  (req, res) => {

    const usuario =
      req.query.usuario;

    const detentora =
      req.query.detentora;

    let execucoes =
      Array.from(
        EXECUCOES.values()
      );

    if (usuario) {
      execucoes =
        execucoes.filter(
          (execucao) =>
            execucao.usuario
              ?.toLowerCase() ===
            usuario.toLowerCase()
        );
    }

    if (detentora) {
      execucoes =
        execucoes.filter(
          (execucao) =>
            execucao.detentora
              ?.toLowerCase() ===
            detentora.toLowerCase()
        );
    }

    execucoes.sort(
      (a, b) =>
        new Date(b.updated_at) -
        new Date(a.updated_at)
    );

    res.json({
      success: true,
      total: execucoes.length,
      executions: execucoes,
    });
  }
);

// ============================================================
// ÚLTIMA EXECUÇÃO DO USUÁRIO
// ============================================================

app.get(
  "/api/executions/latest",
  (req, res) => {

    const usuario =
      req.query.usuario;

    const detentora =
      req.query.detentora;

    if (!usuario) {
      return res.status(400).json({
        success: false,
        error:
          "usuario não informado.",
      });
    }

    let execucoes =
      Array.from(
        EXECUCOES.values()
      );

    execucoes =
      execucoes.filter(
        (execucao) =>
          execucao.usuario
            ?.toLowerCase() ===
          usuario.toLowerCase()
      );

    if (detentora) {

      execucoes =
        execucoes.filter(
          (execucao) =>
            execucao.detentora
              ?.toLowerCase() ===
            detentora.toLowerCase()
        );
    }

    execucoes.sort(
      (a, b) =>
        new Date(b.updated_at) -
        new Date(a.updated_at)
    );

    if (execucoes.length === 0) {

      return res.json({
        success: true,
        execution: null,
      });
    }

    return res.json({
      success: true,
      execution:
        execucoes[0],
    });
  }
);

// ============================================================
// ATUALIZAR STATUS DA EXECUÇÃO
// ============================================================

app.post(
  "/api/executions/:execucao_id/status",
  (req, res) => {

    try {

      const execucaoId =
        req.params.execucao_id;

      const {
        status,
        usuario,
        detentora,
        arquivo,
        protocolo,
      } = req.body;

      const execucao =
        EXECUCOES.get(
          execucaoId
        );

      if (!execucao) {

        return res.status(404).json({
          success: false,
          error:
            "Execução não encontrada.",
        });
      }

      // ------------------------------------------------------
      // ATUALIZA DADOS OPCIONAIS
      // ------------------------------------------------------

      if (usuario) {
        execucao.usuario =
          usuario;
      }

      if (detentora) {
        execucao.detentora =
          detentora;
      }

      if (arquivo) {
        execucao.arquivo =
          arquivo;
      }

      if (protocolo) {
        execucao.protocolo =
          protocolo;
      }

      // ------------------------------------------------------
      // STATUS
      // ------------------------------------------------------

      execucao.status =
        normalizarStatus(status);

      execucao.updated_at =
        new Date().toISOString();

      if (
        execucao.status ===
          "completed" ||
        execucao.status ===
          "error" ||
        execucao.status ===
          "cancelled"
      ) {

        execucao.finished_at =
          new Date().toISOString();
      }

      let mensagem =
        "Status da execução atualizado.";

      let nivel = "info";

      if (
        execucao.status ===
        "completed"
      ) {

        mensagem =
          "Automação concluída com sucesso.";

        nivel = "success";
      }

      if (
        execucao.status ===
        "error"
      ) {

        mensagem =
          "Automação finalizada com erro.";

        nivel = "error";
      }

      if (
        execucao.status ===
        "cancelled"
      ) {

        mensagem =
          "Automação cancelada.";

        nivel = "warn";
      }

      const evento = criarEvento({
        execucao_id:
          execucaoId,

        usuario:
          execucao.usuario,

        detentora:
          execucao.detentora,

        arquivo:
          execucao.arquivo,

        protocolo:
          execucao.protocolo,

        message:
          mensagem,

        level:
          nivel,

        event:
          "execution_status",
      });

      adicionarEvento(
        execucaoId,
        evento
      );

      console.log(
        `[EXECUÇÃO] Status atualizado | ${execucaoId} | ${execucao.status} | protocolo: ${execucao.protocolo || "-"}`
      );

      return res.json({
        success: true,
        execution: execucao,
      });

    } catch (erro) {

      console.error(
        "Erro ao atualizar status:",
        erro
      );

      return res.status(500).json({
        success: false,
        error:
          "Erro interno ao atualizar status.",
      });
    }
  }
);

// ============================================================
// RECEBER LOG
// ============================================================

app.post(
  "/api/logs",
  (req, res) => {

    try {

      const {
        message,
        level,
        execucao_id,
        usuario,
        detentora,
        arquivo,
        protocolo,
        event,
        timestamp,
      } = req.body;

      if (!message) {

        return res.status(400).json({
          success: false,
          error:
            "Mensagem não informada.",
        });
      }

      if (!execucao_id) {

        return res.status(400).json({
          success: false,
          error:
            "execucao_id não informado.",
        });
      }

      // ------------------------------------------------------
      // SE A EXECUÇÃO NÃO EXISTE, CRIA
      // ------------------------------------------------------

      if (
        !EXECUCOES.has(
          execucao_id
        )
      ) {

        const agora =
          new Date().toISOString();

        EXECUCOES.set(
          execucao_id,
          {
            execucao_id,

            usuario:
              usuario || null,

            detentora:
              detentora || null,

            arquivo:
              arquivo || null,

            protocolo:
              protocolo || null,

            status: "running",

            started_at: agora,

            updated_at: agora,

            finished_at: null,
          }
        );

        HISTORICO.set(
          execucao_id,
          []
        );

        limitarExecucoes();
      }

      // ------------------------------------------------------
      // ATUALIZA EXECUÇÃO
      // ------------------------------------------------------

      const execucao =
        EXECUCOES.get(
          execucao_id
        );

      if (usuario) {
        execucao.usuario =
          usuario;
      }

      if (detentora) {
        execucao.detentora =
          detentora;
      }

      if (arquivo) {
        execucao.arquivo =
          arquivo;
      }

      if (protocolo) {
        execucao.protocolo =
          protocolo;
      }

      execucao.updated_at =
        new Date().toISOString();

      // ------------------------------------------------------
      // CRIA EVENTO
      // ------------------------------------------------------

      const evento = criarEvento({
        message,

        level,

        execucao_id,

        usuario:
          usuario ||
          execucao.usuario,

        detentora:
          detentora ||
          execucao.detentora,

        arquivo:
          arquivo ||
          execucao.arquivo,

        protocolo:
          protocolo ||
          execucao.protocolo,

        event,

        timestamp,
      });

      adicionarEvento(
        execucao_id,
        evento
      );

      // ------------------------------------------------------
      // CONSOLE DO RENDER
      // ------------------------------------------------------

      console.log(
        `[${evento.level.toUpperCase()}]`,
        `[${evento.detentora || "?"}]`,
        `[${evento.usuario || "?"}]`,
        evento.protocolo
          ? `[PROTOCOLO: ${evento.protocolo}]`
          : "",
        evento.message
      );

      return res.json({
        success: true,

        event_id:
          evento.id,

        protocolo:
          execucao.protocolo ||
          null,
      });

    } catch (erro) {

      console.error(
        "Erro ao processar log:",
        erro
      );

      return res.status(500).json({
        success: false,
        error:
          "Erro interno ao processar log.",
      });
    }
  }
);

// ============================================================
// ADICIONAR EVENTO
// ============================================================

function adicionarEvento(
  execucaoId,
  evento
) {

  if (
    !HISTORICO.has(
      execucaoId
    )
  ) {

    HISTORICO.set(
      execucaoId,
      []
    );
  }

  const historico =
    HISTORICO.get(
      execucaoId
    );

  historico.push(
    evento
  );

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

  enviarParaExecucao(
    execucaoId,
    evento
  );
}

// ============================================================
// STREAM SSE
// ============================================================

app.get(
  "/api/logs/stream",
  (req, res) => {

    const execucaoId =
      req.query.execucao_id;

    if (!execucaoId) {

      return res.status(400).json({
        success: false,
        error:
          "execucao_id não informado.",
      });
    }

    // --------------------------------------------------------
    // HEADERS
    // --------------------------------------------------------

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
      "no-cache"
    );

    if (res.flushHeaders) {
      res.flushHeaders();
    }

    // --------------------------------------------------------
    // REGISTRA CLIENTE
    // --------------------------------------------------------

    if (
      !CLIENTES_SSE.has(
        execucaoId
      )
    ) {

      CLIENTES_SSE.set(
        execucaoId,
        new Set()
      );
    }

    const clientes =
      CLIENTES_SSE.get(
        execucaoId
      );

    clientes.add(res);

    console.log(
      `[SSE] Cliente conectado: ${execucaoId}`
    );

    // --------------------------------------------------------
    // ENVIA HISTÓRICO
    // --------------------------------------------------------

    const historico =
      HISTORICO.get(
        execucaoId
      ) || [];

    for (
      const evento of historico
    ) {

      enviarEventoSSE(
        res,
        evento
      );
    }

    // --------------------------------------------------------
    // CONFIRMA CONEXÃO
    // --------------------------------------------------------

    enviarEventoSSE(
      res,
      {
        id: gerarId(),

        event: "connected",

        execucao_id:
          execucaoId,

        message:
          "Conectado ao log da execução.",

        level: "info",

        timestamp:
          new Date().toISOString(),
      }
    );

    // --------------------------------------------------------
    // DESCONECTOU
    // --------------------------------------------------------

    req.on(
      "close",
      () => {

        clientes.delete(
          res
        );

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
// ENVIO SSE
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
// ENVIA PARA EXECUÇÃO
// ============================================================

function enviarParaExecucao(
  execucaoId,
  evento
) {

  const clientes =
    CLIENTES_SSE.get(
      execucaoId
    );

  if (!clientes) {
    return;
  }

  for (
    const cliente of clientes
  ) {

    enviarEventoSSE(
      cliente,
      evento
    );
  }
}

// ============================================================
// CONTAR CLIENTES SSE
// ============================================================

function contarClientes() {

  let total = 0;

  for (
    const clientes of
      CLIENTES_SSE.values()
  ) {

    total +=
      clientes.size;
  }

  return total;
}

// ============================================================
// HEARTBEAT
// ============================================================

setInterval(
  () => {

    for (
      const clientes of
        CLIENTES_SSE.values()
    ) {

      for (
        const cliente of
          clientes
      ) {

        try {

          cliente.write(
            `: heartbeat ${Date.now()}\n\n`
          );

        } catch (erro) {
          // conexão encerrada
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
      "Versão: 2.1.0"
    );

    console.log(
      `Porta: ${PORT}`
    );

    console.log(
      "============================================"
    );
  }
);