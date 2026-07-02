// hermes-bearer-proxy.js
//
// Copyright (c) 2026 Rehan Dwiandra - personal, non-commercial use only.
// See the LICENSE file for terms.
//
// This proxy sits between local AI-agent clients (Hermes, Odysseus, etc.)
// and an Anthropic-compatible upstream gateway, and does three things:
//
// 1. Auth rewrite: the gateway requires `Authorization: Bearer <key>`, but
//    some clients (e.g. Hermes' built-in `anthropic` provider) only know
//    how to send `x-api-key`. This proxy accepts either and always
//    forwards Bearer upstream. No secret is hardcoded - it just relays
//    whatever key the calling client sent.
//
// 2. /v1/models + /v1 stub: some clients (e.g. Odysseus) probe these paths
//    to confirm a custom endpoint is alive before they'll let you use it.
//    The real gateway only implements POST /v1/messages and correctly
//    404s on those probe paths. This proxy answers them locally with a
//    small hand-maintained model list instead.
//
// 3. OpenAI <-> Anthropic translation: some clients (e.g. Odysseus) send
//    real chat requests in OpenAI's /v1/chat/completions shape. The real
//    gateway only understands Anthropic's /v1/messages shape. This proxy
//    translates the request on the way in and the response (including
//    streamed responses) on the way back out.
//
// Usage:
//   node hermes-bearer-proxy.js          (single run)
//   ./run-proxy.sh                       (supervised - auto-restarts on crash)
//
// On first run it asks for your gateway host and (optionally) a GitHub repo
// to check for updates, then saves both to ~/.hermes-proxy/config.json. The
// gateway address never lives in this file, so the script can be published
// without revealing where it forwards to. Env vars TARGET_HOST/TARGET_PORT
// still override the saved config.
//
// Tunables (env vars): UPSTREAM_TIMEOUT_MS (default 60000, header-arrival
// timeout), UPSTREAM_RETRIES (default 2), RETRY_BASE_DELAY_MS (default 500).
//
// Point your client's base URL at http://127.0.0.1:8787 instead of the
// gateway directly. Keep this running in its own terminal window the whole
// time you want any of those clients to reach the gateway.

const http = require('http');
const https = require('https');
const fs = require('fs');
const os = require('os');
const path = require('path');
const readline = require('readline');

const VERSION = '1.3.0';

// Set this to 'owner/repo' before building exes you hand out to other
// people - it bakes the update source into the binary so they're never
// prompted for it and their exes auto-update from your GitHub releases.
// (This only names a public repo; the gateway address still lives in each
// user's local config, never in the source.)
const DEFAULT_UPDATE_REPO = 'isoldeapos/bearerProxy';

// True when running as a bun-compiled executable (process.execPath is the
// binary itself rather than node/bun). Controls how update instructions are
// phrased and whether --self-update is allowed to replace the executable.
const IS_COMPILED = (() => {
  const base = path.basename(process.execPath).toLowerCase();
  return !base.startsWith('node') && !base.startsWith('bun');
})();

// Resolved at startup from env > ~/.hermes-proxy/config.json > first-run
// prompt. Deliberately NOT hardcoded - see header comment.
let TARGET_HOST;
let TARGET_PORT;

const LISTEN_PORT = 8787;
const LISTEN_HOST = '127.0.0.1';
const ANTHROPIC_VERSION = '2023-06-01';
const MAX_BODY_BYTES = 50 * 1024 * 1024; // 50 MB safety cap on request bodies

// How long to wait for the gateway to send *response headers* before giving
// up on that attempt. Deliberately applies only up to headers: once a
// streamed response starts, chunks may legitimately sit quiet for a while
// and must not be killed by this timer. Note that on NON-streaming requests
// the gateway sends headers only after generating the full completion, so
// don't set this lower than your longest non-streaming generation.
// Override: UPSTREAM_TIMEOUT_MS=15000 node hermes-bearer-proxy.js
const UPSTREAM_TIMEOUT_MS = parseInt(process.env.UPSTREAM_TIMEOUT_MS, 10) || 60000;

// Transient-failure retries. A request is retried only while it is still
// safe to do so - i.e. before a single response byte has been forwarded to
// the client. Retryable: 502/503/504 from the gateway, connection errors,
// and header timeouts. UPSTREAM_RETRIES is the number of *re*-attempts, so
// 2 means up to 3 total attempts.
const UPSTREAM_RETRIES = parseInt(process.env.UPSTREAM_RETRIES, 10) || 2;
const RETRY_BASE_DELAY_MS = parseInt(process.env.RETRY_BASE_DELAY_MS, 10) || 500;
const RETRYABLE_STATUS = new Set([502, 503, 504]);

// Reuse upstream TLS connections. Without this, every proxied request pays
// a fresh TCP + TLS handshake to the gateway - noticeable when many agents
// hit the proxy concurrently.
const UPSTREAM_AGENT = new https.Agent({
  keepAlive: true,
  maxSockets: 256, // per-host cap on concurrent upstream sockets
  keepAliveMsecs: 30000,
});

// ---------------------------------------------------------------------------
// Fake /v1/models + /v1 responses
// ---------------------------------------------------------------------------
// Hand-maintained, not fetched from anywhere real - the gateway has no
// /v1/models endpoint to query. qwen3.7-max, qwen3.7-plus, deepseek-v4-pro,
// and deepseek-v4-flash have all been confirmed working through this
// gateway. glm-5.2 has NOT been confirmed - added per request, but if you
// actually select it and the gateway doesn't serve that model, expect a
// real "model does not exist" error back from the gateway at request time.
const STUB_MODELS = [
  'qwen3.7-max',
  'qwen3.7-plus',
  'deepseek-v4-pro',
  'deepseek-v4-flash',
  'glm-5.2', // unconfirmed - see note above
];

function sendModelsStub(res) {
  const body = JSON.stringify({
    object: 'list',
    data: STUB_MODELS.map((id) => ({
      id,
      object: 'model',
      created: 0,
      owned_by: 'upstream-gateway',
    })),
  });
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(body);
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------
function extractKey(headers) {
  if (headers['x-api-key']) return headers['x-api-key'];
  const auth = headers['authorization'];
  if (auth && auth.toLowerCase().startsWith('bearer ')) return auth.slice(7);
  return null;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new Error('Request body too large'));
        req.destroy();
        return;
      }
      data += chunk;
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

// Binary-safe variant of readBody - used where the body must be replayable
// for retries and may not be UTF-8 text.
function readBodyBuffer(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new Error('Request body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Upstream request with header-timeout + transient-failure retry
// ---------------------------------------------------------------------------
// One attempt: resolves with proxyRes once response headers arrive, rejects
// on connection error or if headers don't arrive within UPSTREAM_TIMEOUT_MS.
// `ctx.current` tracks the in-flight request so the caller can destroy it if
// the client disconnects mid-retry-loop.
function upstreamOnce(options, bodyBuffer, ctx) {
  return new Promise((resolve, reject) => {
    const proxyReq = https.request(options, (proxyRes) => {
      clearTimeout(timer);
      resolve(proxyRes);
    });
    ctx.current = proxyReq;
    const timer = setTimeout(() => {
      const err = new Error(`upstream timed out after ${UPSTREAM_TIMEOUT_MS}ms waiting for response headers`);
      err.code = 'UPSTREAM_TIMEOUT';
      proxyReq.destroy(err);
    }, UPSTREAM_TIMEOUT_MS);
    proxyReq.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
    if (bodyBuffer && bodyBuffer.length > 0) proxyReq.write(bodyBuffer);
    proxyReq.end();
  });
}

// Retry loop around upstreamOnce. Retries 502/503/504 responses, connection
// errors, and header timeouts, with exponential backoff. This is safe
// because nothing has been written to the client yet when it runs - the
// caller only starts forwarding once this resolves. Mid-stream failures
// (after headers) are NOT retried; replaying into a half-written response
// would corrupt it.
async function upstreamWithRetry(options, bodyBuffer, ctx, label) {
  let lastFailure;
  for (let attempt = 0; attempt <= UPSTREAM_RETRIES; attempt++) {
    if (attempt > 0) {
      const delay = RETRY_BASE_DELAY_MS * 2 ** (attempt - 1);
      console.warn(
        `[retry] ${label}: ${lastFailure} - attempt ${attempt + 1}/${UPSTREAM_RETRIES + 1} in ${delay}ms`
      );
      await sleep(delay);
    }
    if (ctx.aborted) {
      const err = new Error('client disconnected during retry');
      err.code = 'CLIENT_GONE';
      throw err;
    }
    try {
      const proxyRes = await upstreamOnce(options, bodyBuffer, ctx);
      if (RETRYABLE_STATUS.has(proxyRes.statusCode) && attempt < UPSTREAM_RETRIES) {
        lastFailure = `gateway returned ${proxyRes.statusCode}`;
        proxyRes.resume(); // drain and discard so the socket can be reused
        continue;
      }
      return proxyRes;
    } catch (err) {
      if (err.code === 'CLIENT_GONE') throw err;
      lastFailure = err.message;
      if (attempt >= UPSTREAM_RETRIES) throw err;
    }
  }
  throw new Error(lastFailure); // unreachable, but keeps the types honest
}

// Map an exhausted-retries error onto a client-facing status code.
function upstreamErrorStatus(err) {
  return err.code === 'UPSTREAM_TIMEOUT' ? 504 : 502;
}

function mapStopReason(reason) {
  switch (reason) {
    case 'max_tokens':
      return 'length';
    case 'tool_use':
      return 'tool_calls';
    default:
      return 'stop'; // end_turn, stop_sequence, anything else
  }
}

// ---------------------------------------------------------------------------
// OpenAI multi-part content -> Anthropic content blocks
// ---------------------------------------------------------------------------
// String content passes through untouched. Array content gets each part
// converted: OpenAI text parts happen to match Anthropic's text block shape,
// but image parts do not - OpenAI uses {type:'image_url'}, Anthropic wants
// {type:'image', source:{...}}. Both data: URLs and http(s) URLs are handled.
function convertContentParts(content) {
  if (typeof content === 'string' || content == null) return content;
  if (!Array.isArray(content)) return JSON.stringify(content);
  return content.map((part) => {
    if (part.type === 'text') return { type: 'text', text: part.text };
    if (part.type === 'image_url') {
      const url = (part.image_url && part.image_url.url) || '';
      const m = url.match(/^data:(image\/[a-z0-9+.-]+);base64,(.*)$/is);
      if (m) {
        return { type: 'image', source: { type: 'base64', media_type: m[1], data: m[2] } };
      }
      return { type: 'image', source: { type: 'url', url } };
    }
    // Unknown part type - degrade to text rather than sending a shape the
    // gateway will reject outright.
    return { type: 'text', text: JSON.stringify(part) };
  });
}

// ---------------------------------------------------------------------------
// OpenAI /v1/chat/completions request -> Anthropic /v1/messages request
// ---------------------------------------------------------------------------
function openaiToAnthropicRequest(body) {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  const systemParts = [];
  const anthropicMessages = [];

  for (const msg of messages) {
    if (msg.role === 'system') {
      systemParts.push(typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content));
      continue;
    }

    if (msg.role === 'tool') {
      anthropicMessages.push({
        role: 'user',
        content: [
          {
            type: 'tool_result',
            tool_use_id: msg.tool_call_id,
            content: typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content),
          },
        ],
      });
      continue;
    }

    if (msg.role === 'assistant' && Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0) {
      const blocks = [];
      if (msg.content) {
        blocks.push({
          type: 'text',
          text: typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content),
        });
      }
      for (const call of msg.tool_calls) {
        let input;
        try {
          input = JSON.parse(call.function.arguments || '{}');
        } catch (e) {
          input = { _raw_arguments: call.function.arguments };
        }
        blocks.push({ type: 'tool_use', id: call.id, name: call.function.name, input });
      }
      anthropicMessages.push({ role: 'assistant', content: blocks });
      continue;
    }

    // Plain user/assistant content - convert multi-part arrays (esp. images)
    anthropicMessages.push({ role: msg.role, content: convertContentParts(msg.content) });
  }

  const anthropicBody = {
    model: body.model,
    messages: anthropicMessages,
    // Anthropic requires max_tokens; OpenAI doesn't. Newer OpenAI clients
    // send max_completion_tokens instead of max_tokens - honor both.
    max_tokens: body.max_tokens ?? body.max_completion_tokens ?? 4096,
    stream: !!body.stream,
  };

  if (systemParts.length > 0) anthropicBody.system = systemParts.join('\n\n');
  if (body.temperature !== undefined) anthropicBody.temperature = body.temperature;
  if (body.top_p !== undefined) anthropicBody.top_p = body.top_p;
  if (body.stop !== undefined) {
    anthropicBody.stop_sequences = Array.isArray(body.stop) ? body.stop : [body.stop];
  }

  if (Array.isArray(body.tools) && body.tools.length > 0) {
    anthropicBody.tools = body.tools
      .filter((t) => t.type === 'function' && t.function)
      .map((t) => ({
        name: t.function.name,
        description: t.function.description || '',
        input_schema: t.function.parameters || { type: 'object', properties: {} },
      }));
  }

  if (body.tool_choice === 'required') {
    anthropicBody.tool_choice = { type: 'any' };
  } else if (typeof body.tool_choice === 'object' && body.tool_choice?.function?.name) {
    anthropicBody.tool_choice = { type: 'tool', name: body.tool_choice.function.name };
  } else if (body.tool_choice === 'auto') {
    anthropicBody.tool_choice = { type: 'auto' };
  }
  // body.tool_choice === 'none' has no Anthropic equivalent - omitted; not
  // sending tool_choice leaves Anthropic on its own default behavior.

  return anthropicBody;
}

// ---------------------------------------------------------------------------
// Anthropic /v1/messages response -> OpenAI chat/completions response
// (non-streaming case)
// ---------------------------------------------------------------------------
function anthropicToOpenaiMessage(content) {
  let text = '';
  const toolCalls = [];
  for (const block of content || []) {
    if (block.type === 'text') text += block.text;
    else if (block.type === 'tool_use') {
      toolCalls.push({
        id: block.id,
        type: 'function',
        function: { name: block.name, arguments: JSON.stringify(block.input || {}) },
      });
    }
  }
  const message = { role: 'assistant', content: text.length > 0 ? text : null };
  if (toolCalls.length > 0) message.tool_calls = toolCalls;
  return message;
}

function anthropicToOpenaiResponse(a) {
  return {
    id: a.id || `chatcmpl-${Date.now()}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: a.model,
    choices: [
      {
        index: 0,
        message: anthropicToOpenaiMessage(a.content),
        finish_reason: mapStopReason(a.stop_reason),
      },
    ],
    usage: {
      prompt_tokens: a.usage?.input_tokens || 0,
      completion_tokens: a.usage?.output_tokens || 0,
      total_tokens: (a.usage?.input_tokens || 0) + (a.usage?.output_tokens || 0),
    },
  };
}

// ---------------------------------------------------------------------------
// Streaming: translate Anthropic SSE events into OpenAI-style SSE chunks
// as they arrive
// ---------------------------------------------------------------------------
function makeStreamTranslator(res, model, includeUsage) {
  const id = `chatcmpl-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const created = Math.floor(Date.now() / 1000);
  let buffer = '';

  // Anthropic's evt.index is the *content block* index (a text block ahead
  // of the first tool_use pushes tool blocks to index 1+). OpenAI clients
  // expect tool_calls[].index to be 0-based and sequential per tool call,
  // so remap: Anthropic block index -> our own counter.
  const toolIdxByBlock = new Map();
  let nextToolIdx = 0;

  // Token usage, accumulated for an optional final usage chunk
  // (stream_options: {include_usage: true}).
  let inputTokens = 0;
  let outputTokens = 0;

  function writeChunk(deltaObj, finishReason) {
    if (res.writableEnded) return;
    const chunk = {
      id,
      object: 'chat.completion.chunk',
      created,
      model,
      choices: [{ index: 0, delta: deltaObj, finish_reason: finishReason }],
    };
    res.write(`data: ${JSON.stringify(chunk)}\n\n`);
  }

  function handleEvent(dataStr) {
    let evt;
    try {
      evt = JSON.parse(dataStr);
    } catch (e) {
      return; // ignore malformed/keepalive lines
    }

    switch (evt.type) {
      case 'message_start':
        inputTokens = evt.message?.usage?.input_tokens || 0;
        writeChunk({ role: 'assistant', content: '' }, null);
        break;

      case 'content_block_start':
        if (evt.content_block?.type === 'tool_use') {
          const toolIdx = nextToolIdx++;
          toolIdxByBlock.set(evt.index, toolIdx);
          writeChunk(
            {
              tool_calls: [
                {
                  index: toolIdx,
                  id: evt.content_block.id,
                  type: 'function',
                  function: { name: evt.content_block.name, arguments: '' },
                },
              ],
            },
            null
          );
        }
        break;

      case 'content_block_delta':
        if (evt.delta?.type === 'text_delta') {
          writeChunk({ content: evt.delta.text }, null);
        } else if (evt.delta?.type === 'input_json_delta') {
          const toolIdx = toolIdxByBlock.get(evt.index);
          if (toolIdx !== undefined) {
            writeChunk(
              { tool_calls: [{ index: toolIdx, function: { arguments: evt.delta.partial_json || '' } }] },
              null
            );
          }
        }
        break;

      case 'message_delta':
        if (evt.usage?.output_tokens !== undefined) outputTokens = evt.usage.output_tokens;
        if (evt.delta?.stop_reason) {
          writeChunk({}, mapStopReason(evt.delta.stop_reason));
        }
        break;

      case 'message_stop':
        if (res.writableEnded) break;
        if (includeUsage) {
          const usageChunk = {
            id,
            object: 'chat.completion.chunk',
            created,
            model,
            choices: [],
            usage: {
              prompt_tokens: inputTokens,
              completion_tokens: outputTokens,
              total_tokens: inputTokens + outputTokens,
            },
          };
          res.write(`data: ${JSON.stringify(usageChunk)}\n\n`);
        }
        res.write('data: [DONE]\n\n');
        res.end();
        break;

      default:
        break; // content_block_stop, ping, etc. - nothing to emit
    }
  }

  return function feed(rawChunk) {
    if (res.writableEnded) return;
    buffer += rawChunk;
    let sepIndex;
    while ((sepIndex = buffer.indexOf('\n\n')) !== -1) {
      const block = buffer.slice(0, sepIndex);
      buffer = buffer.slice(sepIndex + 2);
      const dataLines = block
        .split('\n')
        .filter((line) => line.startsWith('data:'))
        .map((line) => line.slice(5).trim());
      // Per the SSE spec, multiple data: lines in one event join with '\n'
      if (dataLines.length > 0) handleEvent(dataLines.join('\n'));
    }
  };
}

// ---------------------------------------------------------------------------
// Generic Bearer-rewrite forward - used for native /v1/messages calls
// (e.g. from Hermes) and anything that isn't the chat/completions shim
// ---------------------------------------------------------------------------
async function forwardRaw(req, res) {
  const headers = { ...req.headers };
  const key = extractKey(headers);
  delete headers['x-api-key'];
  delete headers['host'];
  // Strip hop-by-hop headers - they describe the client<->proxy connection,
  // not the proxy<->gateway one.
  delete headers['connection'];
  delete headers['keep-alive'];
  delete headers['proxy-authorization'];
  delete headers['te'];
  delete headers['upgrade'];
  if (key) headers['authorization'] = `Bearer ${key}`;

  // Buffer the request body so it can be replayed on retry. (The old
  // req.pipe(proxyReq) approach made retries impossible - the body was gone
  // after the first attempt.)
  const bodyBuffer = await readBodyBuffer(req);
  delete headers['transfer-encoding'];
  headers['content-length'] = bodyBuffer.length;

  // If the client goes away, abort any in-flight/queued upstream attempt.
  const ctx = { aborted: false, current: null };
  res.on('close', () => {
    if (!res.writableEnded) {
      ctx.aborted = true;
      if (ctx.current) ctx.current.destroy();
    }
  });

  let proxyRes;
  try {
    proxyRes = await upstreamWithRetry(
      { hostname: TARGET_HOST, port: TARGET_PORT, path: req.url, method: req.method, headers, agent: UPSTREAM_AGENT },
      bodyBuffer,
      ctx,
      `${req.method} ${req.url}`
    );
  } catch (err) {
    console.error('Upstream request failed after retries:', err.message);
    if (res.writableEnded) return;
    if (!res.headersSent) res.writeHead(upstreamErrorStatus(err), { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'proxy_upstream_error', message: err.message }));
    return;
  }

  res.writeHead(proxyRes.statusCode, proxyRes.headers);
  proxyRes.pipe(res);
  // Mid-stream upstream failure - too late to retry, just close out.
  proxyRes.on('error', (err) => {
    console.error('Upstream response error mid-stream:', err.message);
    if (!res.writableEnded) res.end();
  });
}

// ---------------------------------------------------------------------------
// OpenAI-shaped /v1/chat/completions handler - translates both directions
// ---------------------------------------------------------------------------
async function handleChatCompletions(req, res) {
  const key = extractKey(req.headers);
  const rawBody = await readBody(req);

  let openaiBody;
  try {
    openaiBody = JSON.parse(rawBody);
  } catch (e) {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'Invalid JSON body', type: 'invalid_request_error' } }));
    return;
  }

  const anthropicBody = openaiToAnthropicRequest(openaiBody);
  const payload = JSON.stringify(anthropicBody);
  const includeUsage = !!openaiBody.stream_options?.include_usage;

  const upstreamHeaders = {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(payload),
    'anthropic-version': ANTHROPIC_VERSION,
  };
  if (key) upstreamHeaders['authorization'] = `Bearer ${key}`;

  // Client disconnected (agent killed, tab closed, timeout) - stop the
  // upstream request too instead of paying for tokens nobody will read.
  const ctx = { aborted: false, current: null };
  res.on('close', () => {
    if (!res.writableEnded) {
      ctx.aborted = true;
      if (ctx.current) ctx.current.destroy();
    }
  });

  let proxyRes;
  try {
    proxyRes = await upstreamWithRetry(
      {
        hostname: TARGET_HOST,
        port: TARGET_PORT,
        path: '/v1/messages',
        method: 'POST',
        headers: upstreamHeaders,
        agent: UPSTREAM_AGENT,
      },
      Buffer.from(payload),
      ctx,
      'POST /v1/chat/completions'
    );
  } catch (err) {
    console.error('Upstream request failed after retries:', err.message);
    if (res.writableEnded) return;
    if (!res.headersSent) res.writeHead(upstreamErrorStatus(err), { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: { message: err.message, type: 'proxy_upstream_error' } }));
    return;
  }

  // On stream requests the gateway still sends errors as a plain JSON
  // body. Don't feed that into the SSE translator (it would silently
  // swallow it) - buffer it and pass it through as JSON instead.
  if (anthropicBody.stream && proxyRes.statusCode < 400) {
    res.writeHead(proxyRes.statusCode, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
    });
    const feed = makeStreamTranslator(res, anthropicBody.model, includeUsage);
    proxyRes.setEncoding('utf8');
    proxyRes.on('data', feed);
    proxyRes.on('end', () => {
      if (!res.writableEnded) res.end();
    });
    proxyRes.on('error', (err) => {
      console.error('Upstream stream error mid-response:', err.message);
      if (!res.writableEnded) res.end();
    });
  } else {
    let raw = '';
    proxyRes.setEncoding('utf8');
    proxyRes.on('data', (c) => (raw += c));
    proxyRes.on('error', (err) => {
      console.error('Upstream response error:', err.message);
      if (res.writableEnded) return;
      if (!res.headersSent) res.writeHead(502, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { message: err.message, type: 'proxy_upstream_error' } }));
    });
    proxyRes.on('end', () => {
      if (res.writableEnded) return;
      if (proxyRes.statusCode >= 400) {
        res.writeHead(proxyRes.statusCode, { 'content-type': 'application/json' });
        res.end(raw);
        return;
      }
      let anthropicResp;
      try {
        anthropicResp = JSON.parse(raw);
      } catch (e) {
        res.writeHead(502, { 'content-type': 'application/json' });
        res.end(
          JSON.stringify({ error: { message: 'Could not parse upstream response', type: 'proxy_error' } })
        );
        return;
      }
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify(anthropicToOpenaiResponse(anthropicResp)));
    });
  }
}

// ---------------------------------------------------------------------------
// Local config: ~/.hermes-proxy/config.json
// ---------------------------------------------------------------------------
// Keeps the gateway address (and optional update repo) out of the source so
// the script can live in a public repo. chmod 600 - it's private local state.
const CONFIG_DIR = path.join(os.homedir(), '.hermes-proxy');
const CONFIG_PATH = path.join(CONFIG_DIR, 'config.json');

function loadConfig() {
  try {
    return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
  } catch (e) {
    return null;
  }
}

function saveConfig(cfg) {
  fs.mkdirSync(CONFIG_DIR, { recursive: true, mode: 0o700 });
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2) + '\n', { mode: 0o600 });
}

// Small prompt helper that works with both an interactive TTY and piped
// stdin (chained rl.question calls can drop lines that arrive before the
// question is asked). Lines are queued; a closed stdin resolves ''.
function makePrompt() {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  const queued = [];
  const waiting = [];
  let closed = false;
  rl.on('line', (line) => {
    const w = waiting.shift();
    if (w) w(line);
    else queued.push(line);
  });
  rl.on('close', () => {
    closed = true;
    while (waiting.length > 0) waiting.shift()('');
  });
  return {
    ask(q) {
      process.stdout.write(q);
      return new Promise((resolve) => {
        if (queued.length > 0) return resolve(queued.shift());
        if (closed) return resolve('');
        waiting.push(resolve);
      });
    },
    isClosed: () => closed,
    close: () => rl.close(),
  };
}

// Accepts "host", "host:port", or "https://host[:port]".
function parseGateway(input) {
  const m = input.match(/^(?:https?:\/\/)?([^\/:]+)(?::(\d+))?\/?$/);
  if (!m) return null;
  return { host: m[1], port: m[2] ? parseInt(m[2], 10) : 443 };
}

async function resolveConfig() {
  let cfg = loadConfig() || {};

  // First run (or config deleted): ask and save.
  if (!cfg.gateway_host && !process.env.TARGET_HOST) {
    const prompt = makePrompt();
    console.log('First-run setup - answers are saved to ' + CONFIG_PATH);
    let gw = null;
    while (!gw) {
      const answer = (await prompt.ask('Gateway host (e.g. https://gateway.example.com): ')).trim();
      if (answer === '' && prompt.isClosed()) {
        throw new Error(
          `no gateway configured and stdin is closed - run interactively once, set TARGET_HOST, or create ${CONFIG_PATH}`
        );
      }
      gw = parseGateway(answer);
      if (!gw) console.log('Could not parse that - enter a hostname or https:// URL.');
    }
    cfg.gateway_host = gw.host;
    cfg.gateway_port = gw.port;
    if (!DEFAULT_UPDATE_REPO) {
      const repo = (await prompt.ask('GitHub repo for update checks (owner/repo, blank to skip): ')).trim();
      if (repo) cfg.update_repo = repo;
    }
    prompt.close();
    saveConfig(cfg);
    console.log('Saved. Delete ' + CONFIG_PATH + ' to run setup again.');
  }

  TARGET_HOST = process.env.TARGET_HOST || cfg.gateway_host;
  TARGET_PORT = parseInt(process.env.TARGET_PORT, 10) || cfg.gateway_port || 443;
  return cfg;
}

// ---------------------------------------------------------------------------
// Update check (non-blocking, best-effort)
// ---------------------------------------------------------------------------
// Fetches version.json from the repo's default branch and prints a notice if
// it's newer than VERSION. Never blocks or crashes startup - any failure is
// silently ignored. Notify-only by design: auto-replacing a running script/
// binary is a separate, riskier step.
function semverNewer(remote, local) {
  const r = String(remote).split('.').map(Number);
  const l = String(local).split('.').map(Number);
  for (let i = 0; i < 3; i++) {
    if ((r[i] || 0) > (l[i] || 0)) return true;
    if ((r[i] || 0) < (l[i] || 0)) return false;
  }
  return false;
}

// GET that follows redirects (GitHub release downloads 302 to a CDN host).
function httpGetFollow(url, redirectsLeft = 5) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = https.get(
      {
        hostname: u.hostname,
        port: u.port || 443,
        path: u.pathname + u.search,
        headers: { 'user-agent': `hermes-bearer-proxy/${VERSION}` },
        timeout: 30000,
      },
      (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location && redirectsLeft > 0) {
          res.resume();
          resolve(httpGetFollow(new URL(res.headers.location, url).href, redirectsLeft - 1));
          return;
        }
        resolve(res);
      }
    );
    req.on('timeout', () => req.destroy(new Error('request timed out')));
    req.on('error', reject);
  });
}

// Both URLs are overridable via env - useful for testing and for hosting
// releases somewhere other than github.com.
function versionUrl(repo) {
  return process.env.UPDATE_VERSION_URL || `https://raw.githubusercontent.com/${repo}/main/version.json`;
}
function assetBaseUrl(repo) {
  return process.env.UPDATE_ASSET_BASE || `https://github.com/${repo}/releases/download`;
}

async function fetchRemoteVersion(repo) {
  const res = await httpGetFollow(versionUrl(repo));
  if (res.statusCode !== 200) {
    res.resume();
    throw new Error(`version check got HTTP ${res.statusCode}`);
  }
  let raw = '';
  res.setEncoding('utf8');
  await new Promise((resolve, reject) => {
    res.on('data', (c) => (raw += c));
    res.on('end', resolve);
    res.on('error', reject);
  });
  return JSON.parse(raw).version;
}

// Startup check: notify only, never blocks or crashes startup.
function checkForUpdate(repo) {
  if (!repo) return;
  fetchRemoteVersion(repo)
    .then((remote) => {
      if (semverNewer(remote, VERSION)) {
        const how = IS_COMPILED ? 'run with --self-update to install it' : 'git pull to update';
        console.log(`[update] v${remote} is available (you have v${VERSION}) - ${how}`);
      }
    })
    .catch(() => {});
}

// ---------------------------------------------------------------------------
// Self-update (--self-update, compiled executable only)
// ---------------------------------------------------------------------------
// Downloads the release asset for this platform from GitHub Releases and
// swaps it over the running executable:
//   - POSIX: rename new binary over the current path (the running process
//     keeps its old inode; the path serves the new file from now on).
//   - Windows: a running .exe can't be overwritten, but it CAN be renamed -
//     so current -> .old, new -> current. The leftover .old is deleted on
//     the next startup.
// Expects release assets named hermes-bearer-proxy-<platform>-<arch>[.exe]
// under a tag named v<version> (build-release.sh produces exactly this).
function releaseAssetName() {
  const plat = { win32: 'windows', darwin: 'darwin', linux: 'linux' }[process.platform] || process.platform;
  const ext = process.platform === 'win32' ? '.exe' : '';
  return `hermes-bearer-proxy-${plat}-${process.arch}${ext}`;
}

async function downloadToFile(url, dest) {
  const res = await httpGetFollow(url);
  if (res.statusCode !== 200) {
    res.resume();
    throw new Error(`download got HTTP ${res.statusCode} for ${url}`);
  }
  await new Promise((resolve, reject) => {
    const out = fs.createWriteStream(dest);
    res.pipe(out);
    out.on('finish', resolve);
    out.on('error', reject);
    res.on('error', reject);
  });
}

// Returns true if a new version was installed, false otherwise.
async function selfUpdate(repo, opts = {}) {
  if (!repo) {
    throw new Error(
      'no update repo configured - set update_repo in ' + CONFIG_PATH + ' or pass UPDATE_REPO=owner/repo'
    );
  }
  const remote = await fetchRemoteVersion(repo);
  if (!semverNewer(remote, VERSION)) {
    if (!opts.quietWhenCurrent) console.log(`Already up to date (v${VERSION}).`);
    return false;
  }
  // HERMES_SELF_UPDATE_TARGET is a test/advanced hook to update a different
  // file than the running executable.
  const target = process.env.HERMES_SELF_UPDATE_TARGET || process.execPath;
  if (!process.env.HERMES_SELF_UPDATE_TARGET && !IS_COMPILED) {
    console.log(
      `v${remote} is available, but you're running the .js via ${path.basename(process.execPath)} - ` +
        'update with git pull instead; --self-update only replaces the compiled executable.'
    );
    return false;
  }
  const url = `${assetBaseUrl(repo)}/v${remote}/${releaseAssetName()}`;
  console.log(`Updating v${VERSION} -> v${remote}`);
  console.log(`Downloading ${url}`);
  const tmp = target + '.new';
  await downloadToFile(url, tmp);
  if (process.platform !== 'win32') fs.chmodSync(tmp, 0o755);
  if (process.platform === 'win32') {
    try {
      fs.unlinkSync(target + '.old');
    } catch (e) {
      /* no leftover */
    }
    fs.renameSync(target, target + '.old');
  }
  fs.renameSync(tmp, target);
  console.log(`Updated to v${remote}.`);
  return true;
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------
const server = http.createServer((req, res) => {
  // Logs method + path only, never headers or bodies, so the key is never
  // written anywhere.
  console.log(`${req.method} ${req.url}`);

  if (req.method === 'GET' && (req.url === '/v1/models' || req.url === '/v1')) {
    return sendModelsStub(res);
  }

  if (req.method === 'POST' && req.url === '/v1/chat/completions') {
    handleChatCompletions(req, res).catch((err) => {
      console.error('Translation error:', err.message);
      if (res.writableEnded) return;
      if (!res.headersSent) res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { message: err.message, type: 'proxy_internal_error' } }));
    });
    return;
  }

  // Anything else (notably native /v1/messages from Hermes) - forward as-is
  // with the same Bearer-rewrite, unchanged.
  forwardRaw(req, res).catch((err) => {
    console.error('Forward error:', err.message);
    if (res.writableEnded) return;
    if (!res.headersSent) res.writeHead(500, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'proxy_internal_error', message: err.message }));
  });
});

// Long streamed completions can sit quiet for a while between chunks; don't
// let Node's default socket timeouts kill them.
server.requestTimeout = 0;
server.headersTimeout = 60000;
server.keepAliveTimeout = 75000;

async function main() {
  if (process.argv.includes('--version')) {
    console.log(VERSION);
    return;
  }

  // Clean up the leftover from a previous Windows self-update.
  try {
    fs.unlinkSync(process.execPath + '.old');
  } catch (e) {
    /* none */
  }

  if (process.argv.includes('--self-update')) {
    // Deliberately does not run first-run setup - just needs the repo.
    const cfgNow = loadConfig() || {};
    await selfUpdate(process.env.UPDATE_REPO || cfgNow.update_repo || DEFAULT_UPDATE_REPO);
    return;
  }

  const cfg = await resolveConfig();
  const updateRepo = process.env.UPDATE_REPO || cfg.update_repo || DEFAULT_UPDATE_REPO;

  // Auto-update at startup (compiled exe only; opt out with
  // "auto_update": false in the config). Best-effort: any failure - repo
  // unreachable, asset missing - logs a warning and starts the current
  // version instead. When an update IS installed:
  //   - under run-proxy.sh (HERMES_SUPERVISED=1): exit and let the
  //     supervisor restart, which picks up the new binary
  //   - standalone: spawn the new binary detached and exit, so a
  //     double-clicked exe seamlessly becomes the new version
  const canSelfReplace = IS_COMPILED || process.env.HERMES_SELF_UPDATE_TARGET;
  if (canSelfReplace && updateRepo && cfg.auto_update !== false) {
    try {
      const updated = await selfUpdate(updateRepo, { quietWhenCurrent: true });
      if (updated) {
        if (process.env.HERMES_SUPERVISED) {
          console.log('[update] exiting so the supervisor restarts the new version');
        } else {
          const target = process.env.HERMES_SELF_UPDATE_TARGET || process.execPath;
          console.log('[update] relaunching as the new version');
          const child = require('child_process').spawn(target, [], { detached: true, stdio: 'inherit' });
          child.unref();
        }
        process.exit(0);
      }
    } catch (err) {
      console.warn(`[update] auto-update failed (${err.message}) - starting current version`);
    }
  } else {
    checkForUpdate(updateRepo);
  }

  server.listen(LISTEN_PORT, LISTEN_HOST, () => {
    console.log(`hermes-bearer-proxy v${VERSION}`);
    console.log(`Proxy listening on http://${LISTEN_HOST}:${LISTEN_PORT}`);
    console.log(`Forwarding to https://${TARGET_HOST}${TARGET_PORT !== 443 ? `:${TARGET_PORT}` : ''}`);
    console.log(
      `Upstream header timeout: ${UPSTREAM_TIMEOUT_MS}ms; retries on 502/503/504 + connection errors: ${UPSTREAM_RETRIES}`
    );
    console.log('Translates OpenAI /v1/chat/completions <-> Anthropic /v1/messages, and rewrites auth headers.');
    console.log('Leave this running while you use Hermes or Odysseus. Ctrl+C to stop.');
  });
}

main().catch((err) => {
  console.error('Fatal startup error:', err.message);
  process.exit(1);
});
