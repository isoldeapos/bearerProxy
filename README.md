# hermes-bearer-proxy

A small local proxy that sits between AI-agent clients (Hermes, Odysseus, etc.) and an Anthropic-compatible upstream gateway. Point your client's base URL at `http://127.0.0.1:8787` and leave the proxy running.

## What it does

1. **Auth rewrite** — the gateway requires `Authorization: Bearer <key>`, but some clients only know how to send `x-api-key`. The proxy accepts either and always forwards Bearer upstream. No secret is hardcoded; it relays whatever key the client sent.
2. **`/v1/models` + `/v1` stub** — some clients probe these paths to confirm an endpoint is alive, but the real gateway only implements `POST /v1/messages`. The proxy answers them locally.
3. **OpenAI ↔ Anthropic translation** — requests in OpenAI's `/v1/chat/completions` shape are translated to Anthropic's `/v1/messages` shape on the way in, and responses (including streams) are translated back on the way out.
4. **Model pinning** — every request is forced to `qwen3.7-plus`, regardless of what the client selects, and it's the only model advertised on `/v1/models`.

## Usage

Run from source:

```bash
node hermes-bearer-proxy.js    # single run
./run-proxy.sh                 # supervised - auto-restarts on crash
```

Or download the compiled executable for your platform from [Releases](https://github.com/isoldeapos/bearerProxy/releases) and run it directly — no Node/Bun required.

On first run you'll be asked for your gateway host, which is saved to `~/.hermes-proxy/config.json` (chmod 600). The gateway address never lives in the source, so this repo can be public. Delete the config file to run setup again.

Then set your client's base URL to `http://127.0.0.1:8787` and keep the proxy running while you use it.

## Configuration

Env vars override the saved config:

| Variable | Default | Purpose |
|---|---|---|
| `TARGET_HOST` / `TARGET_PORT` | from config | Upstream gateway address |
| `UPSTREAM_TIMEOUT_MS` | `60000` | How long to wait for upstream response *headers* (streams aren't killed mid-response) |
| `UPSTREAM_RETRIES` | `2` | Re-attempts on 502/503/504, connection errors, and header timeouts |
| `RETRY_BASE_DELAY_MS` | `500` | Retry backoff base (doubles each attempt) |

Retries only happen before any response byte reaches the client, so they never corrupt a partial response.

## Updates

On startup the proxy checks `version.json` on this repo's `main` branch.

- **Compiled executables** auto-update from GitHub releases and relaunch (opt out with `"auto_update": false` in `~/.hermes-proxy/config.json`). You can also update manually with `--self-update`.
- **Running from source**: update with `git pull`.

## Cutting a release

```bash
# 1. Bump VERSION in hermes-bearer-proxy.js AND version.json (keep in sync)
# 2. Build per-platform executables into dist/
./build-release.sh
# 3. Commit + push, then create a GitHub release tagged v<version>
gh release create v<version> dist/hermes-bearer-proxy-* --title "v<version>"
```

The tag (`v<version>`) and asset filenames must match what `--self-update` expects; `build-release.sh` names them correctly.

## License

Personal, non-commercial use only — see [LICENSE](LICENSE).
