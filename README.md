# api-fucker

`api-fucker` is a lightweight OpenAI-compatible gateway for NVIDIA Integrate API forwarding. It exposes `/v1/chat/completions` and `/v1/models`, manages an upstream API key pool, supports per-key proxy routing, and includes an embedded admin console for operations, diagnostics, and traffic control.

## Features

- OpenAI-compatible forwarding for chat completions and model listing.
- Key pool management with enable/disable, pause/restore, balance/error tracking, and per-key concurrency limits.
- Global and per-key upstream base URL / proxy configuration.
- HTTP proxy support with conservative HTTP/1.1 upstream transport for unstable residential proxies.
- Managed VPS proxy pool with round-robin or per-key binding modes.
- Minimal VPS proxy client with HTTP/HTTPS CONNECT forwarding and 10s status reports.
- Automatic retries for transient upstream and transport failures.
- Admin console with status filtering, last-used sorting, model fetching, and key testing.
- Real-time request diagnostics with a millisecond network timeline.
- Last request/response capture per key, including headers, payloads, errors, and network chain details.
- Request-port IP allowlist/blocklist with exact and wildcard prefix matching.
- Separate request API and admin ports.

## Architecture

The service runs two HTTP servers:

| Server | Default Port | Purpose |
| --- | ---: | --- |
| Request API | `8080` | OpenAI-compatible `/v1/*` forwarding endpoints |
| Admin Console | `8081` | Web admin UI and `/api/admin/*` endpoints |

Persistent state is stored in a local bbolt database specified by `DATA_PATH`.

## Quick Start

```bash
ADMIN_TOKEN=change-me \
PORT=8080 \
ADMIN_PORT=8081 \
DATA_PATH=./data.db \
go run ./cmd/server
```

Open the admin console:

```text
http://127.0.0.1:8081/admin
```

Log in with `ADMIN_TOKEN`, then configure:

1. Global upstream base URL, for example `https://integrate.api.nvidia.com`.
2. Optional global HTTP proxy.
3. One or more NVIDIA API keys.
4. Optional request IP allowlist/blocklist.

## Docker

Run with the published image:

```bash
docker run -d \
  --name api-fucker \
  -p 8080:8080 \
  -p 8081:8081 \
  -e ADMIN_TOKEN=change-me \
  -e DATA_PATH=/data/data.db \
  -v api-fucker-data:/data \
  pyroflux/api-fucker:latest
```

Build locally for amd64:

```bash
docker buildx build --platform linux/amd64 -t pyroflux/api-fucker:latest .
```

## Configuration

### Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `ADMIN_TOKEN` | empty | Admin login token. Must be set to use the admin console. |
| `PORT` | `8080` | Request API port. |
| `ADMIN_PORT` | `8081` | Admin console/API port. Must be different from `PORT`. |
| `DATA_PATH` | `./data.db` | bbolt database path. Use `/data/data.db` in Docker. |
| `PROXY_NODE_TOKEN` | empty | Shared token used by VPS proxy clients to report status and authenticate proxy traffic. Required when the proxy pool is used. |

### Global Admin Settings

| Setting | Description |
| --- | --- |
| Default upstream base URL | Upstream base URL used when a key does not override it. Example: `https://integrate.api.nvidia.com`. |
| Default proxy URL | Optional HTTP proxy used when a key does not override it. Supports proxy auth in the URL. |
| VPS proxy pool | When enabled, all API traffic uses online enabled VPS proxy nodes instead of key/global proxy settings. |
| Proxy mode | `round_robin` rotates online proxy nodes; `key_binding` keeps each upstream API key on a stable node when possible. |
| Request IP allowlist | Optional list of IP patterns allowed to access `/v1/*`. Empty means allow all unless blocked. |
| Request IP blocklist | Optional list of IP patterns denied from accessing `/v1/*`. Blocklist wins over allowlist. |

IP rules support exact and wildcard-prefix matching:

```text
127.0.0.1
192.*
10.0.*
```

Rules may be separated by newlines or commas. IP access control applies only to the request API port and does not block the admin port.

## API Usage

### Chat Completions

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"deepseek-ai/deepseek-v4-flash","messages":[{"role":"user","content":"hello"}]}'
```

The gateway selects an available configured key, injects the upstream `Authorization` header, and forwards the request to:

```text
{upstreamBaseUrl}/v1/chat/completions
```

Streaming requests are supported when the request body contains `"stream": true`.

### Models

```bash
curl http://127.0.0.1:8080/v1/models
```

The gateway forwards to:

```text
{upstreamBaseUrl}/v1/models
```

## Key Management

Keys can be added manually or imported in bulk from the admin console. Bulk import accepts one key per line:

```text
nvapi-xxxx
name,nvapi-xxxx
name,nvapi-xxxx,https://integrate.api.nvidia.com,http://127.0.0.1:7890
```

Columns are:

1. Name (optional when only a key is provided).
2. API key.
3. Per-key upstream base URL override.
4. Per-key proxy URL override.

Per-key settings take precedence over global settings.

## Diagnostics

The admin console records the latest request details for each key:

- Request URL, headers, and body.
- Response status, headers, and body.
- Upstream or transport error text.
- Network timeline with millisecond offsets.

Timeline events distinguish direct and proxied requests, including DNS resolution, proxy connection, CONNECT tunnel establishment, upstream TLS handshake, request send, first byte, response read, retry, and failure steps.

## Proxy Notes

Many residential proxies are unstable with HTTP/2 after `CONNECT`. The gateway disables upstream HTTP/2 and uses HTTP/1.1 to improve compatibility with NVIDIA Integrate API and HTTP proxy tunnels.

Use the admin key test button to verify a proxy/key/model combination before production traffic.

## VPS Proxy Pool

Set a shared node token on the main service, then enable the VPS proxy pool in the admin console:

```bash
ADMIN_TOKEN=change-me \
PROXY_NODE_TOKEN=proxy-node-secret \
PORT=8080 \
ADMIN_PORT=8081 \
DATA_PATH=./data.db \
go run ./cmd/server
```

Run the proxy client on each VPS with only two required values:

```bash
SERVER_URL=http://MAIN_SERVER_IP:8081 \
PROXY_NODE_TOKEN=proxy-node-secret \
./proxy-client
```

Defaults:

- `LISTEN_ADDR=:9070`
- `NODE_ID` is generated once and saved locally.
- `NODE_NAME` uses the host name.
- `PUBLIC_PROXY_URL` is derived by the server from the reporting IP and listen port.
- `REPORT_INTERVAL=10s`
- `MAX_CONCURRENCY=100`

Docker client example:

```bash
docker run -d \
  --name api-fucker-proxy \
  -p 9070:9070 \
  -e SERVER_URL=http://MAIN_SERVER_IP:8081 \
  -e PROXY_NODE_TOKEN=proxy-node-secret \
  pyroflux/api-fucker:latest \
  /app/proxy-client
```

If the VPS is behind NAT, has multiple public IPs, or uses port mapping, set `PUBLIC_PROXY_URL` explicitly, for example `http://203.0.113.10:9070`.

### Batch deploy with systemd

Create a VPS list:

```bash
cat > vps.txt <<'EOF'
root@203.0.113.10
root@203.0.113.11
EOF
```

Build one Linux binary locally:

```bash
./scripts/build-proxy-client.sh
```

Deploy it to all VPS hosts via SSH and install the systemd service:

```bash
SERVER_URL=http://MAIN_SERVER_IP:8081 \
PROXY_NODE_TOKEN=proxy-node-secret \
./scripts/deploy-proxy-clients.sh
```

Check remote service status and recent logs:

```bash
./scripts/check-proxy-clients.sh
```

## Development

Run tests:

```bash
go test ./...
```

Run locally:

```bash
ADMIN_TOKEN=dev-token PORT=8080 ADMIN_PORT=8081 DATA_PATH=./data.db go run ./cmd/server
```

## Security Notes

- Keep the admin port private and set a strong `ADMIN_TOKEN`.
- If VPS proxy reporting is enabled, restrict the admin port to trusted VPS IPs where possible and keep `PROXY_NODE_TOKEN` secret.
- Store `DATA_PATH` on persistent storage and protect the database file.
- Proxy credentials are accepted in proxy URLs but are redacted from diagnostics where displayed.
- Request IP allow/block rules are evaluated from `RemoteAddr`; forwarded headers are intentionally not trusted.
