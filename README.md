# AgentSeek API (TypeScript)

**English** | [中文](README.zh-CN.md)

> [!WARNING]
> This project is under active development and is **not production-ready**.
> Pull requests for bug fixes and enhancements are warmly welcomed!

Self-hosted LangGraph Agent Protocol server on **Bun + TypeScript + PostgreSQL**,
with a standalone `agentseek-api` CLI. This is the TypeScript/Bun counterpart of
the Python edition of AgentSeek API, focused on two hard guarantees:

1. **Background runs** — runs execute server-side; an SSE disconnect never kills
   a task.
2. **Resumable streams** — reconnect with `Last-Event-ID` and missed events are
   replayed from the in-memory replay buffer. After a restart, crashed runs are
   requeued and resume from their last checkpoint, and results/step history
   stay queryable from Postgres — only the pre-restart stream replay is lost.

> [!NOTE]
> **Composition, not fork.** The official
> [`@langchain/langgraph-api`](https://www.npmjs.com/package/@langchain/langgraph-api)
> package is used unmodified for HTTP routes, SSE, protocol semantics, the run
> queue, and graph loading. This repo only adds a PostgreSQL storage backend
> injected through the official `startServer(options, { ops })` extension
> point, plus a Bun runtime plugin that swaps the package's in-memory
> checkpointer for `PostgresSaver` at module-load time (no files are ever
> patched). Everything the official server supports
> (Agent Protocol surface, stream modes, LangGraph Studio) works as upstream.

Current release boundary:

- Implemented: assistants, threads, runs, SSE streaming (all stream modes of
  the official server), background + resumable runs, crash recovery,
  human-in-the-loop (`interrupt` / resume with `Command`), Postgres-persisted
  checkpoints, in-memory event replay (Aegra-style)
- Explicitly not implemented: multi-instance / distributed runtime (single
  instance by design), Postgres-backed Store API (the upstream in-memory store
  is used as-is), crons

## 🚀 Quickstart

### Prerequisites

- [Bun](https://bun.sh) 1.3+
- PostgreSQL (the scaffolded `docker-compose.yml` provides Postgres 16 on
  `:5442` — Docker is enough)

### 1. Scaffold a project

```bash
mkdir my-agent && cd my-agent
bun add agentseek-api-ts
bunx agentseek-api init
bun install
```

`init` scaffolds `langgraph.json`, an example graph (`graphs/agent.ts`),
`docker-compose.yml`, and `.env`. No patching is involved: the Postgres
checkpointer is swapped in at runtime by a Bun plugin inside the server
bootstrap, before any `@langchain/langgraph-api` module loads.

### 2. Start Postgres and the server

```bash
docker compose up -d postgres
bun run dev          # agentseek-api dev — hot reload, default port 2024
```

### 3. Check that it is up

```bash
curl http://127.0.0.1:2024/info
```

### 4. Test with the LangGraph SDK

```ts
import { Client } from '@langchain/langgraph-sdk'

const client = new Client({ apiUrl: 'http://localhost:2024' })

// An assistant is auto-registered for every graph in langgraph.json.
const assistants = await client.assistants.search()
const thread = await client.threads.create()

const stream = client.runs.stream(thread.thread_id, 'agent', {
  input: { steps: [] },
  streamMode: 'values',
})
for await (const chunk of stream) {
  console.log(chunk.event, chunk.data)
}
```

### 5. See the core guarantees in action

Create a background, resumable run; watch it; disconnect; reconnect with
`Last-Event-ID`:

```bash
THREAD=$(curl -s -X POST localhost:2024/threads -H 'Content-Type: application/json' -d '{}' | jq -r .thread_id)

RUN=$(curl -s -X POST localhost:2024/threads/$THREAD/runs \
  -H 'Content-Type: application/json' \
  -d '{"assistant_id":"agent","input":{"steps":[]},"stream_mode":["values"],"stream_resumable":true}' \
  | jq -r .run_id)

# Attach, then Ctrl-C mid-run — the run keeps executing server-side
curl -N localhost:2024/threads/$THREAD/runs/$RUN/stream

# Reconnect later: events after seq 3 are replayed from the buffer, then live-tailed
curl -N -H 'Last-Event-ID: 3' localhost:2024/threads/$THREAD/runs/$RUN/stream
```

Or run the scripted version against your dev server:

```bash
bun run verify                        # disconnect + resumable reconnect
bun run scripts/verify-advanced.ts    # token streaming / interrupt / double-text
```

## 🧰 CLI

The package installs `agentseek-api` as the executable.

| Command | What it does |
| --- | --- |
| `init` | Scaffold a project: `langgraph.json`, example graph, `docker-compose.yml`, `.env`. |
| `dev` | Start the server with hot reload (re-runs on graph/config edits). |
| `serve` | Start the server without reload, for containers or production. |

Options: `-p, --port <port>` (default `2024`, or `PORT` env), `-h, --help`.

### Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5442/langgraph` | Postgres connection string (metadata + run queue + checkpoints). |
| `PORT` | `2024` | HTTP port. |
| `HOST` | `0.0.0.0` | Bind address. |
| `N_WORKERS` | `2` | Concurrent run workers. |
| `RUN_EVENTS_TTL_SECONDS` | `3600` | How long finished runs stay replayable before their events are swept. |

## 🗂️ Config

`agentseek-api` reads `langgraph.json` from the working directory — the same
schema as the official LangGraph CLI, so one config file works with this
server, `langgraph dev`, and LangGraph Studio.

```json
{
  "graphs": {
    "agent": "./graphs/agent.ts:graph"
  },
  "env": ".env"
}
```

- `graphs`: graph id → `./path/to/file.ts:exportName` (object form with
  `path` is also accepted)
- `env`: dotenv file path, or an object of environment values (existing
  process env always wins)
- `auth`, `http` (route toggles, CORS), `ui`, `ui_config`: passed through to
  the official server

## 🏗️ How it works

```text
bin/agentseek-api.ts     CLI: init | dev | serve
src/server.ts            bootstrap: config + env → checkpointer plugin → import the app
src/checkpointer.ts      Bun plugin: swaps the upstream in-memory checkpointer for PostgresSaver
src/app.ts               runServer(): PostgresOps + startServer(..., { ops })
src/storage/             PostgresOps — the Ops implementation injected upstream
```

- **Storage**: `assistants`, `assistant_versions`, `threads`, and `runs`
  tables (idempotent DDL at boot). Checkpoints live in `PostgresSaver`'s own
  tables.
- **Run queue**: pending runs are claimed atomically with
  `FOR UPDATE SKIP LOCKED`; runs on the same thread are serialized.
- **Resumable streams**: every published event goes into an in-memory replay
  buffer with a per-run monotonic sequence (capped at 10k events per run). A
  reconnect replays `seq > Last-Event-ID` from the buffer, then tails live.
  Replayability derives from the run's own `stream_resumable` flag — there is
  no first-joiner race.
- **Crash recovery**: on boot, runs stuck in `running` are requeued (up to 3
  attempts) and resume from their last checkpoint.
- **Replay window**: events of finished runs are kept for
  `RUN_EVENTS_TTL_SECONDS` (default 1 hour), then swept.

Postgres is the source of truth for run state, thread state, and checkpoints —
those survive any restart. The stream replay buffer is deliberately in-memory
(the same trade-off Aegra made): a restart loses the event-by-event replay
window, while results and step history remain queryable via `/state`,
`/history`, and `/join`.

### Demo graphs

The repo registers three LLM-free graphs for exercising the runtime:

- `agent` — deterministic 5-step slow graph (~10s), for disconnect/reconnect
- `chat` — fake streaming chat model, for token-level `messages` stream modes
- `approval` — `interrupt()` human-in-the-loop, resumed with a `Command`

## 📚 Use as a library

```ts
import { runServer, loadConfig, applyEnv } from 'agentseek-api-ts'

const config = loadConfig(process.cwd())
applyEnv(config, process.cwd())
await runServer(config)
```

`runServer` registers the checkpointer plugin first and only then loads the
protocol server, so the module-load ordering is handled for you.

## ⚠️ Limitations

- **Single instance by design.** Queue wakeups, cancellation, stream
  coordination, and the event replay buffer are all in-process. Running two
  copies against one database is not supported — a second process (including a
  forgotten `bun run dev`) steals queued runs and its stream events are
  invisible to the first. Multi-instance needs a shared broker (e.g. Redis
  pub/sub + Redis Lists), not just a second process.
- **Stream replay does not survive a restart.** Events are buffered in memory
  (TTL 1 hour after a run finishes, 10k events per run). Run results and step
  history always survive via Postgres checkpoints; what's lost is only the
  ability to replay the token-by-token stream from before the restart.
- **Store API is not Postgres-backed.** The upstream store is used as-is: it
  operates in memory and snapshots to a `.langgraphjs_api.store.json` file in
  the working directory. Data survives a same-directory restart, but is lost
  when the directory changes or a container has no volume, and it is not
  queryable via SQL.
- `@langchain/langgraph-api` is pinned exactly (currently `1.4.2`) because the
  checkpointer plugin substitutes a specific internal module of it; upgrading
  means re-verifying that module path and export still match.

## 🧪 Contributing

```bash
git clone <repo> && cd agentseek-api-ts
bun install
docker compose -f templates/docker-compose.yml up -d postgres

bun run dev      # server on :2024
bun test         # e2e suite (spawns its own server on :2098; needs Postgres up)
bun run check    # prettier + oxlint + tsc --noEmit — must pass (pre-commit runs it)
```

## 🧱 Built On

- [Bun](https://bun.sh) — runtime, test runner, and the `Bun.plugin`
  module-interception API
- [LangGraph](https://github.com/langchain-ai/langgraphjs) — graph runtime
- [`@langchain/langgraph-api`](https://www.npmjs.com/package/@langchain/langgraph-api)
  — HTTP routes, SSE, protocol semantics, run queue (used unmodified; its
  in-memory checkpointer is swapped at runtime via a Bun plugin)
- [`@langchain/langgraph-checkpoint-postgres`](https://www.npmjs.com/package/@langchain/langgraph-checkpoint-postgres)
  — checkpoint persistence (`PostgresSaver`)
- [PostgreSQL](https://www.postgresql.org/) + [node-postgres](https://node-postgres.com/)
  — metadata, run queue, and checkpoints
- [Agent Protocol](https://github.com/langchain-ai/agent-protocol) — external
  compatibility reference for the assistants/threads/runs surface
