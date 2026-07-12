# AgentSeek API (TypeScript)

[English](README.md) | **中文**

> [!WARNING]
> 本项目正在积极开发中，**尚未达到生产可用状态**。
> 欢迎提交 Pull Request 来修复 Bug 或贡献增强功能！

基于 **Bun + TypeScript + PostgreSQL** 的自托管 LangGraph Agent Protocol
服务器，附带独立的 `agentseek-api` CLI。这是 AgentSeek API Python 版的
TypeScript/Bun 对应实现，聚焦两个核心保证：

1. **后台运行（Background runs）** — run 在服务端执行；SSE 断开连接绝不会
   杀死任务。
2. **可恢复流（Resumable streams）** — 携带 `Last-Event-ID` 重连即可从
   内存回放缓冲补齐错过的事件。服务器重启后，崩溃的 run 会被重新入队并从
   最后一个 checkpoint 继续执行，结果与逐步历史始终可从 Postgres 查询——
   丢失的只有重启前那段流的逐事件回放。

> [!NOTE]
> **组合，而非 fork。** 官方的
> [`@langchain/langgraph-api`](https://www.npmjs.com/package/@langchain/langgraph-api)
> 包被原样用于 HTTP 路由、SSE、协议语义、run 队列与图加载。本仓库只做了
> 两件事：通过官方扩展点 `startServer(options, { ops })` 注入一个
> PostgreSQL 存储后端，外加一个 Bun 运行时插件，在模块加载时把该包内置的
> 内存 checkpointer 换成 `PostgresSaver`（不会修改任何磁盘上的文件）。
> 官方服务器支持的一切（Agent Protocol
> 接口、各种流模式、LangGraph Studio）均与上游行为一致。

当前版本边界：

- 已实现：assistants、threads、runs、SSE 流式输出（官方服务器的全部流
  模式）、后台 + 可恢复 run、崩溃恢复、Human-in-the-loop（`interrupt` /
  `Command` 恢复）、基于 Postgres 的 checkpoint 持久化、内存事件回放
  （Aegra 同款取舍）
- 明确不实现：多实例 / 分布式运行时（按设计即单实例）、基于 Postgres 的
  Store API（沿用上游的内存实现）

## 🚀 快速上手

### 前置条件

- [Bun](https://bun.sh) 1.3+
- PostgreSQL（脚手架生成的 `docker-compose.yml` 提供跑在 `:5442` 的
  Postgres 16 —— 有 Docker 就够了）

### 1. 脚手架初始化项目

```bash
mkdir my-agent && cd my-agent
bun add agentseek-api-ts
bunx agentseek-api init
bun install
```

`init` 会生成 `langgraph.json`、示例图（`graphs/agent.ts`）、
`docker-compose.yml` 和 `.env`。整个过程不涉及任何补丁：Postgres
checkpointer 由服务器引导阶段的一个 Bun 插件在运行时换入，发生在任何
`@langchain/langgraph-api` 模块加载之前。

### 2. 启动 Postgres 与服务器

```bash
docker compose up -d postgres
bun run dev          # agentseek-api dev —— 热重载，默认端口 2024
```

### 3. 验证服务是否启动

```bash
curl http://127.0.0.1:2024/info
```

### 4. 使用 LangGraph SDK 进行测试

```ts
import { Client } from '@langchain/langgraph-sdk'

const client = new Client({ apiUrl: 'http://localhost:2024' })

// langgraph.json 中注册的每个图都会自动创建一个 assistant。
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

### 5. 两个核心保证

创建一个后台的可恢复 run；观察它；断开连接；再带着 `Last-Event-ID`
重连：

```bash
THREAD=$(curl -s -X POST localhost:2024/threads -H 'Content-Type: application/json' -d '{}' | jq -r .thread_id)

RUN=$(curl -s -X POST localhost:2024/threads/$THREAD/runs \
  -H 'Content-Type: application/json' \
  -d '{"assistant_id":"agent","input":{"steps":[]},"stream_mode":["values"],"stream_resumable":true}' \
  | jq -r .run_id)

# 接上流后中途 Ctrl-C —— run 会继续在服务端执行
curl -N localhost:2024/threads/$THREAD/runs/$RUN/stream

# 稍后重连：seq 大于 3 的事件会从回放缓冲补发，然后继续实时跟流
curl -N -H 'Last-Event-ID: 3' localhost:2024/threads/$THREAD/runs/$RUN/stream
```

也可以对着你的 dev 服务器运行脚本化的验证：

```bash
bun run verify                        # 断线 + 可恢复重连
bun run scripts/verify-advanced.ts    # token 流式 / interrupt / double-text
```

## 🧰 CLI

本包安装名为 `agentseek-api` 的可执行文件。

| 命令 | 作用 |
| --- | --- |
| `init` | 脚手架初始化：`langgraph.json`、示例图、`docker-compose.yml`、`.env`。 |
| `dev` | 带热重载启动服务器（图/配置变更时自动重启）。 |
| `serve` | 不带热重载启动服务器，适用于容器或生产环境。 |

参数：`-p, --port <port>`（默认 `2024`，或 `PORT` 环境变量）、`-h, --help`。

### 环境变量

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5442/langgraph` | Postgres 连接串（元数据 + run 队列 + checkpoint）。 |
| `PORT` | `2024` | HTTP 端口。 |
| `HOST` | `0.0.0.0` | 绑定地址。 |
| `N_WORKERS` | `2` | 并发 run worker 数量。 |
| `RUN_EVENTS_TTL_SECONDS` | `3600` | resumable run 结束后保持可回放的时长（非 resumable 的缓冲约 30 秒后即回收）。 |

## 🗂️ 配置

`agentseek-api` 从工作目录读取 `langgraph.json` —— 与官方 LangGraph CLI
使用相同的 schema，因此同一份配置文件可以同时用于本服务器、
`langgraph dev` 和 LangGraph Studio。

```json
{
  "graphs": {
    "agent": "./graphs/agent.ts:graph"
  },
  "env": ".env"
}
```

- `graphs`：graph id → `./path/to/file.ts:exportName`（也接受带 `path`
  字段的对象形式）
- `env`：dotenv 文件路径，或环境变量键值对象（已存在的进程环境变量
  始终优先）
- `auth`、`http`（路由开关、CORS）、`ui`、`ui_config`：透传给官方服务器

## 🏗️ 工作原理

```text
bin/agentseek-api.ts     CLI：init | dev | serve
src/server.ts            引导：配置 + 环境变量 → 注册 checkpointer 插件 → 动态导入应用
src/checkpointer.ts      Bun 插件：把上游的内存 checkpointer 换成 PostgresSaver
src/app.ts               runServer()：PostgresOps + startServer(..., { ops })
src/storage/             PostgresOps —— 注入到上游的 Ops 实现
```

- **存储**：`assistants`、`assistant_versions`、`threads`、`runs` 四张表
  （启动时执行幂等 DDL）。checkpoint 存放在 `PostgresSaver` 自己的表中。
- **Run 队列**：待执行的 run 通过 `FOR UPDATE SKIP LOCKED` 原子认领；
  同一 thread 上的 run 串行执行。
- **可恢复流**：每个发布的事件进入内存回放缓冲，带 run 内单调递增的
  seq（每 run 上限 1 万条）。重连时先从缓冲回放 `seq > Last-Event-ID`
  的事件，再继续实时跟流。可回放性由 run 自身的 `stream_resumable`
  标志决定 —— 不存在"谁先加入"的竞态。
- **崩溃恢复**：启动时把卡在 `running` 状态的 run 重新入队（最多 3 次
  尝试），从最后一个 checkpoint 继续执行。
- **回放窗口**：resumable run 结束后事件缓冲保留 `RUN_EVENTS_TTL_SECONDS`
  （默认 1 小时）后清理；非 resumable 的缓冲在 run 结束约 30 秒后即回收。

run 状态、thread 状态与 checkpoint 以 Postgres 为事实来源，任何重启都不
丢失；流的回放缓冲则刻意放在内存（与 Aegra 相同的取舍）——重启会丢掉
逐事件回放窗口，但结果与逐步历史仍可通过 `/state`、`/history`、`/join`
查询。

### 示例图

仓库注册了三个无需 LLM 的图，用于演练运行时：

- `agent` —— 确定性的 5 步慢速图（约 10 秒），用于断线/重连测试
- `chat` —— 假的流式聊天模型，用于 token 级 `messages` 流模式
- `approval` —— `interrupt()` Human-in-the-loop，用 `Command` 恢复

## 📚 作为库使用

```ts
import { runServer, loadConfig, applyEnv } from 'agentseek-api-ts'

const config = loadConfig(process.cwd())
applyEnv(config, process.cwd())
await runServer(config)
```

`runServer` 会先注册 checkpointer 插件，然后才加载协议服务器——模块
加载顺序已经替你处理好了。

## ⚠️ 限制

- **按设计即单实例。** 队列唤醒、取消、流协调、事件回放缓冲全部在进程
  内。让两个实例共用一个数据库是不受支持的 —— 第二个进程（包括忘记关的
  `bun run dev`）会抢走队列里的 run，而它产生的流事件对第一个进程不可见。
  多实例需要共享的 broker（Redis pub/sub + Redis List），而不是简单地再
  起一个进程。
- **流回放不跨重启。** 事件缓存在内存（resumable run 结束后保留 1 小时，
  非 resumable 约 30 秒，每 run 上限 1 万条）。run 的结果与逐步历史始终由
  Postgres checkpoint 保障；丢的只是重启前 token 级流的回放能力。
- **Store API 未接入 Postgres。** 沿用上游实现：数据在内存中操作，仅快照
  到工作目录的 `.langgraphjs_api.store.json` 文件。同目录重启可恢复，但
  换目录、容器未挂卷时数据即丢失，且无法用 SQL 访问。
- `@langchain/langgraph-api` 被精确锁定版本（当前为 `1.4.2`），因为
  checkpointer 插件替换的是它的一个内部模块；升级时需要重新确认该模块
  路径与导出仍然匹配。

## 🧪 贡献

```bash
git clone <repo> && cd agentseek-api-ts
bun install
docker compose -f templates/docker-compose.yml up -d postgres

bun run dev      # 服务器跑在 :2024
bun test         # 单元 + e2e 套件（e2e 自行拉起 :2098 的服务器；需要 Postgres 已启动）
bun run check    # prettier + oxlint + tsc --noEmit —— 必须通过（pre-commit 会执行）
```

## 🧱 构建基础

- [Bun](https://bun.sh) —— 运行时、测试运行器，以及 `Bun.plugin`
  模块拦截 API
- [LangGraph](https://github.com/langchain-ai/langgraphjs) —— 图运行时
- [`@langchain/langgraph-api`](https://www.npmjs.com/package/@langchain/langgraph-api)
  —— HTTP 路由、SSE、协议语义、run 队列（原样使用；其内存 checkpointer
  由 Bun 插件在运行时换出）
- [`@langchain/langgraph-checkpoint-postgres`](https://www.npmjs.com/package/@langchain/langgraph-checkpoint-postgres)
  —— checkpoint 持久化（`PostgresSaver`）
- [PostgreSQL](https://www.postgresql.org/) + [node-postgres](https://node-postgres.com/)
  —— 元数据、run 队列与 checkpoint
- [Agent Protocol](https://github.com/langchain-ai/agent-protocol) ——
  assistants/threads/runs 接口的对外兼容性参考
