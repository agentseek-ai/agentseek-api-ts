#!/usr/bin/env bun
// agentseek-api — LangGraph server on Bun + Postgres.
//   agentseek-api init          scaffold langgraph.json, example graph, compose, env files
//   agentseek-api dev           start with hot reload (reads ./langgraph.json)
//   agentseek-api serve         start for production
import { parseArgs } from 'node:util'
import { existsSync } from 'node:fs'
import { resolve } from 'node:path'

const HELP = `agentseek-api — LangGraph Agent Protocol server on Bun + PostgreSQL

Usage:
  agentseek-api init            Scaffold a project (langgraph.json, example graph,
                        docker-compose.yml, .env.example)
  agentseek-api dev             Start the server with hot reload
  agentseek-api serve           Start the server (production, no watch)

Options:
  -p, --port <port>     Port to listen on (default 2024, or PORT env)
  -h, --help            Show this help

Environment:
  DATABASE_URL          Postgres connection string (required; init generates compose)
  RUN_EVENTS_TTL_SECONDS  Replay window after a run finishes (default 3600)
  N_WORKERS             Concurrent run workers (default 2)
`

const { values, positionals } = parseArgs({
  args: process.argv.slice(2),
  options: {
    port: { type: 'string', short: 'p' },
    help: { type: 'boolean', short: 'h' },
  },
  allowPositionals: true,
})

const command = positionals[0]

if (values.help || command == null) {
  console.log(HELP)
  process.exit(command == null && !values.help ? 1 : 0)
}

if (values.port) process.env.PORT = values.port

const serverEntry = resolve(import.meta.dir, '../src/server.ts')

switch (command) {
  case 'init': {
    const { init } = await import('../src/cli/init')
    await init(process.cwd())
    break
  }
  case 'dev': {
    requireConfig()
    // Re-exec under --watch so edits to graphs/config restart the server.
    const proc = Bun.spawn(['bun', 'run', '--watch', serverEntry], {
      cwd: process.cwd(),
      env: process.env as Record<string, string>,
      stdout: 'inherit',
      stderr: 'inherit',
    })
    const forward = (): void => {
      proc.kill()
      process.exit(0)
    }
    process.on('SIGINT', forward)
    process.on('SIGTERM', forward)
    await proc.exited
    process.exit(proc.exitCode ?? 0)
  }
  case 'serve': {
    requireConfig()
    await import(serverEntry)
    break
  }
  default: {
    console.error(`Unknown command: ${command}\n`)
    console.log(HELP)
    process.exit(1)
  }
}

function requireConfig(): void {
  if (!existsSync(resolve(process.cwd(), 'langgraph.json'))) {
    console.error('langgraph.json not found in the current directory. Run `agentseek-api init` first.')
    process.exit(1)
  }
}
