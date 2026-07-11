// langgraph.json loader — same schema as the official CLI so one config file
// works across our server, LangGraph Studio and the official tooling.
// IMPORTANT: no @langchain/langgraph-api imports here; this module runs before
// the checkpointer plugin is registered, and that package's modules import the
// checkpointer singleton at module scope.
import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import dotenv from 'dotenv'

export interface HttpConfig {
  app?: string
  disable_assistants?: boolean
  disable_threads?: boolean
  disable_runs?: boolean
  disable_store?: boolean
  disable_meta?: boolean
  cors?: {
    allow_origins?: string[]
    allow_methods?: string[]
    allow_headers?: string[]
    allow_credentials?: boolean
    allow_origin_regex?: string
    expose_headers?: string[]
    max_age?: number
  }
}

export interface LangGraphConfig {
  graphs: Record<string, string>
  env?: string | string[] | Record<string, string>
  auth?: { path?: string; disable_studio_auth?: boolean }
  http?: HttpConfig
  ui?: Record<string, string>
  ui_config?: { shared?: string[] }
}

interface RawConfig {
  graphs?: Record<string, string | { path: string; description?: string }>
  env?: string | string[] | Record<string, string>
  auth?: LangGraphConfig['auth']
  http?: LangGraphConfig['http']
  ui?: LangGraphConfig['ui']
  ui_config?: LangGraphConfig['ui_config']
}

export function loadConfig(cwd: string): LangGraphConfig {
  const file = resolve(cwd, 'langgraph.json')
  if (!existsSync(file)) {
    throw new Error(
      `langgraph.json not found in ${cwd}. Create one with at least: {"graphs": {"agent": "./graphs/agent.ts:graph"}}`,
    )
  }

  const raw = JSON.parse(readFileSync(file, 'utf8')) as RawConfig
  if (raw.graphs == null || typeof raw.graphs !== 'object') {
    throw new Error(`langgraph.json must define a "graphs" object`)
  }

  const graphs: Record<string, string> = {}
  for (const [id, def] of Object.entries(raw.graphs)) {
    const path = typeof def === 'string' ? def : def?.path
    if (typeof path !== 'string' || !path.includes(':')) {
      throw new Error(`Graph "${id}" must be "./path/to/file.ts:exportName" (got ${JSON.stringify(def)})`)
    }
    graphs[id] = path
  }

  return {
    graphs,
    env: raw.env,
    auth: raw.auth,
    http: raw.http,
    ui: raw.ui,
    ui_config: raw.ui_config,
  }
}

// Existing process env always wins (dotenv semantics): config supplies defaults.
export function applyEnv(config: LangGraphConfig, cwd: string): void {
  const env = config.env
  if (env == null) return

  if (typeof env === 'string') {
    dotenv.config({ path: resolve(cwd, env), quiet: true })
    return
  }

  if (Array.isArray(env)) {
    // Array form lists host env vars to pass through (a Docker-build concept);
    // in a local process they are already present, so nothing to do.
    return
  }

  for (const [key, value] of Object.entries(env)) {
    process.env[key] ??= value
  }
}
