// Library entry. Anything that touches @langchain/langgraph-api must only
// load AFTER the checkpointer plugin is registered, so the app module is
// imported lazily inside runServer() instead of re-exported statically.
import type { LangGraphConfig } from './config'
import { DEFAULT_DATABASE_URL, registerPostgresCheckpointer } from './checkpointer'

export { loadConfig, applyEnv, type LangGraphConfig } from './config'
export { registerPostgresCheckpointer, getPostgresCheckpointer, DEFAULT_DATABASE_URL } from './checkpointer'
export type { PostgresOps } from './storage'

export async function runServer(config: LangGraphConfig): Promise<void> {
  process.env.DATABASE_URL ??= DEFAULT_DATABASE_URL
  registerPostgresCheckpointer(process.env.DATABASE_URL)
  const app = await import('./app')
  return app.runServer(config)
}
