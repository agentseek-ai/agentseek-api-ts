import { startServer } from '@langchain/langgraph-api/server'
import type { LangGraphConfig } from './config'
import { assertCheckpointerIntercepted, getPostgresCheckpointer } from './checkpointer'
import { PostgresOps } from './storage'

export async function runServer(config: LangGraphConfig): Promise<void> {
  // `||` (not `??`): a blank PORT= line in .env yields '' → Number('') === 0
  // would bind a random ephemeral port.
  const PORT = Number(process.env.PORT || 2024)

  // Registered by the bootstrap before this module (and therefore
  // @langchain/langgraph-api) was loaded; throws if that ordering was violated
  // or if the plugin no longer intercepts the pinned package's module.
  const { saver, pool } = getPostgresCheckpointer()
  await assertCheckpointerIntercepted()
  await saver.setup()

  const ops = new PostgresOps(pool, saver)
  await ops.initialize()

  // startServer's types are zod OUTPUT shapes: defaulted booleans are required.
  const auth = config.auth
    ? {
        path: config.auth.path,
        disable_studio_auth: config.auth.disable_studio_auth ?? false,
      }
    : undefined
  const http = config.http
    ? {
        app: config.http.app,
        disable_assistants: config.http.disable_assistants ?? false,
        disable_threads: config.http.disable_threads ?? false,
        disable_runs: config.http.disable_runs ?? false,
        disable_store: config.http.disable_store ?? false,
        disable_meta: config.http.disable_meta ?? false,
        cors: config.http.cors,
      }
    : undefined

  const { host, cleanup } = await startServer(
    {
      port: PORT,
      nWorkers: Number(process.env.N_WORKERS || 2),
      host: process.env.HOST ?? '0.0.0.0',
      cwd: process.cwd(),
      graphs: config.graphs,
      auth,
      http,
      ui: config.ui,
      ui_config: config.ui_config,
    },
    { ops },
  )

  console.log(`langgraph-bun-server listening at ${host} (storage: postgres)`)

  const shutdown = async (): Promise<void> => {
    ops.stop()
    await cleanup()
    await pool.end()
    process.exit(0)
  }

  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)
}
