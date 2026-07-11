// Bootstrap ordering is load-bearing: (1) config + env, (2) register the
// Postgres checkpointer plugin — it must intercept @langchain/langgraph-api's
// checkpoint module before any of that package's modules are evaluated —
// (3) only then load the app via dynamic import.
import { applyEnv, loadConfig } from './config'
import { DEFAULT_DATABASE_URL, registerPostgresCheckpointer } from './checkpointer'

const cwd = process.cwd()
const config = loadConfig(cwd)
applyEnv(config, cwd)

process.env.DATABASE_URL ??= DEFAULT_DATABASE_URL
registerPostgresCheckpointer(process.env.DATABASE_URL)

const { runServer } = await import('./app')
await runServer(config)
