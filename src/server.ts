// Bootstrap for `bun run src/server.ts` (dev/serve): load config + env, then
// hand over to the library entry, which registers the checkpointer plugin
// BEFORE any @langchain/langgraph-api module loads — that ordering lives in
// exactly one place (src/index.ts).
import { applyEnv, loadConfig } from './config'
import { runServer } from './index'

const cwd = process.cwd()
const config = loadConfig(cwd)
applyEnv(config, cwd)

await runServer(config)
