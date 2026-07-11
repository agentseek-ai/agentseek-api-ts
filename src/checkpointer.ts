// Replaces @langchain/langgraph-api's in-memory checkpointer singleton with a
// PostgresSaver — via a Bun runtime plugin instead of patching node_modules.
// The plugin intercepts the load of the package's dist/storage/checkpoint.mjs
// and substitutes our saver as the `checkpointer` export, so graph execution
// persists checkpoints to Postgres. No files on disk are modified.
//
// Ordering is load-bearing: registerPostgresCheckpointer() must run BEFORE any
// @langchain/langgraph-api module is evaluated (they import the singleton at
// module scope). This module therefore must not import that package, and the
// app is only ever loaded via dynamic import after registration.
import pg from 'pg'
import { PostgresSaver } from '@langchain/langgraph-checkpoint-postgres'

export const DEFAULT_DATABASE_URL = 'postgresql://postgres:postgres@localhost:5442/langgraph'

const EXCLUDED_KEYS = ['checkpoint_ns', 'checkpoint_id', 'run_id', 'thread_id']

class ApiPostgresSaver extends PostgresSaver {
  // The upstream server boot calls checkpointer.initialize(cwd) and flushes
  // the returned handle on cleanup; map that onto PostgresSaver's setup().
  async initialize(_cwd: string): Promise<{ flush: () => Promise<void> }> {
    await this.setup()
    return { flush: async () => {} }
  }

  // clear/delete/copy are handled by PostgresOps at the storage layer and are
  // no-ops here (same division of labor the upstream in-memory impl relies on).
  clear(): void {}
  async delete(_threadId: string, _runId?: string | null): Promise<void> {}
  copy(_fromThreadId: string, _toThreadId: string): void {}

  // Keep the upstream behavior of folding configurable + config.metadata into
  // checkpoint metadata (used by state-history metadata filters).
  override async put(...args: Parameters<PostgresSaver['put']>): ReturnType<PostgresSaver['put']> {
    const [config, checkpoint, metadata, newVersions] = args
    const folded = {
      ...Object.fromEntries(
        Object.entries(config.configurable ?? {}).filter(
          ([key]) => !key.startsWith('__') && !EXCLUDED_KEYS.includes(key),
        ),
      ),
      ...config.metadata,
      ...metadata,
    } as typeof metadata
    return super.put(config, checkpoint, folded, newVersions)
  }
}

export interface PostgresCheckpointer {
  saver: PostgresSaver
  pool: pg.Pool
}

let registered: PostgresCheckpointer | undefined

export function registerPostgresCheckpointer(databaseUrl: string): PostgresCheckpointer {
  if (registered) return registered

  const pool = new pg.Pool({ connectionString: databaseUrl, max: 10 })
  const saver = new ApiPostgresSaver(pool)

  Bun.plugin({
    name: 'agentseek-postgres-checkpointer',
    setup(build) {
      build.onLoad({ filter: /langgraph-api[/\\]dist[/\\]storage[/\\]checkpoint\.mjs$/ }, () => ({
        loader: 'object',
        exports: { checkpointer: saver },
      }))
    },
  })

  registered = { saver, pool }
  return registered
}

export function getPostgresCheckpointer(): PostgresCheckpointer {
  if (!registered) {
    throw new Error(
      'Postgres checkpointer is not registered: checkpoints would silently stay in memory.\n' +
        'Call registerPostgresCheckpointer(DATABASE_URL) before any @langchain/langgraph-api ' +
        'module is imported (see src/server.ts for the bootstrap ordering).',
    )
  }
  return registered
}
