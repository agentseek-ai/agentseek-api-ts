import { HTTPException } from 'hono/http-exception'
import type pg from 'pg'
import { v7 as uuid7 } from '@langchain/core/utils/uuid'
import { Command, Send } from '@langchain/langgraph'
import type { StateSnapshot } from '@langchain/langgraph'
import type { PostgresSaver } from '@langchain/langgraph-checkpoint-postgres'
import type { RunBroker } from './broker'
import { handleAuthEvent, isAuthMatching, type AuthContext } from '@langchain/langgraph-api/auth'
import { getGraph } from '@langchain/langgraph-api/graph'
import type {
  CheckpointPayload,
  Metadata,
  OnConflictBehavior,
  RunCommand,
  RunnableConfig,
  Thread,
  ThreadsRepo,
  ThreadsStateRepo,
  ThreadStatus,
} from '@langchain/langgraph-api/storage'

interface ThreadRow {
  thread_id: string
  status: ThreadStatus
  config: RunnableConfig
  metadata: Metadata
  values: Record<string, unknown> | null
  interrupts: Record<string, unknown> | null
  created_at: Date
  updated_at: Date
}

const toThread = (row: ThreadRow): Thread => ({
  thread_id: row.thread_id,
  status: row.status,
  config: row.config ?? {},
  metadata: row.metadata ?? {},
  values: row.values ?? undefined,
  interrupts: row.interrupts ?? undefined,
  created_at: row.created_at,
  updated_at: row.updated_at,
})

// Replicated from @langchain/langgraph-api src/command.mts (MIT, not exported).
const getLangGraphCommand = (command: RunCommand): Command => {
  const goto = command.goto != null && !Array.isArray(command.goto) ? [command.goto] : command.goto

  return new Command({
    goto: goto?.map((item) => (typeof item !== 'string' ? new Send(item.node, item.input) : item)),
    update: command.update,
    resume: command.resume,
  })
}

const SORTABLE = new Set(['thread_id', 'status', 'created_at', 'updated_at'])

export class PgThreads implements ThreadsRepo {
  public readonly state: ThreadsStateRepo

  constructor(
    private readonly pool: pg.Pool,
    private readonly saver: PostgresSaver,
    private readonly broker: RunBroker,
  ) {
    this.state = new PgThreadsState(pool, this, saver)
  }

  async *search(
    options: {
      metadata?: Metadata
      ids?: string[]
      status?: ThreadStatus
      values?: Record<string, unknown>
      limit: number
      offset: number
      sort_by?: 'thread_id' | 'status' | 'created_at' | 'updated_at'
      sort_order?: 'asc' | 'desc'
      select?: string[]
    },
    auth: AuthContext | undefined,
  ): AsyncGenerator<{ thread: Thread; total: number }> {
    const [filters] = await handleAuthEvent(auth, 'threads:search', {
      metadata: options.metadata,
      status: options.status,
      values: options.values,
      limit: options.limit,
      offset: options.offset,
    })

    const where: string[] = []
    const params: unknown[] = []
    if (options.metadata) {
      params.push(JSON.stringify(options.metadata))
      where.push(`metadata @> $${params.length}::jsonb`)
    }
    if (options.values) {
      params.push(JSON.stringify(options.values))
      where.push(`"values" @> $${params.length}::jsonb`)
    }
    if (options.status) {
      params.push(options.status)
      where.push(`status = $${params.length}`)
    }
    if (options.ids?.length) {
      params.push(options.ids)
      where.push(`thread_id = ANY($${params.length}::uuid[])`)
    }

    const sortBy = SORTABLE.has(options.sort_by ?? '') ? options.sort_by : 'created_at'
    const sortOrder = options.sort_order === 'asc' ? 'ASC' : 'DESC'
    params.push(options.limit, options.offset)

    const res = await this.pool.query<ThreadRow & { total: string }>(
      `SELECT *, COUNT(*) OVER() AS total FROM threads
       ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
       ORDER BY ${sortBy} ${sortOrder}
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params,
    )

    for (const row of res.rows) {
      if (!isAuthMatching(row.metadata, filters)) continue
      yield { thread: toThread(row), total: Number(row.total) }
    }
  }

  // Auth-scoped thread lookup shared by get/delete/copy: null when the thread
  // is missing OR the caller may not see it (callers pick their own status).
  private async fetchThreadWithAuth(
    thread_id: string,
    filters: Parameters<typeof isAuthMatching>[1],
  ): Promise<ThreadRow | null> {
    const res = await this.pool.query<ThreadRow>(`SELECT * FROM threads WHERE thread_id = $1`, [thread_id])
    const row = res.rows[0]
    if (!row || !isAuthMatching(row.metadata, filters)) return null
    return row
  }

  async get(thread_id: string, auth: AuthContext | undefined): Promise<Thread> {
    const [filters] = await handleAuthEvent(auth, 'threads:read', { thread_id })

    const row = await this.fetchThreadWithAuth(thread_id, filters)
    if (!row) {
      throw new HTTPException(404, {
        message: `Thread with ID ${thread_id} not found`,
      })
    }
    return toThread(row)
  }

  async put(
    thread_id: string,
    options: { metadata?: Metadata; if_exists: OnConflictBehavior },
    auth: AuthContext | undefined,
  ): Promise<Thread> {
    const [filters, mutable] = await handleAuthEvent(auth, 'threads:create', {
      thread_id,
      metadata: options.metadata,
      if_exists: options.if_exists,
    })

    const existing = await this.pool.query<ThreadRow>(`SELECT * FROM threads WHERE thread_id = $1`, [thread_id])
    if (existing.rows[0]) {
      const row = existing.rows[0]
      if (!isAuthMatching(row.metadata, filters) || options.if_exists === 'raise') {
        throw new HTTPException(409, { message: 'Thread already exists' })
      }
      return toThread(row)
    }

    const inserted = await this.pool.query<ThreadRow>(
      `INSERT INTO threads (thread_id, status, metadata, config)
       VALUES ($1, 'idle', $2::jsonb, '{}')
       ON CONFLICT (thread_id) DO NOTHING
       RETURNING *`,
      [thread_id, JSON.stringify(mutable?.metadata ?? {})],
    )
    const row = inserted.rows[0]
    if (!row) return this.get(thread_id, auth)
    return toThread(row)
  }

  async patch(threadId: string, options: { metadata?: Metadata }, auth: AuthContext | undefined): Promise<Thread> {
    const [filters, mutable] = await handleAuthEvent(auth, 'threads:update', {
      thread_id: threadId,
      metadata: options.metadata,
    })

    const res = await this.pool.query<ThreadRow>(`SELECT * FROM threads WHERE thread_id = $1`, [threadId])
    const row = res.rows[0]
    if (!row || !isAuthMatching(row.metadata, filters)) {
      throw new HTTPException(404, { message: 'Thread not found' })
    }

    const metadata = mutable.metadata != null ? { ...row.metadata, ...mutable.metadata } : row.metadata

    const updated = await this.pool.query<ThreadRow>(
      `UPDATE threads SET metadata = $2::jsonb, updated_at = now()
       WHERE thread_id = $1 RETURNING *`,
      [threadId, JSON.stringify(metadata ?? {})],
    )
    return toThread(updated.rows[0]!)
  }

  async setStatus(threadId: string, options: { checkpoint?: CheckpointPayload; exception?: Error }): Promise<void> {
    const hasNext = options.checkpoint != null && options.checkpoint.next.length > 0

    const pendingRes = await this.pool.query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM runs WHERE thread_id = $1 AND status = 'pending'`,
      [threadId],
    )
    const hasPendingRuns = Number(pendingRes.rows[0]!.count) > 0

    let status: ThreadStatus = 'idle'
    if (options.exception != null) status = 'error'
    else if (hasNext) status = 'interrupted'
    else if (hasPendingRuns) status = 'busy'

    const values = options.checkpoint != null ? JSON.stringify(options.checkpoint.values) : null
    const interrupts =
      options.checkpoint != null
        ? JSON.stringify(
            options.checkpoint.tasks.reduce<Record<string, unknown>>((acc, task) => {
              if (task.interrupts) acc[task.id] = task.interrupts
              return acc
            }, {}),
          )
        : null

    const res = await this.pool.query(
      `UPDATE threads
       SET status = $2, "values" = $3::jsonb, interrupts = $4::jsonb, updated_at = now()
       WHERE thread_id = $1`,
      [threadId, status, values, interrupts],
    )
    if (res.rowCount === 0) {
      // Called from the upstream worker's finally block: the thread may have
      // been deleted while its run executed. Throwing would kill the
      // fire-and-forget worker loop, so just log and return.
      console.warn(`[storage] setStatus for missing thread ${threadId}; ignoring`)
    }
  }

  async delete(thread_id: string, auth: AuthContext | undefined): Promise<string[]> {
    const [filters] = await handleAuthEvent(auth, 'threads:delete', { thread_id })

    const row = await this.fetchThreadWithAuth(thread_id, filters)
    if (!row) {
      throw new HTTPException(404, {
        message: `Thread with ID ${thread_id} not found`,
      })
    }

    // Abort in-flight runs and release their event buffers before the FK
    // cascade removes the rows (the worker's tolerant setStatus handles the
    // rest); checkpoints are removed via the saver.
    const runsRes = await this.pool.query<{ run_id: string }>(`SELECT run_id FROM runs WHERE thread_id = $1`, [
      thread_id,
    ])
    for (const { run_id } of runsRes.rows) {
      this.broker.getControl(run_id)?.abort('interrupt')
      this.broker.markFinished(run_id, 0)
      this.broker.notify(run_id)
    }
    await this.pool.query(`DELETE FROM threads WHERE thread_id = $1`, [thread_id])
    await this.saver.deleteThread(thread_id)
    return [thread_id]
  }

  async copy(thread_id: string, auth: AuthContext | undefined): Promise<Thread> {
    const [filters] = await handleAuthEvent(auth, 'threads:read', { thread_id })

    const row = await this.fetchThreadWithAuth(thread_id, filters)
    if (!row) {
      // Upstream quirk kept intentionally: copy signals a missing thread with
      // 409, not 404.
      throw new HTTPException(409, { message: 'Thread not found' })
    }

    const newThreadId = uuid7()
    const newMetadata = { ...row.metadata, thread_id: newThreadId }
    await handleAuthEvent(auth, 'threads:create', {
      thread_id: newThreadId,
      metadata: newMetadata,
    })

    const inserted = await this.pool.query<ThreadRow>(
      `INSERT INTO threads (thread_id, status, metadata, config)
       VALUES ($1, 'idle', $2::jsonb, '{}') RETURNING *`,
      [newThreadId, JSON.stringify(newMetadata)],
    )
    await this.copyCheckpoints(thread_id, newThreadId)
    return toThread(inserted.rows[0]!)
  }

  // PostgresSaver has no copy API; clone its rows with a column list resolved
  // at runtime so saver schema changes across versions can't break the copy.
  private async copyCheckpoints(fromId: string, toId: string): Promise<void> {
    for (const table of ['checkpoints', 'checkpoint_blobs', 'checkpoint_writes']) {
      const colsRes = await this.pool.query<{ column_name: string }>(
        `SELECT column_name FROM information_schema.columns
         WHERE table_name = $1 AND table_schema = current_schema()
         ORDER BY ordinal_position`,
        [table],
      )
      const cols = colsRes.rows.map((r) => r.column_name)
      if (!cols.includes('thread_id')) continue

      const insertCols = cols.map((c) => `"${c}"`).join(', ')
      const selectCols = cols.map((c) => (c === 'thread_id' ? '$2' : `"${c}"`)).join(', ')
      await this.pool.query(
        `INSERT INTO ${table} (${insertCols})
         SELECT ${selectCols} FROM ${table} WHERE thread_id = $1
         ON CONFLICT DO NOTHING`,
        [fromId, toId],
      )
    }
  }

  async count(
    options: {
      metadata?: Metadata
      values?: Record<string, unknown>
      status?: ThreadStatus
    },
    auth: AuthContext | undefined,
  ): Promise<number> {
    const [filters] = await handleAuthEvent(auth, 'threads:search', {
      metadata: options.metadata,
      values: options.values,
      status: options.status,
      limit: 0,
      offset: 0,
    })

    if (filters != null) {
      let total = 0
      for await (const _ of this.search({ ...options, limit: 10_000, offset: 0 }, auth)) total += 1
      return total
    }

    const where: string[] = []
    const params: unknown[] = []
    if (options.metadata) {
      params.push(JSON.stringify(options.metadata))
      where.push(`metadata @> $${params.length}::jsonb`)
    }
    if (options.values) {
      params.push(JSON.stringify(options.values))
      where.push(`"values" @> $${params.length}::jsonb`)
    }
    if (options.status) {
      params.push(options.status)
      where.push(`status = $${params.length}`)
    }
    const res = await this.pool.query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM threads ${where.length ? `WHERE ${where.join(' AND ')}` : ''}`,
      params,
    )
    return Number(res.rows[0]!.count)
  }
}

class PgThreadsState implements ThreadsStateRepo {
  constructor(
    private readonly pool: pg.Pool,
    private readonly threads: PgThreads,
    private readonly saver: PostgresSaver,
  ) {}

  async get(
    config: RunnableConfig,
    options: { subgraphs?: boolean },
    auth: AuthContext | undefined,
  ): Promise<StateSnapshot> {
    const subgraphs = options.subgraphs ?? false
    const threadId = config.configurable?.thread_id
    const thread = threadId ? await this.threads.get(threadId, auth) : undefined

    const graphId = thread?.metadata?.graph_id as string | undefined | null
    if (!thread || graphId == null) {
      return {
        values: {},
        next: [],
        config: {},
        metadata: undefined,
        createdAt: undefined,
        parentConfig: undefined,
        tasks: [],
      }
    }

    const graph = await getGraph(graphId, thread.config, { checkpointer: this.saver })
    const result = await graph.getState(config, { subgraphs })

    if (result.metadata != null && 'checkpoint_ns' in result.metadata && result.metadata['checkpoint_ns'] === '') {
      delete result.metadata['checkpoint_ns']
    }
    return result
  }

  async post(
    config: RunnableConfig,
    values: Record<string, unknown>[] | Record<string, unknown> | null | undefined,
    asNode: string | undefined,
    auth: AuthContext | undefined,
  ): Promise<{ checkpoint: Record<string, unknown> | undefined }> {
    const threadId = config.configurable?.thread_id
    const [filters] = await handleAuthEvent(auth, 'threads:update', {
      thread_id: threadId,
    })

    const thread = threadId ? await this.threads.get(threadId, auth) : undefined
    if (!thread) {
      throw new HTTPException(404, { message: `Thread ${threadId} not found` })
    }
    if (!isAuthMatching(thread.metadata, filters)) throw new HTTPException(403)

    const busyRes = await this.pool.query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM runs
       WHERE thread_id = $1 AND status IN ('pending', 'running')`,
      [threadId],
    )
    if (Number(busyRes.rows[0]!.count) > 0) {
      throw new HTTPException(409, { message: 'Thread is busy' })
    }

    const graphId = thread.metadata?.graph_id as string | undefined | null
    if (graphId == null) {
      throw new HTTPException(400, { message: `Thread ${threadId} has no graph ID` })
    }

    config.configurable ??= {}
    config.configurable.graph_id ??= graphId

    const graph = await getGraph(graphId, thread.config, { checkpointer: this.saver })

    const updateConfig = structuredClone(config)
    updateConfig.configurable ??= {}
    updateConfig.configurable.checkpoint_ns ??= ''

    const nextConfig = await graph.updateState(updateConfig, values, asNode)
    const state = await this.get(config, { subgraphs: false }, auth)

    await this.pool.query(`UPDATE threads SET "values" = $2::jsonb, updated_at = now() WHERE thread_id = $1`, [
      threadId,
      JSON.stringify(state.values ?? {}),
    ])
    return { checkpoint: nextConfig.configurable }
  }

  async bulk(
    config: RunnableConfig,
    supersteps: Array<{
      updates: Array<{
        values?: unknown
        command?: RunCommand | undefined | null
        as_node?: string | undefined
      }>
    }>,
    auth: AuthContext | undefined,
  ): Promise<{ checkpoint: Record<string, unknown> | undefined } | unknown[]> {
    const threadId = config.configurable?.thread_id
    if (!threadId) return []

    const [filters] = await handleAuthEvent(auth, 'threads:update', {
      thread_id: threadId,
    })

    const thread = await this.threads.get(threadId, auth)
    if (!isAuthMatching(thread.metadata, filters)) throw new HTTPException(403)

    const graphId = thread.metadata?.graph_id as string | undefined | null
    if (graphId == null) {
      throw new HTTPException(400, { message: `Thread ${threadId} has no graph ID` })
    }

    config.configurable ??= {}
    config.configurable.graph_id ??= graphId

    const graph = await getGraph(graphId, thread.config, { checkpointer: this.saver })

    const updateConfig = structuredClone(config)
    updateConfig.configurable ??= {}
    updateConfig.configurable.checkpoint_ns ??= ''

    const nextConfig = await graph.bulkUpdateState(
      updateConfig,
      supersteps.map((i) => ({
        updates: i.updates.map((j) => ({
          values: j.command != null ? getLangGraphCommand(j.command) : j.values,
          asNode: j.as_node,
        })),
      })),
    )
    const state = await this.get(config, { subgraphs: false }, auth)

    await this.pool.query(`UPDATE threads SET "values" = $2::jsonb, updated_at = now() WHERE thread_id = $1`, [
      threadId,
      JSON.stringify(state.values ?? {}),
    ])
    return { checkpoint: nextConfig.configurable }
  }

  async list(
    config: RunnableConfig,
    options: {
      limit?: number
      before?: string | RunnableConfig
      metadata?: Metadata
    },
    auth: AuthContext | undefined,
  ): Promise<StateSnapshot[]> {
    const threadId = config.configurable?.thread_id
    if (!threadId) return []

    const [filters] = await handleAuthEvent(auth, 'threads:read', {
      thread_id: threadId,
    })

    const thread = await this.threads.get(threadId, auth)
    if (!isAuthMatching(thread.metadata, filters)) return []

    const graphId = thread.metadata?.graph_id as string | undefined | null
    if (graphId == null) return []

    const graph = await getGraph(graphId, thread.config, { checkpointer: this.saver })
    const before: RunnableConfig | undefined =
      typeof options?.before === 'string' ? { configurable: { checkpoint_id: options.before } } : options?.before

    const states: StateSnapshot[] = []
    for await (const state of graph.getStateHistory(config, {
      limit: options?.limit ?? 10,
      before,
      filter: options?.metadata,
    })) {
      states.push(state)
    }
    return states
  }
}
