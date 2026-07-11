import { HTTPException } from 'hono/http-exception'
import type pg from 'pg'
import { v7 as uuid7 } from '@langchain/core/utils/uuid'
import { handleAuthEvent, isAuthMatching, type AuthContext } from '@langchain/langgraph-api/auth'
import type {
  IfNotExists,
  Metadata,
  MultitaskStrategy,
  Run,
  RunKwargs,
  RunnableConfig,
  RunsRepo,
  RunsStreamRepo,
  RunStatus,
} from '@langchain/langgraph-api/storage'
import type { RunBroker } from './broker'
import { serialiseAsDict, serializeError } from './serde'
import type { PgThreads } from './threads'

interface RunRow {
  run_id: string
  thread_id: string
  assistant_id: string
  status: RunStatus
  metadata: Metadata
  kwargs: RunKwargs
  multitask_strategy: MultitaskStrategy
  attempt: number
  created_at: Date
  updated_at: Date
}

const toRun = (row: RunRow): Run => ({
  run_id: row.run_id,
  thread_id: row.thread_id,
  assistant_id: row.assistant_id,
  status: row.status,
  metadata: row.metadata ?? {},
  kwargs: row.kwargs ?? {},
  multitask_strategy: row.multitask_strategy,
  created_at: row.created_at,
  updated_at: row.updated_at,
})

const JOIN_POLL_MS = 500

export class PgRuns implements RunsRepo {
  public readonly stream: RunsStreamRepo

  constructor(
    private readonly pool: pg.Pool,
    private readonly threads: PgThreads,
    private readonly broker: RunBroker,
  ) {
    this.stream = new PgRunsStream(pool, this, broker)
  }

  // Claims one pending run at a time: pending → running atomically, skipping
  // threads that already have a running run (thread serialization) and runs
  // locked by this process. The control lock lives for the worker's duration
  // so cancel() can abort mid-execution.
  async *next(): AsyncGenerator<{ run: Run; attempt: number; signal: AbortSignal }> {
    for (;;) {
      const res = await this.pool.query<RunRow>(
        `WITH candidate AS (
           SELECT r.run_id FROM runs r
           WHERE r.status = 'pending' AND r.created_at <= now()
             AND NOT EXISTS (
               SELECT 1 FROM runs r2
               WHERE r2.thread_id = r.thread_id AND r2.status = 'running'
             )
           ORDER BY r.created_at
           FOR UPDATE SKIP LOCKED
           LIMIT 1
         )
         UPDATE runs SET status = 'running', attempt = attempt + 1, updated_at = now()
         WHERE run_id IN (SELECT run_id FROM candidate)
         RETURNING *`,
      )

      const row = res.rows[0]
      if (!row) return

      const signal = this.broker.lock(row.run_id)
      try {
        yield { run: toRun(row), attempt: row.attempt, signal }
      } finally {
        this.broker.unlock(row.run_id)
        this.broker.notify(row.run_id)
      }
    }
  }

  async put(
    runId: string,
    assistantId: string,
    kwargs: RunKwargs,
    options: {
      threadId?: string
      userId?: string
      status?: RunStatus
      metadata?: Metadata
      preventInsertInInflight?: boolean
      multitaskStrategy?: MultitaskStrategy
      ifNotExists?: IfNotExists
      afterSeconds?: number
    },
    auth: AuthContext | undefined,
  ): Promise<Run[]> {
    const assistantRes = await this.pool.query<{
      assistant_id: string
      graph_id: string
      config: RunnableConfig
      context: unknown
      metadata: Metadata
    }>(`SELECT * FROM assistants WHERE assistant_id = $1`, [assistantId])
    const assistant = assistantRes.rows[0]
    if (!assistant) {
      throw new HTTPException(404, {
        message: `No assistant found for "${assistantId}". Make sure the assistant ID is for a valid assistant or a valid graph ID.`,
      })
    }

    const ifNotExists = options?.ifNotExists ?? 'reject'
    const multitaskStrategy = options?.multitaskStrategy ?? 'reject'
    const afterSeconds = options?.afterSeconds ?? 0
    const status = options?.status ?? 'pending'

    let threadId = options?.threadId

    const [filters, mutable] = await handleAuthEvent(auth, 'threads:create_run', {
      thread_id: threadId,
      assistant_id: assistantId,
      run_id: runId,
      status,
      metadata: options?.metadata ?? {},
      prevent_insert_if_inflight: options?.preventInsertInInflight,
      multitask_strategy: multitaskStrategy,
      if_not_exists: ifNotExists,
      after_seconds: afterSeconds,
      kwargs,
    })

    const metadata = mutable.metadata ?? {}
    const config: RunnableConfig = kwargs.config ?? {}
    const now = new Date()

    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')

      const threadRes = threadId
        ? await client.query<{
            thread_id: string
            status: string
            metadata: Metadata
            config: RunnableConfig
          }>(`SELECT * FROM threads WHERE thread_id = $1 FOR UPDATE`, [threadId])
        : { rows: [] }
      const existingThread = threadRes.rows[0]

      if (existingThread && !isAuthMatching(existingThread.metadata, filters)) {
        throw new HTTPException(404)
      }

      if (!existingThread && (threadId == null || ifNotExists === 'create')) {
        threadId ??= uuid7()
        const threadConfig = Object.assign({}, assistant.config, config, {
          configurable: Object.assign({}, assistant.config?.configurable, config?.configurable),
        })
        await client.query(
          `INSERT INTO threads (thread_id, status, metadata, config)
           VALUES ($1, 'busy', $2::jsonb, $3::jsonb)`,
          [
            threadId,
            JSON.stringify({
              graph_id: assistant.graph_id,
              assistant_id: assistantId,
              ...metadata,
            }),
            JSON.stringify(threadConfig),
          ],
        )
      } else if (existingThread) {
        if (existingThread.status !== 'busy') {
          const mergedThreadConfig = Object.assign({}, assistant.config, existingThread.config, config, {
            configurable: Object.assign(
              {},
              assistant.config?.configurable,
              existingThread.config?.configurable,
              config?.configurable,
            ),
          })
          await client.query(
            `UPDATE threads
             SET status = 'busy',
                 metadata = metadata || $2::jsonb,
                 config = $3::jsonb,
                 updated_at = now()
             WHERE thread_id = $1`,
            [
              threadId,
              JSON.stringify({
                graph_id: assistant.graph_id,
                assistant_id: assistantId,
              }),
              JSON.stringify(mergedThreadConfig),
            ],
          )
        }
      } else {
        await client.query('COMMIT')
        return []
      }

      const inflightRes = await client.query<RunRow>(
        `SELECT * FROM runs WHERE thread_id = $1 AND status IN ('pending', 'running')
         ORDER BY created_at`,
        [threadId],
      )
      const inflightRuns = inflightRes.rows.map(toRun)

      if (options?.preventInsertInInflight && inflightRuns.length > 0) {
        await client.query('COMMIT')
        return inflightRuns
      }

      const existingThreadConfig = existingThread?.config ?? {}
      const configurable = Object.assign(
        {},
        assistant.config?.configurable,
        existingThreadConfig?.configurable,
        config?.configurable,
        {
          run_id: runId,
          thread_id: threadId,
          graph_id: assistant.graph_id,
          assistant_id: assistantId,
          user_id:
            config.configurable?.user_id ??
            existingThreadConfig?.configurable?.user_id ??
            assistant.config?.configurable?.user_id ??
            options?.userId,
        },
      )

      const mergedMetadata = Object.assign({}, assistant.metadata, existingThread?.metadata, metadata)

      const mergedKwargs = Object.assign({}, kwargs, {
        config: Object.assign({}, assistant.config, config, { configurable }, { metadata: mergedMetadata }),
        context:
          typeof assistant.context !== 'object' && assistant.context != null
            ? (assistant.context ?? kwargs.context)
            : Object.assign({}, assistant.context, kwargs.context),
      })

      const createdAt = new Date(now.valueOf() + afterSeconds * 1000)
      const inserted = await client.query<RunRow>(
        `INSERT INTO runs (run_id, thread_id, assistant_id, status, metadata, kwargs, multitask_strategy, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9)
         RETURNING *`,
        [
          runId,
          threadId,
          assistantId,
          status,
          JSON.stringify(mergedMetadata),
          JSON.stringify(mergedKwargs),
          multitaskStrategy,
          createdAt,
          now,
        ],
      )

      await client.query('COMMIT')
      return [toRun(inserted.rows[0]!), ...inflightRuns]
    } catch (error) {
      await client.query('ROLLBACK')
      throw error
    } finally {
      client.release()
    }
  }

  async get(runId: string, thread_id: string | undefined, auth: AuthContext | undefined): Promise<Run | null> {
    const [filters] = await handleAuthEvent(auth, 'threads:read', { thread_id })

    const res = await this.pool.query<RunRow & { thread_metadata: Metadata }>(
      `SELECT r.*, t.metadata AS thread_metadata FROM runs r
       JOIN threads t ON t.thread_id = r.thread_id
       WHERE r.run_id = $1 ${thread_id != null ? 'AND r.thread_id = $2' : ''}`,
      thread_id != null ? [runId, thread_id] : [runId],
    )
    const row = res.rows[0]
    if (!row) return null
    if (!isAuthMatching(row.thread_metadata, filters)) return null
    return toRun(row)
  }

  async delete(run_id: string, thread_id: string | undefined, auth: AuthContext | undefined): Promise<string | null> {
    const [filters] = await handleAuthEvent(auth, 'threads:delete', {
      run_id,
      thread_id,
    })

    const res = await this.pool.query<RunRow & { thread_metadata: Metadata }>(
      `SELECT r.*, t.metadata AS thread_metadata FROM runs r
       JOIN threads t ON t.thread_id = r.thread_id
       WHERE r.run_id = $1 ${thread_id != null ? 'AND r.thread_id = $2' : ''}`,
      thread_id != null ? [run_id, thread_id] : [run_id],
    )
    const row = res.rows[0]
    if (!row || !isAuthMatching(row.thread_metadata, filters)) {
      throw new HTTPException(404, { message: 'Run not found' })
    }

    await this.pool.query(`DELETE FROM runs WHERE run_id = $1`, [run_id])
    // Let the TTL sweep reclaim the event buffer of a deleted run.
    this.broker.markFinished(run_id)
    return row.run_id
  }

  async wait(runId: string, threadId: string | undefined, auth: AuthContext | undefined): Promise<unknown> {
    const runStream = this.stream.join(runId, threadId, { ignore404: threadId == null, lastEventId: undefined }, auth)

    let lastChunk: unknown = null
    for await (const { event, data } of runStream) {
      if (event === 'values') {
        lastChunk = data as Record<string, unknown>
      } else if (event === 'error') {
        lastChunk = { __error__: serializeError(data) }
      }
    }
    return lastChunk
  }

  async join(runId: string, threadId: string, auth: AuthContext | undefined): Promise<unknown> {
    await this.threads.get(threadId, auth)

    const lastChunk = await this.wait(runId, threadId, auth)
    if (lastChunk != null) return lastChunk

    const thread = await this.threads.get(threadId, auth)
    return thread.values ?? null
  }

  async cancel(
    threadId: string | undefined,
    runIds: string[],
    options: { action?: 'interrupt' | 'rollback' },
    auth: AuthContext | undefined,
  ): Promise<void> {
    const action = options.action ?? 'interrupt'

    const [filters] = await handleAuthEvent(auth, 'threads:update', {
      thread_id: threadId,
      action,
      metadata: { run_ids: runIds, status: 'pending' },
    })

    let foundRunsCount = 0
    const promises: Promise<unknown>[] = []

    for (const runId of runIds) {
      const res = await this.pool.query<RunRow & { thread_metadata: Metadata }>(
        `SELECT r.*, t.metadata AS thread_metadata FROM runs r
         JOIN threads t ON t.thread_id = r.thread_id
         WHERE r.run_id = $1 ${threadId != null ? 'AND r.thread_id = $2' : ''}`,
        threadId != null ? [runId, threadId] : [runId],
      )
      const row = res.rows[0]
      if (!row) continue
      if (!isAuthMatching(row.thread_metadata, filters)) continue

      foundRunsCount += 1

      const control = this.broker.getControl(runId)
      control?.abort(action)

      if (row.status === 'pending') {
        if (control || action !== 'rollback') {
          await this.pool.query(`UPDATE runs SET status = 'interrupted', updated_at = now() WHERE run_id = $1`, [runId])
          await this.pool.query(`UPDATE threads SET status = 'idle', updated_at = now() WHERE thread_id = $1`, [
            row.thread_id,
          ])
          this.broker.markFinished(runId)
        } else {
          promises.push(this.delete(runId, threadId, auth))
        }
      }
      this.broker.notify(runId)
    }

    await Promise.all(promises)

    if (foundRunsCount !== runIds.length) {
      throw new HTTPException(404, { message: 'Run not found' })
    }
  }

  async search(
    threadId: string,
    options: {
      limit?: number | null
      offset?: number | null
      status?: string | null
      metadata?: Metadata | null
    },
    auth: AuthContext | undefined,
  ): Promise<Run[]> {
    const [filters] = await handleAuthEvent(auth, 'threads:search', {
      thread_id: threadId,
      metadata: options.metadata,
      status: options.status,
    })

    const params: unknown[] = [threadId]
    const where: string[] = [`r.thread_id = $1`]
    if (options.status != null) {
      params.push(options.status)
      where.push(`r.status = $${params.length}`)
    }
    if (options.metadata != null) {
      params.push(JSON.stringify(options.metadata))
      where.push(`r.metadata @> $${params.length}::jsonb`)
    }
    params.push(options.limit ?? 10, options.offset ?? 0)

    const res = await this.pool.query<RunRow & { thread_metadata: Metadata }>(
      `SELECT r.*, t.metadata AS thread_metadata FROM runs r
       JOIN threads t ON t.thread_id = r.thread_id
       WHERE ${where.join(' AND ')}
       ORDER BY r.created_at DESC
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params,
    )
    return res.rows.filter((row) => isAuthMatching(row.thread_metadata, filters)).map(toRun)
  }

  async setStatus(runId: string, status: RunStatus): Promise<void> {
    const res = await this.pool.query(`UPDATE runs SET status = $2, updated_at = now() WHERE run_id = $1`, [
      runId,
      status,
    ])
    if (res.rowCount === 0) throw new Error(`Run ${runId} not found`)
    // Terminal status starts the replay-window TTL on the event buffer.
    if (status !== 'pending' && status !== 'running') this.broker.markFinished(runId)
    // Wake joined streams promptly so they can observe the terminal status.
    this.broker.notify(runId)
  }
}

class PgRunsStream implements RunsStreamRepo {
  constructor(
    private readonly pool: pg.Pool,
    private readonly runs: PgRuns,
    private readonly broker: RunBroker,
  ) {}

  // Every event goes into the broker's in-memory replay buffer (fixes the
  // upstream first-joiner race: replayability derives from the run itself,
  // not from whoever created a queue first). Data is stored pre-serialized so
  // replayed payloads are byte-identical to live SSE and never alias objects
  // the graph may still mutate.
  async publish(payload: {
    runId: string
    resumable: boolean
    event: string
    data: unknown
    normalized?: boolean
  }): Promise<void> {
    this.broker.append(payload.runId, payload.event, serialiseAsDict(payload.data), payload.normalized ?? null)
    this.broker.notify(payload.runId)
  }

  async *join(
    runId: string,
    threadId: string | undefined,
    options: {
      ignore404?: boolean
      signal?: AbortSignal
      cancelOnDisconnect?: boolean
      lastEventId: string | undefined
    },
    auth: AuthContext | undefined,
  ): AsyncGenerator<{ id?: string; event: string; data: unknown }> {
    const signal = options.signal

    const [filters] = await handleAuthEvent(auth, 'threads:read', {
      thread_id: threadId,
    })
    if (filters != null && threadId != null) {
      const threadRes = await this.pool.query<{ metadata: Metadata }>(
        `SELECT metadata FROM threads WHERE thread_id = $1`,
        [threadId],
      )
      if (!threadRes.rows[0] || !isAuthMatching(threadRes.rows[0].metadata, filters)) {
        yield {
          event: 'error',
          data: { error: 'Error', message: '404: Thread not found' },
        }
        return
      }
    }

    let lastSeq: number
    if (options.lastEventId != null && !Number.isNaN(+options.lastEventId)) {
      lastSeq = +options.lastEventId
      // A Last-Event-ID from a previous server incarnation can be ahead of the
      // rebuilt buffer (a requeued run republishes from seq 0) — treat it as
      // stale and replay everything we still have.
      if (lastSeq > this.broker.maxSeq(runId)) lastSeq = -1
    } else {
      const run = await this.runs.get(runId, threadId, auth)
      if (run?.kwargs?.resumable) {
        // Official semantics: a bare join on a resumable run tails live events.
        lastSeq = this.broker.maxSeq(runId)
      } else {
        lastSeq = -1
      }
    }

    let terminalDrainDone = false
    for (;;) {
      // signal aborts asynchronously (client disconnect), re-checked per lap.
      if (signal?.aborted) break
      const rows = this.broker.read(runId, lastSeq, 256)

      if (rows.length > 0) {
        for (const row of rows) {
          lastSeq = row.seq
          yield {
            id: String(row.seq),
            event: row.event,
            data: row.data != null ? JSON.parse(row.data) : null,
            ...(row.normalized != null ? { normalized: row.normalized } : {}),
          }
        }
        continue
      }

      if (terminalDrainDone) break

      const woken = await this.broker.wait(runId, {
        timeoutMs: JOIN_POLL_MS,
        signal: signal ?? undefined,
      })
      if (woken) continue
      if (signal?.aborted) break

      const run = await this.runs.get(runId, threadId, auth)
      if (run == null) {
        if (!options.ignore404) yield { event: 'error', data: 'Run not found' }
        break
      }
      if (run.status !== 'pending' && run.status !== 'running') {
        // One final read to drain events published just before the status flip.
        terminalDrainDone = true
      }
    }

    if (signal?.aborted && options.cancelOnDisconnect && threadId != null) {
      await this.runs.cancel(threadId, [runId], { action: 'interrupt' }, auth)
    }
  }
}
