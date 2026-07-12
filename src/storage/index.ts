import type pg from 'pg'
import type { PostgresSaver } from '@langchain/langgraph-checkpoint-postgres'
import type { Ops } from '@langchain/langgraph-api/storage'
import { PgAssistants } from './assistants'
import { PgThreads } from './threads'
import { PgRuns } from './runs'
import { RunBroker } from './broker'
import { ensureSchema, recoverOrphanedRuns } from './schema'

const EVENTS_SWEEP_INTERVAL_MS = 5 * 60 * 1000

export class PostgresOps implements Ops {
  public readonly assistants: PgAssistants
  public readonly threads: PgThreads
  public readonly runs: PgRuns
  public readonly broker: RunBroker
  private sweepTimer: ReturnType<typeof setInterval> | undefined

  constructor(
    private readonly pool: pg.Pool,
    private readonly saver: PostgresSaver,
  ) {
    this.broker = new RunBroker()
    this.assistants = new PgAssistants(pool, this.broker)
    this.threads = new PgThreads(pool, saver, this.broker)
    this.runs = new PgRuns(pool, this.threads, this.broker)
  }

  async initialize(): Promise<void> {
    await ensureSchema(this.pool)
    const requeued = await recoverOrphanedRuns(this.pool)
    if (requeued > 0) {
      console.log(`[storage] requeued ${requeued} orphaned run(s) from previous process`)
    }
    this.startEventsSweep()
  }

  // Replay window: the in-memory event buffer of a finished run is kept
  // RUN_EVENTS_TTL_SECONDS (default 1h) so clients can still reconnect, then
  // swept. Buffers of pending/running runs are never touched.
  private startEventsSweep(): void {
    const ttlSeconds = Number(process.env.RUN_EVENTS_TTL_SECONDS ?? 3600)
    if (!Number.isFinite(ttlSeconds) || ttlSeconds <= 0) return

    const sweep = (): void => {
      const removed = this.broker.sweep(ttlSeconds * 1000)
      if (removed > 0) {
        console.log(`[storage] swept ${removed} expired run event buffer(s)`)
      }
    }

    this.sweepTimer = setInterval(sweep, EVENTS_SWEEP_INTERVAL_MS)
    this.sweepTimer.unref?.()
  }

  stop(): void {
    if (this.sweepTimer) clearInterval(this.sweepTimer)
  }

  async truncate(flags: {
    runs?: boolean
    threads?: boolean
    assistants?: boolean
    checkpointer?: boolean
    store?: boolean
  }): Promise<void> {
    if (flags.runs) {
      await this.pool.query(`DELETE FROM runs`)
      // Only a runs wipe invalidates in-process run state (cancel controls,
      // replay buffers, wakeups); other flags must not touch live runs.
      this.broker.reset()
    }
    if (flags.threads) await this.pool.query(`DELETE FROM threads`)
    if (flags.assistants) {
      // Keep system-registered graph assistants (matches upstream): they are
      // only re-created at boot, so wiping them would 404 all run creation.
      await this.pool.query(`DELETE FROM assistants WHERE metadata->>'created_by' IS DISTINCT FROM 'system'`)
    }
    if (flags.checkpointer) {
      for (const table of ['checkpoint_writes', 'checkpoint_blobs', 'checkpoints']) {
        await this.pool.query(`TRUNCATE TABLE ${table}`).catch(() => undefined) // Saver tables may not exist yet.
      }
    }
    if (flags.store) {
      // The store is upstream's in-memory singleton (not exports-map
      // reachable); resolve its dist path the same way the plugin does.
      try {
        const pkgJsonUrl = import.meta.resolve('@langchain/langgraph-api/package.json')
        const storeUrl = new URL('dist/storage/store.mjs', pkgJsonUrl).href
        const { store } = (await import(storeUrl)) as { store: { clear(): Promise<void> } }
        await store.clear()
      } catch (error) {
        console.warn('[storage] store truncate failed', error)
      }
    }
  }
}
