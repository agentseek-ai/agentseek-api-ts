// In-process coordination for the single-instance server: run cancellation
// controls, live-event wakeups for SSE joins, and the per-run event replay
// buffer. Stream events live ONLY here (not in Postgres): replay works for
// the lifetime of the process; results and step history survive restarts via
// checkpoints. Run state (runs table) and checkpoints remain in Postgres.
class CancellationAbortController extends AbortController {
  override abort(reason: 'rollback' | 'interrupt'): void {
    super.abort(reason)
  }
}

export interface StoredEvent {
  seq: number
  event: string
  data: string | null
  normalized: boolean | null
}

interface RunEventBuffer {
  events: StoredEvent[]
  nextSeq: number
  finishedAt: number | null
  // Per-buffer TTL override (ms); null = use the sweep's default TTL.
  expireAfterMs: number | null
}

// Per-run cap (mirrors Aegra's Redis LTRIM bound): a runaway stream drops its
// oldest events instead of growing without limit. Trimming never rewinds seq.
const MAX_EVENTS_PER_RUN = 10_000

// Each execution attempt gets its own seq range (attempt-1)*EPOCH.., so a
// Last-Event-ID from a previous incarnation can never collide with the
// republished events of a requeued run. Must stay > MAX_EVENTS_PER_RUN.
const EVENT_SEQ_EPOCH = 1_000_000

export class RunBroker {
  private controls = new Map<string, CancellationAbortController>()
  private listeners = new Map<string, Set<() => void>>()
  private buffers = new Map<string, RunEventBuffer>()

  isLocked(runId: string): boolean {
    return this.controls.has(runId)
  }

  lock(runId: string): AbortSignal {
    const control = new CancellationAbortController()
    this.controls.set(runId, control)
    return control.signal
  }

  unlock(runId: string): void {
    this.controls.delete(runId)
  }

  getControl(runId: string): CancellationAbortController | undefined {
    return this.controls.get(runId)
  }

  // --- event replay buffer ---

  // Called when a worker claims a run: seq numbering jumps to this attempt's
  // range so ids stay strictly monotonic across requeues/restarts. Events of
  // an earlier attempt (same process) are dropped — the retry republishes.
  beginIncarnation(runId: string, attempt: number): void {
    const base = Math.max(0, attempt - 1) * EVENT_SEQ_EPOCH
    const buf = this.buffers.get(runId)
    if (!buf) {
      this.buffers.set(runId, { events: [], nextSeq: base, finishedAt: null, expireAfterMs: null })
      return
    }
    if (buf.nextSeq < base) {
      buf.events = []
      buf.nextSeq = base
    }
    buf.finishedAt = null
    buf.expireAfterMs = null
  }

  // Appends an event and returns its seq (monotonic per run).
  append(runId: string, event: string, data: string | null, normalized: boolean | null): number {
    let buf = this.buffers.get(runId)
    if (!buf) {
      buf = { events: [], nextSeq: 0, finishedAt: null, expireAfterMs: null }
      this.buffers.set(runId, buf)
    }
    const seq = buf.nextSeq
    buf.nextSeq += 1
    buf.events.push({ seq, event, data, normalized })
    if (buf.events.length > MAX_EVENTS_PER_RUN) {
      buf.events.splice(0, buf.events.length - MAX_EVENTS_PER_RUN)
    }
    return seq
  }

  // Events with seq > afterSeq, oldest first. Seqs are contiguous (append-only,
  // head-trimmed), so the start index is computed in O(1).
  read(runId: string, afterSeq: number, limit: number): StoredEvent[] {
    const buf = this.buffers.get(runId)
    if (!buf || buf.events.length === 0) return []
    const firstSeq = buf.events[0]!.seq
    const start = Math.max(0, afterSeq + 1 - firstSeq)
    return buf.events.slice(start, start + limit)
  }

  maxSeq(runId: string): number {
    const buf = this.buffers.get(runId)
    return buf ? buf.nextSeq - 1 : -1
  }

  // Starts the replay-window countdown; the buffer is swept after finish
  // (expireAfterMs overrides the sweep's default TTL, e.g. a short grace for
  // non-resumable runs that no reconnecting client will ever replay).
  markFinished(runId: string, expireAfterMs?: number): void {
    const buf = this.buffers.get(runId)
    if (!buf) return
    if (buf.finishedAt == null) buf.finishedAt = Date.now()
    if (expireAfterMs != null) buf.expireAfterMs = expireAfterMs
  }

  // Drops buffers of runs finished more than their TTL ago. Buffers of live
  // runs are never touched.
  sweep(ttlMs: number): number {
    const now = Date.now()
    let removed = 0
    for (const [runId, buf] of this.buffers) {
      if (buf.finishedAt != null && now - buf.finishedAt > (buf.expireAfterMs ?? ttlMs)) {
        this.buffers.delete(runId)
        removed += 1
      }
    }
    return removed
  }

  notify(runId: string): void {
    const set = this.listeners.get(runId)
    if (!set) return
    for (const wake of set) wake()
  }

  // Resolves true when notified, false on timeout/abort.
  wait(runId: string, options: { timeoutMs: number; signal?: AbortSignal }): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
      let set = this.listeners.get(runId)
      if (!set) {
        set = new Set()
        this.listeners.set(runId, set)
      }
      const listeners = set

      const finish = (value: boolean): void => {
        clearTimeout(timer)
        listeners.delete(wake)
        if (listeners.size === 0) this.listeners.delete(runId)
        options.signal?.removeEventListener('abort', onAbort)
        resolve(value)
      }

      const wake = (): void => finish(true)
      const onAbort = (): void => finish(false)
      const timer = setTimeout(() => finish(false), options.timeoutMs)

      options.signal?.addEventListener('abort', onAbort, { once: true })
      listeners.add(wake)
    })
  }

  reset(): void {
    this.controls.clear()
    this.listeners.clear()
    this.buffers.clear()
  }
}
