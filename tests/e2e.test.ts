// E2E suite against a real server + Postgres (docker compose up -d postgres).
// Covers the two core guarantees (background runs, resumable streams) plus
// token streaming, interrupt/resume, and double-texting.
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { sleep, TestServer, type SseEvent } from './helpers'

const server = new TestServer(2098)

beforeAll(async () => {
  await server.start()
}, 40_000)

afterAll(() => {
  server.stop()
})

const createThread = async (): Promise<string> => {
  const t = await server.req<{ thread_id: string }>('POST', '/threads', {})
  return t.thread_id
}

const createRun = async (threadId: string, body: Record<string, unknown>): Promise<string> => {
  const r = await server.req<{ run_id: string }>('POST', `/threads/${threadId}/runs`, body)
  return r.run_id
}

const maxSteps = (events: SseEvent[]): number =>
  Math.max(
    0,
    ...events.filter((e) => e.event === 'values').map((e) => ((JSON.parse(e.data).steps as string[]) ?? []).length),
  )

describe('background runs', () => {
  test('run survives SSE disconnect and completes server-side', async () => {
    const tid = await createThread()
    const rid = await createRun(tid, {
      assistant_id: 'agent',
      input: { steps: [] },
      stream_mode: ['values'],
      stream_resumable: true,
    })

    const first = await server.readSse(`/threads/${tid}/runs/${rid}/stream`, { maxMs: 3000 })
    expect(first.ended).toBe(false)
    expect(maxSteps(first.events)).toBeLessThan(5)

    // Client is gone; the run must keep executing.
    let status = ''
    for (let i = 0; i < 20; i += 1) {
      await sleep(1000)
      const run = await server.req<{ status: string }>('GET', `/threads/${tid}/runs/${rid}`)
      status = run.status
      if (status === 'success') break
    }
    expect(status).toBe('success')

    const state = await server.req<{ values: { steps: string[] } }>('GET', `/threads/${tid}/state`)
    expect(state.values.steps).toHaveLength(5)
  }, 40_000)

  test('runs/wait blocks until final values', async () => {
    const tid = await createThread()
    const result = await server.req<{ steps: string[] }>('POST', `/threads/${tid}/runs/wait`, {
      assistant_id: 'agent',
      input: { steps: [] },
    })
    expect(result.steps).toHaveLength(5)
  }, 30_000)
})

describe('resumable streams', () => {
  test('reconnect with Last-Event-ID replays missed events (immediate bare join)', async () => {
    const tid = await createThread()
    const rid = await createRun(tid, {
      assistant_id: 'agent',
      input: { steps: [] },
      stream_mode: ['values'],
      stream_resumable: true,
    })
    const path = `/threads/${tid}/runs/${rid}/stream`

    // Immediate join — regression guard for the upstream first-joiner race.
    const first = await server.readSse(path, { maxMs: 3000 })
    const lastId = [...first.events].reverse().find((e) => e.id)?.id
    expect(lastId).toBeTruthy()
    const seenBefore = maxSteps(first.events)

    await sleep(5000)

    const second = await server.readSse(path, {
      lastEventId: lastId!,
      maxMs: 20_000,
    })
    expect(second.ended).toBe(true)
    expect(maxSteps(second.events)).toBe(5)
    // Replay must include events missed while disconnected, not just the tail.
    const replayedSeqs = second.events.map((e) => Number(e.id))
    expect(Math.min(...replayedSeqs)).toBe(Number(lastId) + 1)
    expect(seenBefore).toBeLessThan(5)
  }, 60_000)

  test('token stream (messages-tuple) replays byte-exact from the replay buffer', async () => {
    const FULL_TEXT =
      'Hello! This is a fake streaming response used to verify that token ' +
      'streams survive disconnect, replay and server restarts.'

    const tid = await createThread()
    const rid = await createRun(tid, {
      assistant_id: 'chat',
      input: { messages: [{ type: 'human', content: 'hi' }] },
      stream_mode: ['messages-tuple'],
      stream_resumable: true,
    })

    // Drop mid-stream, let it finish, then replay everything.
    await server.readSse(`/threads/${tid}/runs/${rid}/stream`, { maxMs: 1500 })
    await sleep(5000)

    const replay = await server.readSse(`/threads/${tid}/runs/${rid}/stream`, { lastEventId: '-1', maxMs: 10_000 })
    const contents = replay.events
      .filter((e) => e.event === 'messages')
      .map((e) => {
        const [chunk] = JSON.parse(e.data) as [{ content?: string }]
        return chunk?.content ?? ''
      })
    const chunksText = contents.filter((c) => c.length < FULL_TEXT.length).join('')
    expect(chunksText).toBe(FULL_TEXT)
    expect(contents.filter((c) => c === FULL_TEXT)).toHaveLength(1)
  }, 60_000)
})

describe('human-in-the-loop', () => {
  test('interrupt() pauses the thread; Command resume completes it', async () => {
    const tid = await createThread()
    await createRun(tid, {
      assistant_id: 'approval',
      input: { steps: [] },
      stream_mode: ['values'],
    })

    let status = ''
    for (let i = 0; i < 20; i += 1) {
      const t = await server.req<{ status: string }>('GET', `/threads/${tid}`)
      status = t.status
      if (status === 'interrupted') break
      await sleep(500)
    }
    expect(status).toBe('interrupted')

    const result = await server.req<{ steps: string[] }>('POST', `/threads/${tid}/runs/wait`, {
      assistant_id: 'approval',
      command: { resume: 'yes' },
    })
    expect(result.steps).toContain('approved:yes')
    expect(result.steps).toContain('finished')
  }, 40_000)
})

describe('double-texting', () => {
  test('multitask_strategy=reject returns 422 while a run is inflight', async () => {
    const tid = await createThread()
    await createRun(tid, {
      assistant_id: 'agent',
      input: { steps: [] },
      stream_mode: ['values'],
    })

    const res = await server.rawPost(`/threads/${tid}/runs`, {
      assistant_id: 'agent',
      input: { steps: [] },
      multitask_strategy: 'reject',
    })
    expect(res.status).toBe(422)
    await res.body?.cancel()
  }, 20_000)
})

describe('thread state', () => {
  test('history exposes checkpoints from Postgres', async () => {
    const tid = await createThread()
    await server.req('POST', `/threads/${tid}/runs/wait`, {
      assistant_id: 'agent',
      input: { steps: [] },
    })
    const history = await server.req<unknown[]>('GET', `/threads/${tid}/history?limit=20`)
    expect(history.length).toBeGreaterThanOrEqual(6)
  }, 30_000)
})

describe('thread serialization', () => {
  test('enqueued runs on one thread execute strictly in order', async () => {
    const tid = await createThread()
    const r1 = await createRun(tid, { assistant_id: 'agent', input: { steps: [] }, stream_mode: ['values'] })
    const r2 = await createRun(tid, {
      assistant_id: 'agent',
      input: { steps: [] },
      stream_mode: ['values'],
      multitask_strategy: 'enqueue',
    })

    let sawQueued = false
    let s1 = ''
    let s2 = ''
    for (let i = 0; i < 80; i += 1) {
      ;[s1, s2] = await Promise.all([
        server.req<{ status: string }>('GET', `/threads/${tid}/runs/${r1}`).then((r) => r.status),
        server.req<{ status: string }>('GET', `/threads/${tid}/runs/${r2}`).then((r) => r.status),
      ])
      if (s1 === 'running' && s2 === 'pending') sawQueued = true
      if (s1 === 'success' && s2 === 'success') break
      await sleep(500)
    }
    expect(sawQueued).toBe(true)
    expect(s1).toBe('success')
    expect(s2).toBe('success')

    // Serial execution appends run 2's five steps cleanly after run 1's;
    // concurrent execution would interleave/corrupt the checkpoint chain.
    const state = await server.req<{ values: { steps: string[] } }>('GET', `/threads/${tid}/state`)
    expect(state.values.steps.map((s) => s.split(' ')[0])).toEqual([
      'step1',
      'step2',
      'step3',
      'step4',
      'step5',
      'step1',
      'step2',
      'step3',
      'step4',
      'step5',
    ])
  }, 60_000)
})

describe('deletion safety (review regressions)', () => {
  test('deleting a thread mid-run keeps the workers alive', async () => {
    const tid = await createThread()
    await createRun(tid, { assistant_id: 'agent', input: { steps: [] }, stream_mode: ['values'] })
    await sleep(2500) // claimed and mid-execution
    await server.req('DELETE', `/threads/${tid}`)

    // Both workers must still process runs afterwards (two in parallel).
    const results = await Promise.all(
      [1, 2].map(async () => {
        const t = await createThread()
        return server.req<{ messages: unknown[] }>('POST', `/threads/${t}/runs/wait`, {
          assistant_id: 'chat',
          input: { messages: [{ type: 'human', content: 'alive?' }] },
        })
      }),
    )
    expect(results[0]!.messages.length).toBeGreaterThan(0)
    expect(results[1]!.messages.length).toBeGreaterThan(0)
  }, 40_000)

  test('deleting a run purges its checkpoints from history', async () => {
    const tid = await createThread()
    await server.req('POST', `/threads/${tid}/runs/wait`, {
      assistant_id: 'chat',
      input: { messages: [{ type: 'human', content: 'hi' }] },
    })
    const runs = await server.req<Array<{ run_id: string }>>('GET', `/threads/${tid}/runs`)
    const before = await server.req<unknown[]>('GET', `/threads/${tid}/history?limit=50`)
    expect(before.length).toBeGreaterThan(0)

    await server.req('DELETE', `/threads/${tid}/runs/${runs[0]!.run_id}`)
    const after = await server.req<unknown[]>('GET', `/threads/${tid}/history?limit=50`)
    expect(after).toHaveLength(0)
  }, 30_000)

  test('deleting an assistant cascades to its pending runs and frees the thread', async () => {
    const assistant = await server.req<{ assistant_id: string }>('POST', '/assistants', { graph_id: 'agent' })
    const tid = await createThread()
    await createRun(tid, { assistant_id: assistant.assistant_id, input: { steps: [] }, after_seconds: 30 })

    await server.req('DELETE', `/assistants/${assistant.assistant_id}`)

    const runs = await server.req<unknown[]>('GET', `/threads/${tid}/runs`)
    expect(runs).toHaveLength(0)
    const thread = await server.req<{ status: string }>('GET', `/threads/${tid}`)
    expect(thread.status).toBe('idle')
  }, 20_000)
})

describe('stale stream ids', () => {
  test('a Last-Event-ID beyond the buffer replays from the start', async () => {
    const tid = await createThread()
    const rid = await createRun(tid, {
      assistant_id: 'chat',
      input: { messages: [{ type: 'human', content: 'hi' }] },
      stream_mode: ['messages-tuple'],
      stream_resumable: true,
    })
    let status = ''
    for (let i = 0; i < 30; i += 1) {
      status = (await server.req<{ status: string }>('GET', `/threads/${tid}/runs/${rid}`)).status
      if (status === 'success') break
      await sleep(500)
    }
    expect(status).toBe('success')

    // e.g. an id minted by a previous server incarnation.
    const replay = await server.readSse(`/threads/${tid}/runs/${rid}/stream`, {
      lastEventId: '999999999',
      maxMs: 10_000,
    })
    expect(replay.events.length).toBeGreaterThan(0)
    expect(Number(replay.events[0]!.id)).toBe(0)
  }, 40_000)
})

describe('assistants', () => {
  test('name search matches case-insensitive substrings', async () => {
    await server.req('POST', '/assistants', { graph_id: 'agent', name: 'My Fancy Agent' })
    const hits = await server.req<Array<{ name: string }>>('POST', '/assistants/search', {
      name: 'fancy',
      limit: 10,
      offset: 0,
    })
    expect(hits.some((a) => a.name === 'My Fancy Agent')).toBe(true)
  }, 15_000)
})

// Keep this LAST: it wipes user-created assistants.
describe('truncate', () => {
  test('preserves system graph assistants, clears the store, runs keep working', async () => {
    const put = await fetch(`${server.base}/store/items`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace: ['e2e'], key: 'k1', value: { hello: 1 } }),
    })
    expect(put.ok).toBe(true)
    const found = await server.req<{ items: unknown[] }>('POST', '/store/items/search', {
      namespace_prefix: ['e2e'],
    })
    expect(found.items).toHaveLength(1)

    await server.req('POST', '/internal/truncate', { assistants: true, store: true })
    await sleep(500) // the upstream route fires ops.truncate without awaiting it

    const assistants = await server.req<unknown[]>('POST', '/assistants/search', { limit: 50, offset: 0 })
    expect(assistants.length).toBeGreaterThanOrEqual(3) // agent/chat/approval survive

    const cleared = await server.req<{ items: unknown[] }>('POST', '/store/items/search', {
      namespace_prefix: ['e2e'],
    })
    expect(cleared.items).toHaveLength(0)

    const tid = await createThread()
    const result = await server.req<{ steps: string[] }>('POST', `/threads/${tid}/runs/wait`, {
      assistant_id: 'agent',
      input: { steps: [] },
    })
    expect(result.steps).toHaveLength(5)
  }, 40_000)
})
