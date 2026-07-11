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
