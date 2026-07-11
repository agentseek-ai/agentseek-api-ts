// Advanced scenarios against a running server:
// 1. messages-tuple token streaming: disconnect mid-stream, replay from PG,
//    tokens must reassemble into the full text without corruption
// 2. interrupt() + Command resume (human-in-the-loop)
// 3. double-texting with multitask_strategy=reject → 409
const BASE = process.env.BASE_URL ?? 'http://localhost:2024'

interface SseEvent {
  id: string | null
  event: string
  data: string
}

async function req<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: body != null ? JSON.stringify(body) : undefined,
  })
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}: ${await res.text()}`)
  return (await res.json()) as T
}

async function readSse(
  path: string,
  opts: { lastEventId?: string; maxMs?: number; stopAfter?: number },
): Promise<{ events: SseEvent[]; ended: boolean }> {
  const controller = new AbortController()
  const timer = opts.maxMs ? setTimeout(() => controller.abort(), opts.maxMs) : null
  const headers: Record<string, string> = { Accept: 'text/event-stream' }
  if (opts.lastEventId) headers['Last-Event-ID'] = opts.lastEventId

  const events: SseEvent[] = []
  let ended = false
  try {
    const res = await fetch(`${BASE}${path}`, { headers, signal: controller.signal })
    if (!res.ok || !res.body) throw new Error(`GET ${path} -> ${res.status}`)
    const reader = res.body.getReader()
    const decoder = new TextDecoder()
    let buf = ''
    let cur: Partial<SseEvent> = {}
    for (;;) {
      const { done, value } = await reader.read()
      if (done) {
        ended = true
        break
      }
      buf += decoder.decode(value, { stream: true })
      const lines = buf.split('\n')
      buf = lines.pop() ?? ''
      for (const line of lines) {
        if (line.startsWith('id:')) cur.id = line.slice(3).trim()
        else if (line.startsWith('event:')) cur.event = line.slice(6).trim()
        else if (line.startsWith('data:')) cur.data = (cur.data ?? '') + line.slice(5).trim()
        else if (line === '' && cur.event) {
          events.push({ id: cur.id ?? null, event: cur.event, data: cur.data ?? '' })
          cur = {}
          if (opts.stopAfter && events.length >= opts.stopAfter) {
            controller.abort()
          }
        }
      }
    }
  } catch (e) {
    if (!(e instanceof DOMException && e.name === 'AbortError')) throw e
  } finally {
    if (timer) clearTimeout(timer)
  }
  return { events, ended }
}

const tokenText = (events: SseEvent[]): string =>
  events
    .filter((e) => e.event === 'messages')
    .map((e) => {
      const [chunk] = JSON.parse(e.data) as [{ content?: string; type?: string }]
      return chunk?.type === 'AIMessageChunk' || chunk?.type === 'ai' ? (chunk.content ?? '') : ''
    })
    .join('')

const FULL_TEXT =
  'Hello! This is a fake streaming response used to verify that token ' +
  'streams survive disconnect, replay and server restarts.'

let failures = 0
const check = (name: string, ok: boolean, detail?: string): void => {
  console.log(`${ok ? '✅' : '❌'} ${name}${detail ? ` — ${detail}` : ''}`)
  if (!ok) failures += 1
}

// --- 1. Token streaming with mid-stream disconnect + full replay ---
{
  const thread = await req<{ thread_id: string }>('POST', '/threads', {})
  const run = await req<{ run_id: string }>(`POST`, `/threads/${thread.thread_id}/runs`, {
    assistant_id: 'chat',
    input: { messages: [{ type: 'human', content: 'hi' }] },
    stream_mode: ['messages-tuple'],
    stream_resumable: true,
  })
  const streamPath = `/threads/${thread.thread_id}/runs/${run.run_id}/stream`

  // Watch a handful of tokens, then drop the connection mid-stream.
  const live = await readSse(streamPath, { maxMs: 1500 })
  const liveText = tokenText(live.events)
  check('tokens stream live', liveText.length > 0, `saw ${liveText.length} chars before disconnect`)

  // Let the run finish server-side, then replay EVERYTHING from PG.
  // The stream contains per-token chunks PLUS one complete message emitted
  // when the node returns — assert both reassemble to the exact text.
  await new Promise((r) => setTimeout(r, 5000))
  const replay = await readSse(streamPath, { lastEventId: '-1', maxMs: 10000 })
  const contents = replay.events
    .filter((e) => e.event === 'messages')
    .map((e) => {
      const [chunk] = JSON.parse(e.data) as [{ content?: string }]
      return chunk?.content ?? ''
    })
  const chunksText = contents.filter((c) => c.length < FULL_TEXT.length).join('')
  const completeCount = contents.filter((c) => c === FULL_TEXT).length
  check(
    'replayed token chunks reassemble the exact text',
    chunksText === FULL_TEXT,
    `${chunksText.length}/${FULL_TEXT.length} chars`,
  )
  check('replayed complete message intact', completeCount === 1)

  const state = await req<{ values: { messages: Array<{ content: string }> } }>(
    'GET',
    `/threads/${thread.thread_id}/state`,
  )
  const finalMsg = state.values.messages.at(-1)
  check('final AI message persisted in thread state', finalMsg?.content === FULL_TEXT)
}

// --- 2. interrupt() + Command resume ---
{
  const thread = await req<{ thread_id: string }>('POST', '/threads', {})
  await req('POST', `/threads/${thread.thread_id}/runs`, {
    assistant_id: 'approval',
    input: { steps: [] },
    stream_mode: ['values'],
  })

  let status = ''
  for (let i = 0; i < 20; i += 1) {
    const t = await req<{ status: string }>('GET', `/threads/${thread.thread_id}`)
    status = t.status
    if (status === 'interrupted') break
    await new Promise((r) => setTimeout(r, 500))
  }
  check('thread reaches interrupted status', status === 'interrupted')

  const state = await req<{ tasks: Array<{ interrupts: unknown[] }> }>('GET', `/threads/${thread.thread_id}/state`)
  const hasInterrupt = state.tasks?.some((t) => t.interrupts?.length > 0)
  check('state exposes the pending interrupt', Boolean(hasInterrupt))

  const result = await req<{ steps: string[] }>('POST', `/threads/${thread.thread_id}/runs/wait`, {
    assistant_id: 'approval',
    command: { resume: 'yes' },
  })
  check(
    'resume via Command completes the graph',
    result.steps?.includes('approved:yes') && result.steps?.includes('finished'),
    JSON.stringify(result.steps),
  )
}

// --- 3. double-texting: reject strategy ---
{
  const thread = await req<{ thread_id: string }>('POST', '/threads', {})
  await req('POST', `/threads/${thread.thread_id}/runs`, {
    assistant_id: 'agent',
    input: { steps: [] },
    stream_mode: ['values'],
  })

  const res = await fetch(`${BASE}/threads/${thread.thread_id}/runs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      assistant_id: 'agent',
      input: { steps: [] },
      multitask_strategy: 'reject',
    }),
  })
  // Upstream returns 422 for the reject strategy (not 409).
  check('second run rejected while first is inflight', res.status === 422, `status=${res.status}`)
  await res.body?.cancel()
}

console.log(failures === 0 ? '\n✅ ALL ADVANCED SCENARIOS PASS' : `\n❌ ${failures} FAILURE(S)`)
process.exit(failures === 0 ? 0 : 1)
