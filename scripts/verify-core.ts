// Verifies the two core requirements against a running server:
// 1. Background run survives SSE disconnect (client detach ≠ task death)
// 2. Resumable streaming: reconnect with Last-Event-ID replays missed events
const BASE = process.env.BASE_URL ?? 'http://localhost:2024'

interface SseEvent {
  id: string | null
  event: string
  data: string
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(`POST ${path} -> ${res.status}: ${await res.text()}`)
  return (await res.json()) as T
}

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`GET ${path} -> ${res.status}`)
  return (await res.json()) as T
}

// Minimal SSE parser: reads events until aborted, maxEvents reached, or stream ends.
async function readSse(
  path: string,
  opts: { lastEventId?: string; maxMs?: number },
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

function summarize(events: SseEvent[]): string {
  return events
    .map(
      (e) =>
        `${e.id ?? '-'} ${e.event}${e.event === 'values' ? ` steps=${(JSON.parse(e.data).steps ?? []).length}` : ''}`,
    )
    .join('\n  ')
}

// --- Scenario setup: thread + background resumable run ---
const thread = await postJson<{ thread_id: string }>('/threads', {})
console.log(`thread: ${thread.thread_id}`)

const run = await postJson<{ run_id: string }>(`/threads/${thread.thread_id}/runs`, {
  assistant_id: 'agent',
  input: { steps: [] },
  stream_mode: ['values'],
  stream_resumable: true,
})
console.log(`run:    ${run.run_id} (background, resumable)`)

const streamPath = `/threads/${thread.thread_id}/runs/${run.run_id}/stream`

// Join immediately (no delay): with broker-backed streams, replayability derives
// from the run's own resumable flag, so an early bare join cannot poison it.
// (The upstream in-memory impl has a first-joiner race here.)

// --- Test 1: connect, watch ~3s (≈1-2 steps), then disconnect ---
console.log('\n[1] streaming for 3s then DISCONNECTING mid-run...')
const first = await readSse(streamPath, { maxMs: 3000 })
console.log(`  ${summarize(first.events)}`)
const lastId = [...first.events].reverse().find((e) => e.id)?.id
if (first.ended) throw new Error('stream ended within 3s — graph too fast to test disconnect')
if (!lastId) throw new Error('no event ids received — resumable not active?')
console.log(`  disconnected. last-event-id=${lastId}`)

// --- Test 2: stay away 5s (run should keep executing server-side) ---
console.log('\n[2] client away for 5s (run should continue in background)...')
await new Promise((r) => setTimeout(r, 5000))

// --- Test 3: reconnect with Last-Event-ID, expect replay + live tail to end ---
console.log('\n[3] reconnecting with Last-Event-ID (expect replay of missed events + live tail)...')
const second = await readSse(streamPath, { lastEventId: lastId, maxMs: 20000 })
console.log(`  ${summarize(second.events)}`)
if (!second.ended) throw new Error('stream did not end naturally after reconnect')

// --- Verdict ---
const finalRun = await getJson<{ status: string }>(`/threads/${thread.thread_id}/runs/${run.run_id}`)
const state = await getJson<{ values: { steps: string[] } }>(`/threads/${thread.thread_id}/state`)

const stepsSeenBeforeDisconnect = Math.max(
  ...first.events.filter((e) => e.event === 'values').map((e) => (JSON.parse(e.data).steps ?? []).length),
)
const replayedOrMissed = second.events
  .filter((e) => e.event === 'values')
  .some((e) => (JSON.parse(e.data).steps ?? []).length > stepsSeenBeforeDisconnect)

console.log('\n=== RESULTS ===')
console.log(`run status:              ${finalRun.status} (expect success)`)
console.log(`final steps in state:    ${state.values.steps?.length} (expect 5)`)
console.log(`events before disconnect: ${first.events.length} (max steps seen: ${stepsSeenBeforeDisconnect})`)
console.log(`events after reconnect:   ${second.events.length} (got newer steps: ${replayedOrMissed})`)

const pass = finalRun.status === 'success' && state.values.steps?.length === 5 && replayedOrMissed && second.ended
console.log(pass ? '\n✅ PASS: background run + resumable reconnect both work' : '\n❌ FAIL')
process.exit(pass ? 0 : 1)
