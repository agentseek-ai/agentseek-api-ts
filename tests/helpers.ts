import type { Subprocess } from 'bun'

export interface SseEvent {
  id: string | null
  event: string
  data: string
}

export class TestServer {
  private proc: Subprocess | undefined
  readonly base: string

  constructor(private readonly port: number) {
    this.base = `http://localhost:${port}`
  }

  async start(): Promise<void> {
    this.proc = Bun.spawn(['bun', 'run', 'src/server.ts'], {
      cwd: new URL('..', import.meta.url).pathname,
      env: { ...process.env, PORT: String(this.port) },
      stdout: 'ignore',
      stderr: 'pipe',
    })

    for (let i = 0; i < 60; i += 1) {
      try {
        const res = await fetch(`${this.base}/info`)
        if (res.ok) return
      } catch {
        // server not up yet
      }
      await sleep(500)
    }
    throw new Error('server did not become healthy within 30s')
  }

  stop(): void {
    // SIGKILL: graceful shutdown can hang on open SSE connections, and test
    // teardown must never leave a stray server holding the port.
    this.proc?.kill(9)
  }

  async req<T>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await fetch(`${this.base}${path}`, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: body != null ? JSON.stringify(body) : undefined,
    })
    if (!res.ok) {
      throw new Error(`${method} ${path} -> ${res.status}: ${await res.text()}`)
    }
    return (await res.json()) as T
  }

  async rawPost(path: string, body: unknown): Promise<Response> {
    return fetch(`${this.base}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
  }

  async readSse(
    path: string,
    opts: { lastEventId?: string; maxMs?: number } = {},
  ): Promise<{ events: SseEvent[]; ended: boolean }> {
    const controller = new AbortController()
    const timer = opts.maxMs ? setTimeout(() => controller.abort(), opts.maxMs) : null
    const headers: Record<string, string> = { Accept: 'text/event-stream' }
    if (opts.lastEventId) headers['Last-Event-ID'] = opts.lastEventId

    const events: SseEvent[] = []
    let ended = false
    try {
      const res = await fetch(`${this.base}${path}`, {
        headers,
        signal: controller.signal,
      })
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
            events.push({
              id: cur.id ?? null,
              event: cur.event,
              data: cur.data ?? '',
            })
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
}

export const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))
