// Unit tests for RunBroker — the in-memory event replay buffer and run
// coordination. Pure in-process, no server/Postgres needed.
import { describe, expect, test } from 'bun:test'
import { RunBroker } from '../src/storage/broker'
import { sleep } from './helpers'

const EPOCH = 1_000_000

describe('event replay buffer', () => {
  test('append assigns contiguous seqs; read slices strictly after a seq', () => {
    const broker = new RunBroker()
    expect(broker.maxSeq('r1')).toBe(-1)

    const seqs = ['a', 'b', 'c'].map((e) => broker.append('r1', e, `"${e}"`, null))
    expect(seqs).toEqual([0, 1, 2])
    expect(broker.maxSeq('r1')).toBe(2)

    expect(broker.read('r1', -1, 256).map((e) => e.event)).toEqual(['a', 'b', 'c'])
    expect(broker.read('r1', 0, 256).map((e) => e.seq)).toEqual([1, 2])
    expect(broker.read('r1', 2, 256)).toEqual([])
    expect(broker.read('r1', 1, 1).map((e) => e.seq)).toEqual([2])
    expect(broker.read('unknown', -1, 256)).toEqual([])
  })

  test('beginIncarnation namespaces seq by attempt and drops stale same-process events', () => {
    const broker = new RunBroker()
    broker.beginIncarnation('r1', 1)
    broker.append('r1', 'metadata', '{}', null)
    broker.append('r1', 'values', '{}', null)
    expect(broker.maxSeq('r1')).toBe(1)

    // Retry: attempt 2 republishes from its own epoch, old events are gone.
    broker.beginIncarnation('r1', 2)
    expect(broker.read('r1', -1, 256)).toEqual([])
    const seq = broker.append('r1', 'metadata', '{}', null)
    expect(seq).toBe(EPOCH)
    // A stale Last-Event-ID from attempt 1 (e.g. 1) yields all new events.
    expect(broker.read('r1', 1, 256).map((e) => e.seq)).toEqual([EPOCH])
  })

  test('caps each buffer at 10k events, dropping the oldest without rewinding seq', () => {
    const broker = new RunBroker()
    for (let i = 0; i < 10_050; i += 1) broker.append('r1', 'values', null, null)
    const all = broker.read('r1', -1, 20_000)
    expect(all).toHaveLength(10_000)
    expect(all[0]!.seq).toBe(50)
    expect(broker.maxSeq('r1')).toBe(10_049)
    // Contiguity-based O(1) indexing still lines up after the head trim.
    expect(broker.read('r1', 10_047, 256).map((e) => e.seq)).toEqual([10_048, 10_049])
  })

  test('sweep reclaims finished buffers by TTL, honors per-buffer override, never touches live runs', async () => {
    const broker = new RunBroker()
    broker.append('live', 'values', null, null)
    broker.append('short', 'values', null, null)
    broker.append('long', 'values', null, null)

    broker.markFinished('short', 0) // e.g. non-resumable / deleted run
    broker.markFinished('long') // default TTL
    await sleep(5)

    expect(broker.sweep(60_000)).toBe(1) // only 'short' (override 0 < 5ms age)
    expect(broker.maxSeq('short')).toBe(-1)
    expect(broker.maxSeq('long')).toBe(0)
    expect(broker.maxSeq('live')).toBe(0)

    expect(broker.sweep(1)).toBe(1) // now 'long' exceeds the 1ms default TTL
    expect(broker.maxSeq('long')).toBe(-1)
    expect(broker.maxSeq('live')).toBe(0) // never marked finished → never swept
  })

  test('markFinished on an unknown run is a no-op', () => {
    const broker = new RunBroker()
    broker.markFinished('ghost', 0)
    expect(broker.sweep(0)).toBe(0)
  })
})

describe('coordination', () => {
  test('notify wakes a pending wait; timeout and abort resolve false', async () => {
    const broker = new RunBroker()

    const woken = broker.wait('r1', { timeoutMs: 5_000 })
    broker.notify('r1')
    expect(await woken).toBe(true)

    expect(await broker.wait('r1', { timeoutMs: 20 })).toBe(false)

    const controller = new AbortController()
    const aborted = broker.wait('r1', { timeoutMs: 5_000, signal: controller.signal })
    controller.abort()
    expect(await aborted).toBe(false)
  })

  test('lock exposes an abortable control; unlock removes it', () => {
    const broker = new RunBroker()
    const signal = broker.lock('r1')
    expect(signal.aborted).toBe(false)

    broker.getControl('r1')?.abort('interrupt')
    expect(signal.aborted).toBe(true)
    expect(signal.reason).toBe('interrupt')

    broker.unlock('r1')
    expect(broker.getControl('r1')).toBeUndefined()
  })

  test('reset drops buffers, controls and listeners', async () => {
    const broker = new RunBroker()
    broker.append('r1', 'values', null, null)
    broker.lock('r1')
    broker.reset()
    expect(broker.maxSeq('r1')).toBe(-1)
    expect(broker.getControl('r1')).toBeUndefined()
  })
})
