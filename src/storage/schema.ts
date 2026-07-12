import type pg from 'pg'

const DDL = `
CREATE TABLE IF NOT EXISTS assistants (
  assistant_id UUID PRIMARY KEY,
  graph_id     TEXT NOT NULL,
  name         TEXT NOT NULL,
  description  TEXT,
  version      INT NOT NULL DEFAULT 1,
  config       JSONB NOT NULL DEFAULT '{}',
  context      JSONB NOT NULL DEFAULT '{}',
  metadata     JSONB NOT NULL DEFAULT '{}',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS assistant_versions (
  assistant_id UUID NOT NULL REFERENCES assistants(assistant_id) ON DELETE CASCADE,
  version      INT NOT NULL,
  graph_id     TEXT NOT NULL,
  name         TEXT NOT NULL,
  description  TEXT,
  config       JSONB NOT NULL DEFAULT '{}',
  context      JSONB NOT NULL DEFAULT '{}',
  metadata     JSONB NOT NULL DEFAULT '{}',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (assistant_id, version)
);

CREATE TABLE IF NOT EXISTS threads (
  thread_id  UUID PRIMARY KEY,
  status     TEXT NOT NULL DEFAULT 'idle',
  config     JSONB NOT NULL DEFAULT '{}',
  metadata   JSONB NOT NULL DEFAULT '{}',
  "values"   JSONB,
  interrupts JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS threads_metadata_idx ON threads USING GIN (metadata);

CREATE TABLE IF NOT EXISTS runs (
  run_id             UUID PRIMARY KEY,
  thread_id          UUID NOT NULL REFERENCES threads(thread_id) ON DELETE CASCADE,
  assistant_id       UUID NOT NULL,
  status             TEXT NOT NULL DEFAULT 'pending',
  metadata           JSONB NOT NULL DEFAULT '{}',
  kwargs             JSONB NOT NULL DEFAULT '{}',
  multitask_strategy TEXT NOT NULL DEFAULT 'reject',
  attempt            INT NOT NULL DEFAULT 0,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS runs_pending_idx ON runs (created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS runs_thread_idx ON runs (thread_id);

-- Stream events moved to the in-process replay buffer (RunBroker); drop the
-- table left behind by earlier versions. Its contents were ephemeral (TTL'd
-- replay data), so this is safe.
DROP TABLE IF EXISTS run_events;
`

export async function ensureSchema(pool: pg.Pool): Promise<void> {
  await pool.query(DDL)
}

const MAX_RETRY_ATTEMPTS = 3

// Single-instance recovery: runs left 'running' by a crashed/restarted server
// are requeued (they resume from their last checkpoint) or failed if exhausted.
export async function recoverOrphanedRuns(pool: pg.Pool): Promise<number> {
  const requeued = await pool.query(
    `UPDATE runs SET status = 'pending', updated_at = now()
     WHERE status = 'running' AND attempt < $1`,
    [MAX_RETRY_ATTEMPTS],
  )
  const errored = await pool.query<{ thread_id: string }>(
    `UPDATE runs SET status = 'error', updated_at = now()
     WHERE status = 'running'
     RETURNING thread_id`,
  )
  // Force-erroring a run bypasses the worker's thread-status callback, so
  // release its thread here — otherwise it stays 'busy' forever.
  if (errored.rows.length > 0) {
    await pool.query(
      `UPDATE threads SET status = 'error', updated_at = now()
       WHERE thread_id = ANY($1) AND NOT EXISTS (
         SELECT 1 FROM runs r
         WHERE r.thread_id = threads.thread_id AND r.status IN ('pending', 'running')
       )`,
      [errored.rows.map((r) => r.thread_id)],
    )
  }
  // Hard guarantee of thread serialization: the claim query's NOT EXISTS
  // check runs on a statement snapshot, so two workers can race past it —
  // this index makes the loser's UPDATE fail (23505) instead of letting two
  // runs of one thread execute concurrently. Created AFTER recovery so
  // leftover duplicate 'running' rows from a crashed process can't block it.
  await pool.query(
    `CREATE UNIQUE INDEX IF NOT EXISTS runs_one_running_per_thread ON runs (thread_id) WHERE status = 'running'`,
  )

  return requeued.rowCount ?? 0
}
