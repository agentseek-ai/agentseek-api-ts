import { HTTPException } from 'hono/http-exception'
import type pg from 'pg'
import { handleAuthEvent, isAuthMatching, type AuthContext } from '@langchain/langgraph-api/auth'
import type {
  Assistant,
  AssistantsRepo,
  AssistantVersion,
  Metadata,
  OnConflictBehavior,
  RunnableConfig,
} from '@langchain/langgraph-api/storage'

interface AssistantRow {
  assistant_id: string
  graph_id: string
  name: string
  description: string | null
  version: number
  config: RunnableConfig
  context: unknown
  metadata: Metadata
  created_at: Date
  updated_at: Date
}

const toAssistant = (row: AssistantRow): Assistant => ({
  assistant_id: row.assistant_id,
  graph_id: row.graph_id,
  name: row.name,
  description: row.description,
  version: row.version,
  config: row.config ?? {},
  context: row.context ?? {},
  metadata: row.metadata ?? {},
  created_at: row.created_at,
  updated_at: row.updated_at,
})

const SORTABLE = new Set(['assistant_id', 'created_at', 'updated_at', 'name', 'graph_id'])

export class PgAssistants implements AssistantsRepo {
  constructor(private readonly pool: pg.Pool) {}

  async *search(
    options: {
      graph_id?: string
      name?: string
      metadata?: Metadata
      limit: number
      offset: number
      sort_by?: 'assistant_id' | 'created_at' | 'updated_at' | 'name' | 'graph_id'
      sort_order?: 'asc' | 'desc'
      select?: string[]
    },
    auth: AuthContext | undefined,
  ): AsyncGenerator<{ assistant: Assistant; total: number }> {
    const [filters] = await handleAuthEvent(auth, 'assistants:search', {
      graph_id: options.graph_id,
      metadata: options.metadata,
      limit: options.limit,
      offset: options.offset,
    })

    const where: string[] = []
    const params: unknown[] = []
    if (options.graph_id) {
      params.push(options.graph_id)
      where.push(`graph_id = $${params.length}`)
    }
    if (options.name) {
      params.push(options.name)
      where.push(`name = $${params.length}`)
    }
    if (options.metadata) {
      params.push(JSON.stringify(options.metadata))
      where.push(`metadata @> $${params.length}::jsonb`)
    }

    const sortBy = SORTABLE.has(options.sort_by ?? '') ? options.sort_by : 'created_at'
    const sortOrder = options.sort_order === 'asc' ? 'ASC' : 'DESC'
    params.push(options.limit, options.offset)

    const res = await this.pool.query<AssistantRow & { total: string }>(
      `SELECT *, COUNT(*) OVER() AS total FROM assistants
       ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
       ORDER BY ${sortBy} ${sortOrder}
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params,
    )

    for (const row of res.rows) {
      if (!isAuthMatching(row.metadata, filters)) continue
      yield { assistant: toAssistant(row), total: Number(row.total) }
    }
  }

  async get(assistant_id: string, auth: AuthContext | undefined): Promise<Assistant> {
    const [filters] = await handleAuthEvent(auth, 'assistants:read', { assistant_id })

    const res = await this.pool.query<AssistantRow>(`SELECT * FROM assistants WHERE assistant_id = $1`, [assistant_id])
    const row = res.rows[0]
    if (!row || !isAuthMatching(row.metadata, filters)) {
      throw new HTTPException(404, {
        message: `Assistant with ID ${assistant_id} not found`,
      })
    }
    return toAssistant(row)
  }

  async put(
    assistant_id: string,
    options: {
      config: RunnableConfig
      context: unknown
      graph_id: string
      metadata?: Metadata
      if_exists: OnConflictBehavior
      name?: string
      description?: string
    },
    auth: AuthContext | undefined,
  ): Promise<Assistant> {
    const [filters, mutable] = await handleAuthEvent(auth, 'assistants:create', {
      assistant_id,
      config: options.config,
      context: options.context,
      graph_id: options.graph_id,
      metadata: options.metadata,
      if_exists: options.if_exists,
      name: options.name,
      description: options.description,
    })

    const existing = await this.pool.query<AssistantRow>(`SELECT * FROM assistants WHERE assistant_id = $1`, [
      assistant_id,
    ])
    if (existing.rows[0]) {
      const row = existing.rows[0]
      if (!isAuthMatching(row.metadata, filters) || options.if_exists === 'raise') {
        throw new HTTPException(409, { message: 'Assistant already exists' })
      }
      return toAssistant(row)
    }

    const metadata = JSON.stringify(mutable.metadata ?? {})
    const config = JSON.stringify(options.config ?? {})
    const context = JSON.stringify(options.context ?? {})
    const name = options.name || options.graph_id

    const inserted = await this.pool.query<AssistantRow>(
      `INSERT INTO assistants (assistant_id, graph_id, name, description, version, config, context, metadata)
       VALUES ($1, $2, $3, $4, 1, $5::jsonb, $6::jsonb, $7::jsonb)
       ON CONFLICT (assistant_id) DO NOTHING
       RETURNING *`,
      [assistant_id, options.graph_id, name, options.description ?? null, config, context, metadata],
    )
    const row = inserted.rows[0]
    if (!row) return this.get(assistant_id, auth)

    await this.pool.query(
      `INSERT INTO assistant_versions (assistant_id, version, graph_id, name, description, config, context, metadata, created_at)
       VALUES ($1, 1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8)
       ON CONFLICT DO NOTHING`,
      [assistant_id, options.graph_id, name, options.description ?? null, config, context, metadata, row.created_at],
    )
    return toAssistant(row)
  }

  async patch(
    assistantId: string,
    options: {
      config?: RunnableConfig
      context?: unknown
      graph_id?: string
      metadata?: Metadata
      name?: string
      description?: string
    },
    auth: AuthContext | undefined,
  ): Promise<Assistant> {
    const [filters, mutable] = await handleAuthEvent(auth, 'assistants:update', {
      assistant_id: assistantId,
      graph_id: options.graph_id,
      config: options.config,
      context: options.context,
      metadata: options.metadata,
      name: options.name,
      description: options.description,
    })

    const current = await this.pool.query<AssistantRow>(`SELECT * FROM assistants WHERE assistant_id = $1`, [
      assistantId,
    ])
    const row = current.rows[0]
    if (!row || !isAuthMatching(row.metadata, filters)) {
      throw new HTTPException(404, { message: 'Assistant not found' })
    }

    const now = new Date()
    const graphId = options.graph_id ?? row.graph_id
    const config = options.config ?? row.config ?? {}
    const context = options.context ?? row.context ?? {}
    const name = options.name ?? row.name
    const description = options.description ?? row.description
    const metadata = mutable.metadata != null ? { ...row.metadata, ...mutable.metadata } : (row.metadata ?? {})

    const versionRes = await this.pool.query<{ next_version: number }>(
      `SELECT COALESCE(MAX(version), 0) + 1 AS next_version FROM assistant_versions WHERE assistant_id = $1`,
      [assistantId],
    )
    const nextVersion = versionRes.rows[0]!.next_version

    const updated = await this.pool.query<AssistantRow>(
      `UPDATE assistants
       SET graph_id = $2, name = $3, description = $4, version = $5,
           config = $6::jsonb, context = $7::jsonb, metadata = $8::jsonb, updated_at = $9
       WHERE assistant_id = $1
       RETURNING *`,
      [
        assistantId,
        graphId,
        name,
        description,
        nextVersion,
        JSON.stringify(config),
        JSON.stringify(context),
        JSON.stringify(metadata),
        now,
      ],
    )
    await this.pool.query(
      `INSERT INTO assistant_versions (assistant_id, version, graph_id, name, description, config, context, metadata, created_at)
       VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9)`,
      [
        assistantId,
        nextVersion,
        graphId,
        name,
        description,
        JSON.stringify(config),
        JSON.stringify(context),
        JSON.stringify(metadata),
        now,
      ],
    )
    return toAssistant(updated.rows[0]!)
  }

  async delete(assistant_id: string, delete_threads: boolean, auth: AuthContext | undefined): Promise<string[]> {
    const [filters] = await handleAuthEvent(auth, 'assistants:delete', { assistant_id })

    const res = await this.pool.query<AssistantRow>(`SELECT * FROM assistants WHERE assistant_id = $1`, [assistant_id])
    const row = res.rows[0]
    if (!row || !isAuthMatching(row.metadata, filters)) {
      throw new HTTPException(404, { message: 'Assistant not found' })
    }

    await this.pool.query(`DELETE FROM assistants WHERE assistant_id = $1`, [assistant_id])
    if (delete_threads) {
      await this.pool.query(`DELETE FROM threads WHERE metadata->>'assistant_id' = $1`, [assistant_id])
    }
    return [assistant_id]
  }

  async count(
    options: { graph_id?: string; name?: string; metadata?: Metadata },
    auth: AuthContext | undefined,
  ): Promise<number> {
    const [filters] = await handleAuthEvent(auth, 'assistants:search', {
      graph_id: options.graph_id,
      metadata: options.metadata,
      limit: 0,
      offset: 0,
    })

    // Auth filters need per-row matching, so count in JS when auth is active.
    if (filters != null) {
      let total = 0
      for await (const _ of this.search({ ...options, limit: 10_000, offset: 0 }, auth)) total += 1
      return total
    }

    const where: string[] = []
    const params: unknown[] = []
    if (options.graph_id) {
      params.push(options.graph_id)
      where.push(`graph_id = $${params.length}`)
    }
    if (options.name) {
      params.push(options.name)
      where.push(`name = $${params.length}`)
    }
    if (options.metadata) {
      params.push(JSON.stringify(options.metadata))
      where.push(`metadata @> $${params.length}::jsonb`)
    }
    const res = await this.pool.query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM assistants ${where.length ? `WHERE ${where.join(' AND ')}` : ''}`,
      params,
    )
    return Number(res.rows[0]!.count)
  }

  async setLatest(assistant_id: string, version: number, auth: AuthContext | undefined): Promise<Assistant> {
    const [filters] = await handleAuthEvent(auth, 'assistants:update', { assistant_id })

    const res = await this.pool.query<AssistantRow>(`SELECT * FROM assistants WHERE assistant_id = $1`, [assistant_id])
    const row = res.rows[0]
    if (!row || !isAuthMatching(row.metadata, filters)) {
      throw new HTTPException(404, { message: 'Assistant not found' })
    }

    const versionRes = await this.pool.query<AssistantRow>(
      `SELECT * FROM assistant_versions WHERE assistant_id = $1 AND version = $2`,
      [assistant_id, version],
    )
    const versionRow = versionRes.rows[0]
    if (!versionRow) throw new HTTPException(404, { message: 'Assistant version not found' })

    const updated = await this.pool.query<AssistantRow>(
      `UPDATE assistants
       SET graph_id = $2, name = $3, description = $4, version = $5,
           config = $6::jsonb, context = $7::jsonb, metadata = $8::jsonb, updated_at = now()
       WHERE assistant_id = $1
       RETURNING *`,
      [
        assistant_id,
        versionRow.graph_id,
        versionRow.name,
        versionRow.description,
        version,
        JSON.stringify(versionRow.config ?? {}),
        JSON.stringify(versionRow.context ?? {}),
        JSON.stringify(versionRow.metadata ?? {}),
      ],
    )
    return toAssistant(updated.rows[0]!)
  }

  async getVersions(
    assistant_id: string,
    options: { limit: number; offset: number; metadata?: Metadata },
    auth: AuthContext | undefined,
  ): Promise<AssistantVersion[]> {
    const [filters] = await handleAuthEvent(auth, 'assistants:read', { assistant_id })

    const params: unknown[] = [assistant_id]
    let metadataClause = ''
    if (options.metadata) {
      params.push(JSON.stringify(options.metadata))
      metadataClause = `AND metadata @> $${params.length}::jsonb`
    }
    params.push(options.limit, options.offset)

    const res = await this.pool.query<AssistantRow>(
      `SELECT * FROM assistant_versions
       WHERE assistant_id = $1 ${metadataClause}
       ORDER BY version DESC
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params,
    )
    return res.rows
      .filter((row) => isAuthMatching(row.metadata, filters))
      .map((row) => ({
        assistant_id: row.assistant_id,
        version: row.version,
        graph_id: row.graph_id,
        config: row.config ?? {},
        context: row.context ?? {},
        metadata: row.metadata ?? {},
        created_at: row.created_at,
        name: row.name,
        description: row.description,
      }))
  }
}
