// Verifies PostgresSaver works under Bun: setup, checkpoint write/read,
// state history, and resume-from-checkpoint across saver instances.
import { PostgresSaver } from '@langchain/langgraph-checkpoint-postgres'
import { graph } from '../graphs/agent'

const DSN = process.env.DATABASE_URL ?? 'postgresql://postgres:postgres@localhost:5442/langgraph'

const saver = PostgresSaver.fromConnString(DSN)
await saver.setup()
console.log('✓ PostgresSaver.setup() — tables created')

graph.checkpointer = saver
const config = { configurable: { thread_id: 'pg-spike-1' } }

const result = await graph.invoke({ steps: [] }, config)
console.log(`✓ graph.invoke with PG checkpointer — ${result.steps.length} steps`)

const snapshot = await graph.getState(config)
console.log(
  `✓ getState from PG — steps=${snapshot.values.steps.length}, checkpoint_id=${snapshot.config.configurable?.checkpoint_id}`,
)

let historyCount = 0
for await (const _ of graph.getStateHistory(config)) historyCount += 1
console.log(`✓ getStateHistory from PG — ${historyCount} checkpoints`)

// Fresh saver instance (simulates process restart) must see the same state.
const saver2 = PostgresSaver.fromConnString(DSN)
const tuple = await saver2.getTuple(config)
const persistedSteps = (tuple?.checkpoint.channel_values?.steps as string[]) ?? []
console.log(`✓ fresh saver instance reads persisted checkpoint — steps=${persistedSteps.length}`)

await saver.end()
await saver2.end()

const pass = result.steps.length === 5 && historyCount >= 6 && persistedSteps.length === 5
console.log(pass ? '\n✅ PASS: PostgresSaver fully functional on Bun' : '\n❌ FAIL')
process.exit(pass ? 0 : 1)
