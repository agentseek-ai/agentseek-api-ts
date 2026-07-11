import { Annotation, StateGraph } from '@langchain/langgraph'

// Deterministic 5-step slow graph (~10s) so we can disconnect mid-run
// and verify background execution + resumable streaming without an LLM.
const State = Annotation.Root({
  steps: Annotation<string[]>({
    reducer: (a, b) => a.concat(b),
    default: () => [],
  }),
})

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

const makeNode = (name: string) => async (_state: typeof State.State) => {
  await sleep(2000)
  return { steps: [`${name} done at ${new Date().toISOString()}`] }
}

const builder = new StateGraph(State)
  .addNode('step1', makeNode('step1'))
  .addNode('step2', makeNode('step2'))
  .addNode('step3', makeNode('step3'))
  .addNode('step4', makeNode('step4'))
  .addNode('step5', makeNode('step5'))
  .addEdge('__start__', 'step1')
  .addEdge('step1', 'step2')
  .addEdge('step2', 'step3')
  .addEdge('step3', 'step4')
  .addEdge('step4', 'step5')
  .addEdge('step5', '__end__')

export const graph = builder.compile()
