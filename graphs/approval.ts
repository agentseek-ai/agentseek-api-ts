import { Annotation, interrupt, StateGraph } from '@langchain/langgraph'

// Human-in-the-loop graph: pauses at `ask` until resumed with a Command.
const State = Annotation.Root({
  steps: Annotation<string[]>({
    reducer: (a, b) => a.concat(b),
    default: () => [],
  }),
})

const ask = async (_state: typeof State.State) => {
  const answer = interrupt({ question: 'Approve this action?' })
  return { steps: [`approved:${answer}`] }
}

const finish = async (_state: typeof State.State) => {
  return { steps: ['finished'] }
}

const builder = new StateGraph(State)
  .addNode('ask', ask)
  .addNode('finish', finish)
  .addEdge('__start__', 'ask')
  .addEdge('ask', 'finish')
  .addEdge('finish', '__end__')

export const graph = builder.compile()
