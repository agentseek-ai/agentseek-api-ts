import { AIMessage } from '@langchain/core/messages'
import { FakeListChatModel } from '@langchain/core/utils/testing'
import { MessagesAnnotation, StateGraph } from '@langchain/langgraph'

// Token-streaming graph without an API key: FakeListChatModel streams the
// response character by character, exercising the messages/messages-tuple
// stream modes end to end (including replay serialization through Postgres).
const model = new FakeListChatModel({
  responses: [
    'Hello! This is a fake streaming response used to verify that token ' +
      'streams survive disconnect, replay and server restarts.',
  ],
  sleep: 25,
})

const respond = async (state: typeof MessagesAnnotation.State): Promise<{ messages: AIMessage[] }> => {
  let content = ''
  const stream = await model.stream(state.messages)
  for await (const chunk of stream) {
    content += typeof chunk.content === 'string' ? chunk.content : ''
  }
  return { messages: [new AIMessage(content)] }
}

const builder = new StateGraph(MessagesAnnotation)
  .addNode('respond', respond)
  .addEdge('__start__', 'respond')
  .addEdge('respond', '__end__')

export const graph = builder.compile()
